import asyncio
import cv2
import numpy as np
import json
import getpass
from pathlib import Path
from datetime import datetime
from aiortc import RTCPeerConnection, RTCSessionDescription, VideoStreamTrack
from aiortc.contrib.media import MediaPlayer, MediaRelay
from ring_doorbell import (
    Auth,
    AuthenticationError,
    Requires2FAError,
    Ring,
    RingDoorBell,
)
from ring_doorbell.webrtcstream import RingWebRtcMessage
from aiortc.contrib.media import MediaBlackhole, MediaPlayer, MediaRecorder

# Configuration
user_agent = "my_ring_app/1.0"
cache_file = Path("/Users/ty/code/ring_token.cache")
output_dir = Path("ring_images")
output_dir.mkdir(exist_ok=True)

# WebRTC Peer Connection
pcs = set()


class VideoTransformTrack(VideoStreamTrack):
    def __init__(self):
        super().__init__()
        self.frame_count = 0
        self.save_interval = 30  # Save image every 30 frames (roughly every second)
        self.last_save_time = 0
        self.track = None
        print("VideoTransformTrack initialized")

    async def recv(self):
        if self.track is None:
            print("No video track assigned yet, waiting...")
            await asyncio.sleep(0.1)
            return None

        try:
            frame = await self.track.recv()
            self.frame_count += 1

            print(frame)
            # Convert frame to numpy array
            img = frame.to_ndarray(format="bgr24")

            # Save image periodically
            current_time = datetime.now()

            timestamp = current_time.strftime("%Y%m%d_%H%M%S")
            filename = output_dir / f"ring_doorbell_{timestamp}.jpg"
            cv2.imwrite(str(filename), img)
            print(f"Saved image: {filename}")
            self.last_save_time = current_time

            return frame
        except Exception as e:
            print(f"Error in VideoTransformTrack.recv(): {e}")
            return None


def token_updated(token):
    cache_file.write_text(json.dumps(token))


def otp_callback():
    auth_code = input("2FA code: ")
    return auth_code


async def do_auth():
    username = input("Username: ")
    password = getpass.getpass("Password: ")
    auth = Auth(user_agent, None, token_updated)
    try:
        await auth.async_fetch_token(username, password)
    except Requires2FAError:
        await auth.async_fetch_token(username, password, otp_callback())
    return auth


async def get_ring_doorbell():
    """Authenticate and get the first doorbell device."""
    if cache_file.is_file():  # auth token is cached
        auth = Auth(user_agent, json.loads(cache_file.read_text()), token_updated)
        ring = Ring(auth)
        try:
            await ring.async_create_session()  # auth token still valid
        except AuthenticationError:  # auth token has expired
            auth = await do_auth()
            ring = Ring(auth)
    else:
        auth = await do_auth()  # Get new auth token
        ring = Ring(auth)

    await ring.async_update_data()
    devices = ring.devices()

    doorbell: RingDoorBell = devices["doorbots"][0]
    print(f"Using doorbell: {doorbell.name} ({doorbell.model})")

    return ring, doorbell


async def handle_webrtc_message(message: RingWebRtcMessage):
    """Handle WebRTC messages from the Ring doorbell."""
    if message.answer:
        print("Received SDP answer from doorbell")
        # Store the answer for later use
        return message.answer
    elif message.candidate:
        print(f"Received ICE candidate: {message.candidate}")
    elif message.error_code:
        print(f"WebRTC error: {message.error_code} - {message.error_message}")


async def run_ring_webrtc_stream():
    """Run WebRTC stream with Ring doorbell integration."""
    ring, doorbell = await get_ring_doorbell()

    # Create WebRTC peer connection
    pc = RTCPeerConnection()
    pcs.add(pc)

    @pc.on("track")
    def on_track(track):
        print(f"Receiving {track.kind} track from doorbell")
        if track.kind == "video":
            video_track.track = track
            print("Video track assigned to VideoTransformTrack")
        elif track.kind == "audio":
            print("Audio track received (not processing)")

    # Add video track to receive the stream
    video_track = VideoTransformTrack()
    pc.addTrack(video_track)

    # Create SDP offer
    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    print("Generated SDP offer:")
    print(pc.localDescription.sdp)

    # Set up event handlers BEFORE establishing connection

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        print(f"Connection state: {pc.connectionState}")
        if pc.connectionState == "failed":
            await pc.close()
            pcs.discard(pc)
        elif pc.connectionState == "connected":
            print("Successfully connected to Ring doorbell stream")

    @pc.on("iceconnectionstatechange")
    async def on_iceconnectionstatechange():
        print(f"ICE connection state: {pc.iceConnectionState}")

    @pc.on("signalingstatechange")
    async def on_signalingstatechange():
        print(f"Signaling state: {pc.signalingState}")

    # Use Ring doorbell's WebRTC stream generation
    try:
        sdp_answer = await doorbell.generate_webrtc_stream(
            pc.localDescription.sdp,
            keep_alive_timeout=300,  # 5 minutes
        )

        if sdp_answer:
            print("Received SDP answer from Ring doorbell:")
            print(sdp_answer)

            # Set the remote description
            remote_desc = RTCSessionDescription(sdp=sdp_answer, type="answer")
            await pc.setRemoteDescription(remote_desc)

            print("WebRTC connection established with Ring doorbell")

            # Check if we have any tracks after connection
            print(f"Current tracks: {len(pc.getReceivers())}")
            # for receiver in pc.getReceivers():
            #     if receiver.track:
            #         print(f"Found track: {receiver.track.kind}")
            #         if receiver.track.kind == "video" and video_track.track is None:
            #             video_track.track = receiver.track
            #             print("Video track assigned from existing receiver")

            # Keep the connection alive and save images
            print("Streaming started. Press Ctrl+C to stop.")
            print(f"Images will be saved to: {output_dir}")

            try:
                # Keep alive the stream
                while True:
                    await asyncio.sleep(5)
                    await video_track.recv()
                    # You can also call doorbell.keep_alive_webrtc_stream() if needed

            except KeyboardInterrupt:
                print("\nStopping stream...")

    except Exception as e:
        print(f"Error establishing WebRTC stream: {e}")
        raise
    finally:
        # Clean up
        await ring.auth.async_close()


async def main():
    try:
        await run_ring_webrtc_stream()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Clean up
        for pc in pcs:
            await pc.close()
        pcs.clear()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    asyncio.run(main())
