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
output_dir = Path("ring_images")
output_dir.mkdir(exist_ok=True)


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

            # Convert frame to numpy array for processing
            img = frame.to_ndarray(format="bgr24")
            # current_time = datetime.now()
            # timestamp = current_time.strftime("%Y%m%d_%H%M%S")
            # filename = output_dir / f"ring_doorbell_{timestamp}.jpg"
            # cv2.imwrite(str(filename), img)
            # print(f"Saved image: {filename}")
            
            # Convert to JPEG bytes for Home Assistant
            _, buffer = cv2.imencode('.jpg', img)
            jpeg_bytes = buffer.tobytes()
            return jpeg_bytes

        except Exception as e:
            print(f"Error in VideoTransformTrack.recv(): {e}")
            return None


async def get_image_from_ring_webrtc_stream(doorbell: RingDoorBell) -> bytes | None:
    pc = RTCPeerConnection()

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

            async with asyncio.timeout(10):
                return await video_track.recv()
    finally:
        await pc.close()
