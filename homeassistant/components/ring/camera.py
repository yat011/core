"""Component providing support to the Ring Door Bell camera."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
import logging
from typing import TYPE_CHECKING, Any, Generic
import asyncio
import time

from aiohttp import web
from haffmpeg.camera import CameraMjpeg
from ring_doorbell import RingDoorBell
from ring_doorbell.webrtcstream import RingWebRtcMessage

from .webrtc_client import RingWebRTCClient

from homeassistant.components import ffmpeg
from homeassistant.components.camera import (
    Camera,
    CameraEntityDescription,
    CameraEntityFeature,
    RTCIceCandidateInit,
    WebRTCAnswer,
    WebRTCCandidate,
    WebRTCError,
    WebRTCSendMessage,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_aiohttp_proxy_stream
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.util import dt as dt_util

from . import RingConfigEntry
from .const import DOMAIN
from .coordinator import RingDataCoordinator
from .entity import RingDeviceT, RingEntity, exception_wrap

# Coordinator is used to centralize the data updates
# Actions restricted to 1 at a time
PARALLEL_UPDATES = 1

FORCE_REFRESH_INTERVAL = timedelta(minutes=3)
MOTION_DETECTION_CAPABILITY = "motion_detection"

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class RingCameraEntityDescription(CameraEntityDescription, Generic[RingDeviceT]):
    """Base class for event entity description."""

    exists_fn: Callable[[RingDoorBell], bool]
    live_stream: bool
    motion_detection: bool


CAMERA_DESCRIPTIONS: tuple[RingCameraEntityDescription, ...] = (
    RingCameraEntityDescription(
        key="live_view",
        translation_key="live_view",
        exists_fn=lambda _: True,
        live_stream=True,
        motion_detection=False,
    ),
    RingCameraEntityDescription(
        key="last_recording",
        translation_key="last_recording",
        entity_registry_enabled_default=False,
        exists_fn=lambda camera: camera.has_subscription,
        live_stream=False,
        motion_detection=True,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: RingConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up a Ring Door Bell and StickUp Camera."""
    ring_data = entry.runtime_data
    devices_coordinator = ring_data.devices_coordinator
    ffmpeg_manager = ffmpeg.get_ffmpeg_manager(hass)

    cams = [
        RingCam(camera, devices_coordinator, description, ffmpeg_manager=ffmpeg_manager)
        for description in CAMERA_DESCRIPTIONS
        for camera in ring_data.devices.video_devices
        if description.exists_fn(camera)
    ]

    async_add_entities(cams)


class RingCam(RingEntity[RingDoorBell], Camera):
    """An implementation of a Ring Door Bell camera."""

    def __init__(
        self,
        device: RingDoorBell,
        coordinator: RingDataCoordinator,
        description: RingCameraEntityDescription,
        *,
        ffmpeg_manager: ffmpeg.FFmpegManager,
    ) -> None:
        """Initialize a Ring Door Bell camera."""
        super().__init__(device, coordinator)
        self.entity_description = description
        Camera.__init__(self)
        self._ffmpeg_manager = ffmpeg_manager
        self._last_event: dict[str, Any] | None = None
        self._last_video_id: int | None = None
        self._video_url: str | None = None
        self._images: dict[tuple[int | None, int | None], bytes] = {}
        self._expires_at = dt_util.utcnow() - FORCE_REFRESH_INTERVAL
        self._attr_unique_id = f"{device.id}-{description.key}"
        if description.motion_detection and device.has_capability(
            MOTION_DETECTION_CAPABILITY
        ):
            self._attr_motion_detection_enabled = device.motion_detection
        if description.live_stream:
            self._attr_supported_features |= CameraEntityFeature.STREAM

        self._has_webrtc_stream = False
        self._webrtc_client: RingWebRTCClient | None = None

    @callback
    def _handle_coordinator_update(self) -> None:
        """Call update method."""
        self._device = self._get_coordinator_data().get_video_device(
            self._device.device_api_id
        )
        history_data = self._device.last_history
        if history_data:
            self._last_event = history_data[0]
            # will call async_update to update the attributes and get the
            # video url from the api
            self.async_schedule_update_ha_state(True)
        else:
            self._last_event = None
            self._last_video_id = None
            self._video_url = None
            self._images = {}
            self._expires_at = dt_util.utcnow()
            self.async_write_ha_state()

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return the state attributes."""
        return {
            "video_url": self._video_url,
            "last_video_id": self._last_video_id,
        }

    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Return a still image response from the camera."""

        # Generate a valid WebRTC offer SDP with a unique session ID
        session_id = str(int(time.time() * 1000))
        offer_sdp = (
            f"v=0\r\n"
            f"o=- {session_id} 2 IN IP4 127.0.0.1\r\n"
            f"s=-\r\n"
            f"t=0 0\r\n"
            f"a=group:BUNDLE 0 1\r\n"
            f"a=extmap-allow-mixed\r\n"
            f"a=msid-semantic: WMS\r\n"
            f"m=audio 9 UDP/TLS/RTP/SAVPF 111 63 9 0 8 13 110 126\r\n"
            f"c=IN IP4 0.0.0.0\r\n"
            f"a=rtcp:9 IN IP4 0.0.0.0\r\n"
            f"a=ice-ufrag:d+sg\r\n"
            f"a=ice-pwd:rh83OrDBymg0ys+ImCG1pFWd\r\n"
            f"a=ice-options:trickle\r\n"
            f"a=fingerprint:sha-256 94:1E:57:6B:76:E7:79:E6:F1:37:CA:99:1E:7B:8F:F3:C7:A0:5A:E5:8C:DA:04:FA:40:F5:49:21:D0:9E:E2:15\r\n"
            f"a=setup:actpass\r\n"
            f"a=mid:0\r\n"
            f"a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level\r\n"
            f"a=extmap:2 http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time\r\n"
            f"a=extmap:3 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01\r\n"
            f"a=extmap:4 urn:ietf:params:rtp-hdrext:sdes:mid\r\n"
            f"a=recvonly\r\n"
            f"a=rtcp-mux\r\n"
            f"a=rtcp-rsize\r\n"
            f"a=rtpmap:111 opus/48000/2\r\n"
            f"a=rtcp-fb:111 transport-cc\r\n"
            f"a=fmtp:111 minptime=10;useinbandfec=1\r\n"
            f"a=rtpmap:63 red/48000/2\r\n"
            f"a=fmtp:63 111/111\r\n"
            f"a=rtpmap:9 G722/8000\r\n"
            f"a=rtpmap:0 PCMU/8000\r\n"
            f"a=rtpmap:8 PCMA/8000\r\n"
            f"a=rtpmap:13 CN/8000\r\n"
            f"a=rtpmap:110 telephone-event/48000\r\n"
            f"a=rtpmap:126 telephone-event/8000\r\n"
            f"m=video 9 UDP/TLS/RTP/SAVPF 96 97 98 99 100 101 35 36 37 38 103 104 107 108 109 114 115 116 117 118 39 40 41 42 43 44 45 46 47 48 119 120 121 122 49 50 51 52 123 124 125 53\r\n"
            f"c=IN IP4 0.0.0.0\r\n"
            f"a=rtcp:9 IN IP4 0.0.0.0\r\n"
            f"a=ice-ufrag:d+sg\r\n"
            f"a=ice-pwd:rh83OrDBymg0ys+ImCG1pFWd\r\n"
            f"a=ice-options:trickle\r\n"
            f"a=fingerprint:sha-256 94:1E:57:6B:76:E7:79:E6:F1:37:CA:99:1E:7B:8F:F3:C7:A0:5A:E5:8C:DA:04:FA:40:F5:49:21:D0:9E:E2:15\r\n"
            f"a=setup:actpass\r\n"
            f"a=mid:1\r\n"
            f"a=extmap:14 urn:ietf:params:rtp-hdrext:toffset\r\n"
            f"a=extmap:2 http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time\r\n"
            f"a=extmap:13 urn:3gpp:video-orientation\r\n"
            f"a=extmap:3 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01\r\n"
            f"a=extmap:5 http://www.webrtc.org/experiments/rtp-hdrext/playout-delay\r\n"
            f"a=extmap:6 http://www.webrtc.org/experiments/rtp-hdrext/video-content-type\r\n"
            f"a=extmap:7 http://www.webrtc.org/experiments/rtp-hdrext/video-timing\r\n"
            f"a=extmap:8 http://www.webrtc.org/experiments/rtp-hdrext/color-space\r\n"
            f"a=extmap:4 urn:ietf:params:rtp-hdrext:sdes:mid\r\n"
            f"a=extmap:10 urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id\r\n"
            f"a=extmap:11 urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id\r\n"
            f"a=recvonly\r\n"
            f"a=rtcp-mux\r\n"
            f"a=rtcp-rsize\r\n"
            f"a=rtpmap:96 VP8/90000\r\n"
            f"a=rtcp-fb:96 goog-remb\r\n"
            f"a=rtcp-fb:96 transport-cc\r\n"
            f"a=rtcp-fb:96 ccm fir\r\n"
            f"a=rtcp-fb:96 nack\r\n"
            f"a=rtcp-fb:96 nack pli\r\n"
            f"a=rtpmap:97 rtx/90000\r\n"
            f"a=fmtp:97 apt=96\r\n"
            f"a=rtpmap:98 VP9/90000\r\n"
            f"a=rtcp-fb:98 goog-remb\r\n"
            f"a=rtcp-fb:98 transport-cc\r\n"
            f"a=rtcp-fb:98 ccm fir\r\n"
            f"a=rtcp-fb:98 nack\r\n"
            f"a=rtcp-fb:98 nack pli\r\n"
            f"a=fmtp:98 profile-id=0\r\n"
            f"a=rtpmap:99 rtx/90000\r\n"
            f"a=fmtp:99 apt=98\r\n"
            f"a=rtpmap:100 VP9/90000\r\n"
            f"a=rtcp-fb:100 goog-remb\r\n"
            f"a=rtcp-fb:100 transport-cc\r\n"
            f"a=rtcp-fb:100 ccm fir\r\n"
            f"a=rtcp-fb:100 nack\r\n"
            f"a=rtcp-fb:100 nack pli\r\n"
            f"a=fmtp:100 profile-id=2\r\n"
            f"a=rtpmap:101 rtx/90000\r\n"
            f"a=fmtp:101 apt=100\r\n"
            f"a=rtpmap:35 VP9/90000\r\n"
            f"a=rtcp-fb:35 goog-remb\r\n"
            f"a=rtcp-fb:35 transport-cc\r\n"
            f"a=rtcp-fb:35 ccm fir\r\n"
            f"a=rtcp-fb:35 nack\r\n"
            f"a=rtcp-fb:35 nack pli\r\n"
            f"a=fmtp:35 profile-id=1\r\n"
            f"a=rtpmap:36 rtx/90000\r\n"
            f"a=fmtp:36 apt=35\r\n"
            f"a=rtpmap:37 VP9/90000\r\n"
            f"a=rtcp-fb:37 goog-remb\r\n"
            f"a=rtcp-fb:37 transport-cc\r\n"
            f"a=rtcp-fb:37 ccm fir\r\n"
            f"a=rtcp-fb:37 nack\r\n"
            f"a=rtcp-fb:37 nack pli\r\n"
            f"a=fmtp:37 profile-id=3\r\n"
            f"a=rtpmap:38 rtx/90000\r\n"
            f"a=fmtp:38 apt=37\r\n"
            f"a=rtpmap:103 H264/90000\r\n"
            f"a=rtcp-fb:103 goog-remb\r\n"
            f"a=rtcp-fb:103 transport-cc\r\n"
            f"a=rtcp-fb:103 ccm fir\r\n"
            f"a=rtcp-fb:103 nack\r\n"
            f"a=rtcp-fb:103 nack pli\r\n"
            f"a=fmtp:103 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f\r\n"
            f"a=rtpmap:104 rtx/90000\r\n"
            f"a=fmtp:104 apt=103\r\n"
            f"a=rtpmap:107 H264/90000\r\n"
            f"a=rtcp-fb:107 goog-remb\r\n"
            f"a=rtcp-fb:107 transport-cc\r\n"
            f"a=rtcp-fb:107 ccm fir\r\n"
            f"a=rtcp-fb:107 nack\r\n"
            f"a=rtcp-fb:107 nack pli\r\n"
            f"a=fmtp:107 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f\r\n"
            f"a=rtpmap:108 rtx/90000\r\n"
            f"a=fmtp:108 apt=107\r\n"
            f"a=rtpmap:109 H264/90000\r\n"
            f"a=rtcp-fb:109 goog-remb\r\n"
            f"a=rtcp-fb:109 transport-cc\r\n"
            f"a=rtcp-fb:109 ccm fir\r\n"
            f"a=rtcp-fb:109 nack\r\n"
            f"a=rtcp-fb:109 nack pli\r\n"
            f"a=fmtp:109 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f\r\n"
            f"a=rtpmap:114 rtx/90000\r\n"
            f"a=fmtp:114 apt=109\r\n"
            f"a=rtpmap:115 H264/90000\r\n"
            f"a=rtcp-fb:115 goog-remb\r\n"
            f"a=rtcp-fb:115 transport-cc\r\n"
            f"a=rtcp-fb:115 ccm fir\r\n"
            f"a=rtcp-fb:115 nack\r\n"
            f"a=rtcp-fb:115 nack pli\r\n"
            f"a=fmtp:115 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f\r\n"
            f"a=rtpmap:116 rtx/90000\r\n"
            f"a=fmtp:116 apt=115\r\n"
            f"a=rtpmap:117 H264/90000\r\n"
            f"a=rtcp-fb:117 goog-remb\r\n"
            f"a=rtcp-fb:117 transport-cc\r\n"
            f"a=rtcp-fb:117 ccm fir\r\n"
            f"a=rtcp-fb:117 nack\r\n"
            f"a=rtcp-fb:117 nack pli\r\n"
            f"a=fmtp:117 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=4d001f\r\n"
            f"a=rtpmap:118 rtx/90000\r\n"
            f"a=fmtp:118 apt=117\r\n"
            f"a=rtpmap:39 H264/90000\r\n"
            f"a=rtcp-fb:39 goog-remb\r\n"
            f"a=rtcp-fb:39 transport-cc\r\n"
            f"a=rtcp-fb:39 ccm fir\r\n"
            f"a=rtcp-fb:39 nack\r\n"
            f"a=rtcp-fb:39 nack pli\r\n"
            f"a=fmtp:39 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=4d001f\r\n"
            f"a=rtpmap:40 rtx/90000\r\n"
            f"a=fmtp:40 apt=39\r\n"
            f"a=rtpmap:41 H264/90000\r\n"
            f"a=rtcp-fb:41 goog-remb\r\n"
            f"a=rtcp-fb:41 transport-cc\r\n"
            f"a=rtcp-fb:41 ccm fir\r\n"
            f"a=rtcp-fb:41 nack\r\n"
            f"a=rtcp-fb:41 nack pli\r\n"
            f"a=fmtp:41 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=f4001f\r\n"
            f"a=rtpmap:42 rtx/90000\r\n"
            f"a=fmtp:42 apt=41\r\n"
            f"a=rtpmap:43 H264/90000\r\n"
            f"a=rtcp-fb:43 goog-remb\r\n"
            f"a=rtcp-fb:43 transport-cc\r\n"
            f"a=rtcp-fb:43 ccm fir\r\n"
            f"a=rtcp-fb:43 nack\r\n"
            f"a=rtcp-fb:43 nack pli\r\n"
            f"a=fmtp:43 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=f4001f\r\n"
            f"a=rtpmap:44 rtx/90000\r\n"
            f"a=fmtp:44 apt=43\r\n"
            f"a=rtpmap:45 AV1/90000\r\n"
            f"a=rtcp-fb:45 goog-remb\r\n"
            f"a=rtcp-fb:45 transport-cc\r\n"
            f"a=rtcp-fb:45 ccm fir\r\n"
            f"a=rtcp-fb:45 nack\r\n"
            f"a=rtcp-fb:45 nack pli\r\n"
            f"a=fmtp:45 level-idx=5;profile=0;tier=0\r\n"
            f"a=rtpmap:46 rtx/90000\r\n"
            f"a=fmtp:46 apt=45\r\n"
            f"a=rtpmap:47 AV1/90000\r\n"
            f"a=rtcp-fb:47 goog-remb\r\n"
            f"a=rtcp-fb:47 transport-cc\r\n"
            f"a=rtcp-fb:47 ccm fir\r\n"
            f"a=rtcp-fb:47 nack\r\n"
            f"a=rtcp-fb:47 nack pli\r\n"
            f"a=fmtp:47 level-idx=5;profile=1;tier=0\r\n"
            f"a=rtpmap:48 rtx/90000\r\n"
            f"a=fmtp:48 apt=47\r\n"
            f"a=rtpmap:119 H264/90000\r\n"
            f"a=rtcp-fb:119 goog-remb\r\n"
            f"a=rtcp-fb:119 transport-cc\r\n"
            f"a=rtcp-fb:119 ccm fir\r\n"
            f"a=rtcp-fb:119 nack\r\n"
            f"a=rtcp-fb:119 nack pli\r\n"
            f"a=fmtp:119 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=64001f\r\n"
            f"a=rtpmap:120 rtx/90000\r\n"
            f"a=fmtp:120 apt=119\r\n"
            f"a=rtpmap:121 H264/90000\r\n"
            f"a=rtcp-fb:121 goog-remb\r\n"
            f"a=rtcp-fb:121 transport-cc\r\n"
            f"a=rtcp-fb:121 ccm fir\r\n"
            f"a=rtcp-fb:121 nack\r\n"
            f"a=rtcp-fb:121 nack pli\r\n"
            f"a=fmtp:121 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=64001f\r\n"
            f"a=rtpmap:122 rtx/90000\r\n"
            f"a=fmtp:122 apt=121\r\n"
            f"a=rtpmap:49 H265/90000\r\n"
            f"a=rtcp-fb:49 goog-remb\r\n"
            f"a=rtcp-fb:49 transport-cc\r\n"
            f"a=rtcp-fb:49 ccm fir\r\n"
            f"a=rtcp-fb:49 nack\r\n"
            f"a=rtcp-fb:49 nack pli\r\n"
            f"a=fmtp:49 level-id=180;profile-id=1;tier-flag=0;tx-mode=SRST\r\n"
            f"a=rtpmap:50 rtx/90000\r\n"
            f"a=fmtp:50 apt=49\r\n"
            f"a=rtpmap:51 H265/90000\r\n"
            f"a=rtcp-fb:51 goog-remb\r\n"
            f"a=rtcp-fb:51 transport-cc\r\n"
            f"a=rtcp-fb:51 ccm fir\r\n"
            f"a=rtcp-fb:51 nack\r\n"
            f"a=rtcp-fb:51 nack pli\r\n"
            f"a=fmtp:51 level-id=180;profile-id=2;tier-flag=0;tx-mode=SRST\r\n"
            f"a=rtpmap:52 rtx/90000\r\n"
            f"a=fmtp:52 apt=51\r\n"
            f"a=rtpmap:123 red/90000\r\n"
            f"a=rtpmap:124 rtx/90000\r\n"
            f"a=fmtp:124 apt=123\r\n"
            f"a=rtpmap:125 ulpfec/90000\r\n"
            f"a=rtpmap:53 flexfec-03/90000\r\n"
            f"a=rtcp-fb:53 goog-remb\r\n"
            f"a=rtcp-fb:53 transport-cc\r\n"
            f"a=fmtp:53 repair-window=10000000\r\n"
        )

        # Create a queue to receive the WebRTC messages
        message_queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        def message_handler(message: RingWebRtcMessage) -> None:
            """Handle WebRTC messages and capture first frame."""
            if message.error_code:
                message_queue.put_nowait(None)
            elif message.answer:
                # TODO: Implement frame capture from WebRTC stream
                message_queue.put_nowait(None)  # Signal no image for now
            else:
                print("Unknown message type", message)

        try:
            # Only create a new stream if one does not already exist
            if not self._has_webrtc_stream:
                self._has_webrtc_stream = True
                await self._device.generate_async_webrtc_stream(
                    offer_sdp, session_id, message_handler, keep_alive_timeout=None
                )
            else:
                _LOGGER.debug(f"WebRTC stream for session {session_id} already exists.")

            # Wait for image capture
            try:
                async with asyncio.timeout(20):
                    image = await message_queue.get()
                    return image
            except asyncio.TimeoutError:
                print("Timeout waiting for camera image")
                return None
            finally:
                self._has_webrtc_stream = False

        except Exception as ex:
            _LOGGER.error("Failed to get camera image: %s", str(ex))
            return None
        finally:
            # Ensure WebRTC session is cleaned up
            self._device.sync_close_webrtc_stream(session_id)

    async def handle_async_mjpeg_stream(
        self, request: web.Request
    ) -> web.StreamResponse | None:
        """Generate an HTTP MJPEG stream from the camera."""
        if self._video_url is None:
            return None

        stream = CameraMjpeg(self._ffmpeg_manager.binary)
        await stream.open_camera(self._video_url)

        try:
            stream_reader = await stream.get_reader()
            return await async_aiohttp_proxy_stream(
                self.hass,
                request,
                stream_reader,
                self._ffmpeg_manager.ffmpeg_stream_content_type,
            )
        finally:
            await stream.close()

    async def async_handle_async_webrtc_offer(
        self, offer_sdp: str, session_id: str, send_message: WebRTCSendMessage
    ) -> None:
        """Return the source of the stream."""

        def message_wrapper(ring_message: RingWebRtcMessage) -> None:
            if ring_message.error_code:
                msg = ring_message.error_message or ""
                send_message(WebRTCError(ring_message.error_code, msg))
            elif ring_message.answer:
                send_message(WebRTCAnswer(ring_message.answer))
            elif ring_message.candidate:
                send_message(
                    WebRTCCandidate(
                        RTCIceCandidateInit(
                            ring_message.candidate,
                            sdp_m_line_index=ring_message.sdp_m_line_index or 0,
                        )
                    )
                )

        return await self._device.generate_async_webrtc_stream(
            offer_sdp, session_id, message_wrapper, keep_alive_timeout=None
        )

    async def async_on_webrtc_candidate(
        self, session_id: str, candidate: RTCIceCandidateInit
    ) -> None:
        """Handle a WebRTC candidate."""
        if candidate.sdp_m_line_index is None:
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="sdp_m_line_index_required",
                translation_placeholders={
                    "device": self._device.name,
                },
            )
        await self._device.on_webrtc_candidate(
            session_id, candidate.candidate, candidate.sdp_m_line_index
        )

    @callback
    def close_webrtc_session(self, session_id: str) -> None:
        """Close a WebRTC session."""
        self._device.sync_close_webrtc_stream(session_id)

    async def async_update(self) -> None:
        """Update camera entity and refresh attributes."""
        if (
            self._device.has_capability(MOTION_DETECTION_CAPABILITY)
            and self._attr_motion_detection_enabled != self._device.motion_detection
        ):
            self._attr_motion_detection_enabled = self._device.motion_detection
            self.async_write_ha_state()

        if TYPE_CHECKING:
            # _last_event is set before calling update so will never be None
            assert self._last_event

        if self._last_event["recording"]["status"] != "ready":
            return

        utcnow = dt_util.utcnow()
        if self._last_video_id == self._last_event["id"] and utcnow <= self._expires_at:
            return

        if self._last_video_id != self._last_event["id"]:
            self._images = {}

        self._video_url = await self._async_get_video()

        self._last_video_id = self._last_event["id"]
        self._expires_at = FORCE_REFRESH_INTERVAL + utcnow

    @exception_wrap
    async def _async_get_video(self) -> str | None:
        if TYPE_CHECKING:
            # _last_event is set before calling update so will never be None
            assert self._last_event
        event_id = self._last_event.get("id")
        assert event_id and isinstance(event_id, int)
        return await self._device.async_recording_url(event_id)

    @exception_wrap
    async def _async_set_motion_detection_enabled(self, new_state: bool) -> None:
        if not self._device.has_capability(MOTION_DETECTION_CAPABILITY):
            _LOGGER.error(
                "Entity %s does not have motion detection capability", self.entity_id
            )
            return

        await self._device.async_set_motion_detection(new_state)
        self._attr_motion_detection_enabled = new_state
        self.async_write_ha_state()

    async def async_enable_motion_detection(self) -> None:
        """Enable motion detection in the camera."""
        await self._async_set_motion_detection_enabled(True)

    async def async_disable_motion_detection(self) -> None:
        """Disable motion detection in camera."""
        await self._async_set_motion_detection_enabled(False)
