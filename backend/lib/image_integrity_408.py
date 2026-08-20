#!/usr/bin/env python3
"""Deterministic full-byte validation for frozen 408 image evidence."""

from __future__ import annotations

import io
import struct
import warnings
import zlib
from typing import Final

try:
    from PIL import Image as _PILImage
except ImportError:  # pragma: no cover - exercised by fail-closed callers
    _PILImage = None


IMAGE_INTEGRITY_ALGORITHM: Final = "png-jpeg-webp-full-decode-v1"
SUPPORTED_IMAGE_FORMATS: Final = {
    "png": "image/png",
    "jpeg": "image/jpeg",
    "webp": "image/webp",
}


class ImageIntegrityError(RuntimeError):
    """The original bytes are unsupported, malformed, or not fully decodable."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _invalid() -> None:
    raise ImageIntegrityError("container_invalid")


def _detect_signature(data: bytes) -> tuple[str, str]:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png", "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "jpeg", "image/jpeg"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp", "image/webp"
    raise ImageIntegrityError("signature_unsupported")


def _validate_png_container(data: bytes) -> None:
    offset = 8
    chunk_index = 0
    saw_ihdr = False
    saw_idat = False
    while offset < len(data):
        if offset + 12 > len(data):
            _invalid()
        length = int.from_bytes(data[offset : offset + 4], "big")
        chunk_type = data[offset + 4 : offset + 8]
        if length > 0x7FFFFFFF or any(
            not (65 <= value <= 90 or 97 <= value <= 122)
            for value in chunk_type
        ):
            _invalid()
        chunk_end = offset + 12 + length
        if chunk_end > len(data):
            _invalid()
        payload = data[offset + 8 : offset + 8 + length]
        expected_crc = int.from_bytes(
            data[offset + 8 + length : chunk_end], "big"
        )
        actual_crc = zlib.crc32(chunk_type)
        actual_crc = zlib.crc32(payload, actual_crc) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            _invalid()
        if chunk_index == 0 and (chunk_type != b"IHDR" or length != 13):
            _invalid()
        if chunk_type == b"IHDR":
            if saw_ihdr or length != 13:
                _invalid()
            width, height = struct.unpack(">II", payload[:8])
            if width == 0 or height == 0:
                _invalid()
            saw_ihdr = True
        elif not saw_ihdr:
            _invalid()
        if chunk_type == b"IDAT":
            saw_idat = True
        if chunk_type == b"IEND":
            if length != 0 or not saw_idat or chunk_end != len(data):
                _invalid()
            return
        offset = chunk_end
        chunk_index += 1
    _invalid()


def _validate_jpeg_container(data: bytes) -> None:
    if len(data) < 4 or data[:2] != b"\xff\xd8":
        _invalid()
    offset = 2
    in_scan = False
    saw_scan = False
    while offset < len(data):
        if in_scan:
            marker_start = data.find(b"\xff", offset)
            if marker_start < 0:
                _invalid()
            offset = marker_start
        elif data[offset] != 0xFF:
            _invalid()
        while offset < len(data) and data[offset] == 0xFF:
            offset += 1
        if offset >= len(data):
            _invalid()
        marker = data[offset]
        offset += 1
        if in_scan and (marker == 0x00 or 0xD0 <= marker <= 0xD7):
            continue
        in_scan = False
        if marker == 0xD9:
            if not saw_scan or offset != len(data):
                _invalid()
            return
        if marker in {0x00, 0xD8}:
            _invalid()
        if marker == 0x01 or 0xD0 <= marker <= 0xD7:
            continue
        if offset + 2 > len(data):
            _invalid()
        segment_length = int.from_bytes(data[offset : offset + 2], "big")
        if segment_length < 2:
            _invalid()
        segment_end = offset + segment_length
        if segment_end > len(data):
            _invalid()
        offset = segment_end
        if marker == 0xDA:
            saw_scan = True
            in_scan = True
    _invalid()


def _webp_chunks(
    data: bytes, start: int, end: int
) -> list[tuple[bytes, bytes]]:
    chunks: list[tuple[bytes, bytes]] = []
    offset = start
    while offset < end:
        if offset + 8 > len(data):
            _invalid()
        chunk_type = data[offset : offset + 4]
        chunk_length = int.from_bytes(data[offset + 4 : offset + 8], "little")
        payload_end = offset + 8 + chunk_length
        padded_end = payload_end + (chunk_length & 1)
        if payload_end > end or padded_end > end:
            _invalid()
        if chunk_length & 1 and data[payload_end] != 0:
            _invalid()
        chunks.append((chunk_type, data[offset + 8 : payload_end]))
        offset = padded_end
    if offset != end or not chunks:
        _invalid()
    return chunks


def _validate_webp_frame_payload(payload: bytes) -> None:
    if len(payload) < 16:
        _invalid()
    chunks = _webp_chunks(payload, 16, len(payload))
    types = [chunk_type for chunk_type, _ in chunks]
    bitstreams = [value for value in types if value in {b"VP8 ", b"VP8L"}]
    if len(bitstreams) != 1 or any(
        value not in {b"ALPH", b"VP8 ", b"VP8L"} for value in types
    ):
        _invalid()
    if types.count(b"ALPH") > 1:
        _invalid()
    if b"ALPH" in types and (
        bitstreams[0] != b"VP8 "
        or types.index(b"ALPH") + 1 != types.index(b"VP8 ")
    ):
        _invalid()


def _validate_webp_container(data: bytes) -> None:
    if len(data) < 20 or int.from_bytes(data[4:8], "little") != len(data) - 8:
        _invalid()
    chunks = _webp_chunks(data, 12, len(data))
    types = [chunk_type for chunk_type, _ in chunks]
    if types[0] in {b"VP8 ", b"VP8L"}:
        if len(types) != 1:
            _invalid()
        return
    if types[0] != b"VP8X" or types.count(b"VP8X") != 1:
        _invalid()
    vp8x_payload = chunks[0][1]
    if (
        len(vp8x_payload) != 10
        or vp8x_payload[0] & 0xC1
        or vp8x_payload[1:4] != b"\x00\x00\x00"
    ):
        _invalid()
    animation = bool(vp8x_payload[0] & 0x02)
    if animation:
        if (
            types.count(b"ANIM") != 1
            or types.count(b"ANMF") < 1
            or any(value in types for value in {b"ALPH", b"VP8 ", b"VP8L"})
        ):
            _invalid()
        for chunk_type, payload in chunks:
            if chunk_type == b"ANMF":
                _validate_webp_frame_payload(payload)
    else:
        if any(value in types for value in {b"ANIM", b"ANMF"}):
            _invalid()
        bitstreams = [
            value for value in types if value in {b"VP8 ", b"VP8L"}
        ]
        if len(bitstreams) != 1 or types.count(b"ALPH") > 1:
            _invalid()
        if b"ALPH" in types and (
            bitstreams[0] != b"VP8 "
            or types.index(b"ALPH") + 1 != types.index(b"VP8 ")
        ):
            _invalid()
    for metadata_type in (b"ICCP", b"EXIF", b"XMP "):
        if types.count(metadata_type) > 1:
            _invalid()


def _decode_all_frames(data: bytes, detected_format: str) -> None:
    if _PILImage is None:
        raise ImageIntegrityError("decoder_unavailable")
    expected = {"png": "PNG", "jpeg": "JPEG", "webp": "WEBP"}[
        detected_format
    ]
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", _PILImage.DecompressionBombWarning)
            with _PILImage.open(io.BytesIO(data)) as image:
                if image.format != expected:
                    raise ImageIntegrityError("decoder_format_mismatch")
                image.verify()
            with _PILImage.open(io.BytesIO(data)) as image:
                if image.format != expected:
                    raise ImageIntegrityError("decoder_format_mismatch")
                frame_count = int(getattr(image, "n_frames", 1))
                if frame_count < 1:
                    raise ImageIntegrityError("decode_invalid")
                for frame_index in range(frame_count):
                    image.seek(frame_index)
                    image.load()
    except ImageIntegrityError:
        raise
    except Exception as exc:
        raise ImageIntegrityError("decode_invalid") from exc


def validate_image_bytes(data: bytes) -> tuple[str, str]:
    """Validate the exact container bytes and fully decode every image frame."""

    if not isinstance(data, bytes) or not data:
        raise ImageIntegrityError("bytes_invalid")
    detected_format, detected_mime = _detect_signature(data)
    if detected_format == "png":
        _validate_png_container(data)
    elif detected_format == "jpeg":
        _validate_jpeg_container(data)
    else:
        _validate_webp_container(data)
    _decode_all_frames(data, detected_format)
    return detected_format, detected_mime


__all__ = [
    "IMAGE_INTEGRITY_ALGORITHM",
    "ImageIntegrityError",
    "SUPPORTED_IMAGE_FORMATS",
    "validate_image_bytes",
]
