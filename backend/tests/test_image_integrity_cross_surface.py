from __future__ import annotations

import hashlib
import importlib.util
import io
import sys
import tempfile
import unittest
from pathlib import Path

from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import image_integrity_408 as release_integrity  # noqa: E402
import preprocessor_core as core  # noqa: E402


REPO_INTEGRITY_PATH = Path(
    "/Users/xiazhibin/Documents/kaoyan-408/scripts/image_integrity_408.py"
)


def encoded_image(image_format: str) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (3, 2), color=(12, 34, 56)).save(
        output, format=image_format
    )
    return output.getvalue()


def webp_chunk_spans(data: bytes) -> list[tuple[bytes, int, int]]:
    result: list[tuple[bytes, int, int]] = []
    offset = 12
    while offset < len(data):
        size = int.from_bytes(data[offset + 4 : offset + 8], "little")
        end = offset + 8 + size + (size & 1)
        result.append((data[offset : offset + 4], offset, end))
        offset = end
    return result


def bind_webp_riff_size(data: bytes) -> bytes:
    value = bytearray(data)
    value[4:8] = (len(value) - 8).to_bytes(4, "little")
    return bytes(value)


class ImageIntegrityCrossSurfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        spec = importlib.util.spec_from_file_location(
            "repo_image_integrity_408", REPO_INTEGRITY_PATH
        )
        if spec is None or spec.loader is None:
            raise AssertionError("408 image integrity module is unavailable")
        cls.repo_integrity = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.repo_integrity)

    def test_release_and_408_producer_use_identical_algorithm_bytes(self) -> None:
        release_path = ROOT / "lib/image_integrity_408.py"
        self.assertEqual(
            hashlib.sha256(release_path.read_bytes()).hexdigest(),
            hashlib.sha256(REPO_INTEGRITY_PATH.read_bytes()).hexdigest(),
        )
        self.assertEqual(
            release_integrity.IMAGE_INTEGRITY_ALGORITHM,
            self.repo_integrity.IMAGE_INTEGRITY_ALGORITHM,
        )

    def test_png_jpeg_webp_valid_and_corrupt_vectors_match(self) -> None:
        valid = {
            "png": encoded_image("PNG"),
            "jpeg": encoded_image("JPEG"),
            "webp": encoded_image("WEBP"),
        }
        expected_mime = {
            "png": "image/png",
            "jpeg": "image/jpeg",
            "webp": "image/webp",
        }
        for name, data in valid.items():
            with self.subTest(name=name, state="valid"):
                expected = (name, expected_mime[name])
                self.assertEqual(
                    release_integrity.validate_image_bytes(data), expected
                )
                self.assertEqual(
                    self.repo_integrity.validate_image_bytes(data), expected
                )

        corrupt_png = bytearray(valid["png"])
        corrupt_png[-8] ^= 0x01
        invalid = [
            valid["png"][:-8],
            bytes(corrupt_png),
            valid["jpeg"][:-2],
            valid["jpeg"] + b"trailing",
            valid["webp"][:-4],
            b"not-an-image",
        ]
        for index, data in enumerate(invalid):
            release_code = None
            repo_code = None
            try:
                release_integrity.validate_image_bytes(data)
            except release_integrity.ImageIntegrityError as exc:
                release_code = exc.code
            try:
                self.repo_integrity.validate_image_bytes(data)
            except self.repo_integrity.ImageIntegrityError as exc:
                repo_code = exc.code
            with self.subTest(index=index, state="invalid"):
                self.assertIsNotNone(release_code)
                self.assertEqual(release_code, repo_code)

    def test_extended_alpha_metadata_and_animated_webp_are_valid(self) -> None:
        alpha_output = io.BytesIO()
        Image.new("RGBA", (4, 4), (20, 30, 40, 100)).save(
            alpha_output, format="WEBP", lossless=False, quality=80
        )
        metadata_output = io.BytesIO()
        Image.new("RGB", (4, 4), (10, 20, 30)).save(
            metadata_output,
            format="WEBP",
            exif=b"Exif\x00\x00bounded-test",
        )
        animation_output = io.BytesIO()
        frames = [
            Image.new("RGBA", (4, 4), (255, 0, 0, 180)),
            Image.new("RGBA", (4, 4), (0, 255, 0, 120)),
        ]
        frames[0].save(
            animation_output,
            format="WEBP",
            save_all=True,
            append_images=frames[1:],
            duration=100,
            loop=0,
            lossless=False,
        )
        for name, data in (
            ("alpha", alpha_output.getvalue()),
            ("metadata", metadata_output.getvalue()),
            ("animation", animation_output.getvalue()),
        ):
            with self.subTest(name=name):
                self.assertEqual(
                    release_integrity.validate_image_bytes(data),
                    ("webp", "image/webp"),
                )
                self.assertEqual(
                    self.repo_integrity.validate_image_bytes(data),
                    ("webp", "image/webp"),
                )

        alpha_data = alpha_output.getvalue()
        bitstream = next(
            (start, end)
            for chunk_type, start, end in webp_chunk_spans(alpha_data)
            if chunk_type in {b"VP8 ", b"VP8L"}
        )
        missing_payload = bind_webp_riff_size(alpha_data[: bitstream[0]])
        duplicate_payload = bind_webp_riff_size(
            alpha_data + alpha_data[bitstream[0] : bitstream[1]]
        )
        for name, data in (
            ("missing-payload", missing_payload),
            ("duplicate-payload", duplicate_payload),
        ):
            with self.subTest(name=name):
                with self.assertRaises(
                    release_integrity.ImageIntegrityError
                ):
                    release_integrity.validate_image_bytes(data)
                with self.assertRaises(self.repo_integrity.ImageIntegrityError):
                    self.repo_integrity.validate_image_bytes(data)

    def test_core_rejects_corruption_before_any_luna_runner_call(self) -> None:
        calls = 0
        corrupt = encoded_image("PNG")[:-8]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "attachment.bin"
            path.write_bytes(corrupt)
            with self.assertRaisesRegex(
                core.PreprocessorError,
                "current_question_attachment_integrity_invalid",
            ):
                core._detect_image_format(path)
        self.assertEqual(calls, 0)


if __name__ == "__main__":
    unittest.main()
