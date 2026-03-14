from __future__ import annotations

from io import BytesIO

from PIL import Image


def compute_image_phash(image_bytes: bytes, *, hash_size: int = 8) -> str:
    with Image.open(BytesIO(image_bytes)) as image:
        gray = image.convert("L").resize((hash_size, hash_size))
        pixels = list(gray.getdata())
    if not pixels:
        return ""
    avg = sum(int(p) for p in pixels) / len(pixels)
    bits = "".join("1" if int(p) >= avg else "0" for p in pixels)
    width = hash_size * hash_size // 4
    return f"{int(bits, 2):0{width}x}"


def hamming_distance(left: str, right: str) -> int:
    if not left or not right or len(left) != len(right):
        return 9999
    return sum(ch1 != ch2 for ch1, ch2 in zip(f"{int(left,16):0{len(left)*4}b}", f"{int(right,16):0{len(right)*4}b}"))
