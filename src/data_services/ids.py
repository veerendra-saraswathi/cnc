"""RFC 9562 UUID version 7.

The outbox primary key is a UUID v7 so rows sort by creation time.
Python 3.13's uuid module has no uuid7(); 3.14 added it.
"""

from __future__ import annotations

import os
import time
import uuid


def uuid7() -> str:
    unix_ms = int(time.time() * 1000) & 0xFFFFFFFFFFFF
    randomness = int.from_bytes(os.urandom(10), "big")
    rand_a = (randomness >> 62) & 0xFFF
    rand_b = randomness & ((1 << 62) - 1)
    value = (
        (unix_ms << 80)
        | (0x7 << 76)
        | (rand_a << 64)
        | (0b10 << 62)
        | rand_b
    )
    parsed = uuid.UUID(int=value)
    if parsed.version != 7:
        raise RuntimeError("uuid7 did not set version 7")
    return str(parsed)
