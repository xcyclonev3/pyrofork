#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrofork.
#
#  Pyrofork is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrofork is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrofork.  If not, see <http://www.gnu.org/licenses/>.

"""Shared helpers for the offline benchmarks: pick the source tree, time things, build server packets."""
import logging
import os
import sys
import time
from pathlib import Path

# The source tree to measure: this one, or another checkout (with its raw API generated) to compare with
TREE = os.environ.get("TREE") or str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, TREE)
logging.disable(logging.CRITICAL)

from hashlib import sha1, sha256  # noqa: E402

AUTH_KEY = bytes(range(256))
AUTH_KEY_ID = sha1(AUTH_KEY).digest()[-8:]
SESSION_ID = b"\x01" * 8


def timeit(func, seconds=0.6, min_runs=5):
    """Best of 3 batches: seconds per call."""
    best = float("inf")
    for _ in range(3):
        n, start = 0, time.perf_counter()
        while True:
            func()
            n += 1
            elapsed = time.perf_counter() - start
            if elapsed > seconds / 3 and n >= min_runs:
                break
        best = min(best, elapsed / n)
    return best


def server_pack(body, msg_id=None):
    """A packet as Telegram sends it (the inverse direction of mtproto.pack)."""
    from pyrogram.crypto import aes, mtproto
    from pyrogram.raw.core import Long, Message
    msg_id = msg_id or ((int(time.time()) << 32) | 1)
    data = Long(1) + SESSION_ID + Message(body, msg_id, 1, len(body.write())).write()
    padding = os.urandom(-(len(data) + 12) % 16 + 12)
    msg_key = sha256(AUTH_KEY[96:128] + data + padding).digest()[8:24]
    key, iv = mtproto.kdf(AUTH_KEY, msg_key, False)
    return AUTH_KEY_ID + msg_key + aes.ige256_encrypt(data + padding, key, iv)
