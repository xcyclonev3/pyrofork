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

"""MTProto packing and unpacking give exactly the results (and the errors) of the previous implementation."""

import random
from hashlib import sha1, sha256
from io import BytesIO

import pytest

from pyrogram import raw
from pyrogram.crypto import aes, mtproto
from pyrogram.crypto.mtproto import kdf
from pyrogram.errors import SecurityCheckMismatch
from pyrogram.raw.core import Long, Message


def old_pack(message, salt, session_id, auth_key, auth_key_id, urandom):
    """mtproto.pack before it avoided copies."""
    data = Long(salt) + session_id + message.write()
    padding = urandom(-(len(data) + 12) % 16 + 12)
    msg_key_large = sha256(auth_key[88: 88 + 32] + data + padding).digest()
    msg_key = msg_key_large[8:24]
    aes_key, aes_iv = kdf(auth_key, msg_key, True)
    return auth_key_id + msg_key + aes.ige256_encrypt(data + padding, aes_key, aes_iv)


def old_unpack(b, session_id, auth_key, auth_key_id, result_vectors=None):
    """mtproto.unpack before it avoided copies."""
    SecurityCheckMismatch.check(b.read(8) == auth_key_id, "b.read(8) == auth_key_id")
    msg_key = b.read(16)
    aes_key, aes_iv = kdf(auth_key, msg_key, False)
    data = BytesIO(aes.ige256_decrypt(b.read(), aes_key, aes_iv))
    data.read(8)
    SecurityCheckMismatch.check(data.read(8) == session_id, "data.read(8) == session_id")
    try:
        message = Message.read(data, result_vectors) if result_vectors else Message.read(data)
    except KeyError as e:
        if e.args[0] == 0:
            raise ConnectionError("Received empty data. Check your internet connection.")
        left = data.read().hex()
        left = [left[i:i + 64] for i in range(0, len(left), 64)]
        left = [[left[i:i + 8] for i in range(0, len(left), 8)] for left in left]
        left = "\n".join(" ".join(x for x in left) for left in left)
        raise ValueError(f"The server sent an unknown constructor: {hex(e.args[0])}\n{left}")
    SecurityCheckMismatch.check(
        msg_key == sha256(auth_key[96:96 + 32] + data.getvalue()).digest()[8:24],
        "msg_key == sha256(auth_key[96:96 + 32] + data.getvalue()).digest()[8:24]"
    )
    data.seek(32)
    payload = data.read()
    padding = payload[message.length:]
    SecurityCheckMismatch.check(12 <= len(padding) <= 1024, "12 <= len(padding) <= 1024")
    SecurityCheckMismatch.check(len(payload) % 4 == 0, "len(payload) % 4 == 0")
    SecurityCheckMismatch.check(message.msg_id % 2 != 0, "message.msg_id % 2 != 0")
    return message


class Body:
    """A message body already serialized."""

    def __init__(self, data):
        self.data = data

    def write(self, *args):
        return self.data


def keys(rng):
    auth_key = rng.randbytes(256)
    return auth_key, sha1(auth_key).digest()[-8:]


SIZES = [0, 4, 12, 16, 28, 100, 1000, 4096, 4100, 65532, 65536, 524288, 1048576]


@pytest.mark.parametrize("seed", range(40))
def test_pack_is_unchanged(monkeypatch, seed):
    rng = random.Random(seed)
    auth_key, auth_key_id = keys(rng)

    for size in SIZES + [rng.randrange(0, 70000) // 4 * 4 for _ in range(10)]:
        body = Body(rng.randbytes(size))
        message = Message(body, rng.getrandbits(63), rng.getrandbits(31), size)
        salt, session_id = rng.getrandbits(63), rng.randbytes(8)
        padding_seed = rng.getrandbits(32)

        def urandom(n):
            return random.Random(padding_seed).randbytes(n)

        monkeypatch.setattr(mtproto, "urandom", urandom)
        new = mtproto.pack(message, salt, session_id, auth_key, auth_key_id)
        assert new == old_pack(message, salt, session_id, auth_key, auth_key_id, urandom)


def server_packet(rng, body_data, auth_key, auth_key_id, session_id, msg_id=None, length=None, padding=None):
    """A packet as the server sends it, with the given message fields and padding (valid msg_key)."""
    msg_id = rng.getrandbits(62) * 2 + 1 if msg_id is None else msg_id
    length = len(body_data) if length is None else length
    message = msg_id.to_bytes(8, "little") + rng.getrandbits(31).to_bytes(4, "little") + \
        length.to_bytes(4, "little", signed=True) + body_data
    data = rng.randbytes(8) + session_id + message
    padding = rng.randbytes(-(len(data) + 12) % 16 + 12) if padding is None else padding
    msg_key = sha256(auth_key[96:128] + data + padding).digest()[8:24]
    aes_key, aes_iv = kdf(auth_key, msg_key, False)
    return auth_key_id + msg_key + aes.ige256_encrypt(data + padding, aes_key, aes_iv)


def outcome(unpack, packet, *args):
    try:
        return unpack(BytesIO(packet), *args)
    except Exception as e:
        return type(e), str(e)


@pytest.mark.parametrize("seed", range(40))
def test_unpack_is_unchanged(seed):
    rng = random.Random(seed)
    auth_key, auth_key_id = keys(rng)
    session_id = rng.randbytes(8)
    args = (session_id, auth_key, auth_key_id)

    for size in [0, 1, 100, 4096, 65536, 524288, rng.randrange(0, 70000)]:
        body = raw.types.RpcResult(req_msg_id=rng.getrandbits(62), result=raw.types.upload.File(
            type=raw.types.storage.FilePartial(), mtime=rng.getrandbits(31), bytes=rng.randbytes(size)))
        packet = server_packet(rng, body.write(), *args[1:], session_id)
        new, old = outcome(mtproto.unpack, packet, *args), outcome(old_unpack, packet, *args)
        assert isinstance(new, Message) and new == old
        assert (new.msg_id, new.seq_no, new.length) == (old.msg_id, old.seq_no, old.length)


def malformed_packets(rng, auth_key, auth_key_id, session_id):
    body = raw.types.Pong(msg_id=rng.getrandbits(62), ping_id=rng.getrandbits(62)).write()
    good = server_packet(rng, body, auth_key, auth_key_id, session_id)
    yield "auth key id", rng.randbytes(8) + good[8:]
    yield "session id", server_packet(rng, body, auth_key, auth_key_id, rng.randbytes(8))
    yield "msg_key", good[:8] + rng.randbytes(16) + good[24:]
    yield "tampered", good[:-16] + rng.randbytes(16)
    yield "even msg_id", server_packet(rng, body, auth_key, auth_key_id, session_id, msg_id=rng.getrandbits(62) * 2)
    for padding in (0, 4, 8, 11, 12, 1024, 1025, 1028, 2048):
        # Filler after the body, counted in the length, keeps the data a multiple of 16 bytes
        filler = rng.randbytes(-(32 + len(body) + padding) % 16)
        yield f"padding {padding}", server_packet(rng, body + filler, auth_key, auth_key_id, session_id,
                                                  padding=rng.randbytes(padding))
    for delta in (-8, -4, 4, 8, 2000):
        yield f"length {delta:+}", server_packet(rng, body, auth_key, auth_key_id, session_id,
                                                 length=len(body) + delta)
    yield "negative length", server_packet(rng, body, auth_key, auth_key_id, session_id, length=-20)
    yield "unknown constructor", server_packet(rng, b"\x11\x22\x33\x44" + rng.randbytes(20), auth_key,
                                               auth_key_id, session_id)


@pytest.mark.parametrize("seed", range(30))
def test_unpack_rejects_the_same_packets(seed):
    rng = random.Random(seed)
    auth_key, auth_key_id = keys(rng)
    session_id = rng.randbytes(8)
    args = (session_id, auth_key, auth_key_id)

    for name, packet in malformed_packets(rng, auth_key, auth_key_id, session_id):
        new, old = outcome(mtproto.unpack, packet, *args), outcome(old_unpack, packet, *args)
        assert new == old, name
        if name in ("auth key id", "session id", "msg_key", "even msg_id", "padding 0", "padding 11",
                    "padding 1025", "padding 2048", "length +2000"):
            assert new[0] is SecurityCheckMismatch, name
        if name in ("padding 12", "padding 1024"):
            assert isinstance(new, Message), name
