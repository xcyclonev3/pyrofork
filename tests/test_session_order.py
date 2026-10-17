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

"""Sessions keep the order of messages, whether they are packed right away or in crypto_executor."""

import asyncio
import random
import time
from hashlib import sha1, sha256
from io import BytesIO

import pytest

from pyrogram import raw
from pyrogram.connection.transport import TCPAbridgedO, TCPIntermediateO
from pyrogram.connection.transport.tcp.tcp import TCP
from pyrogram.crypto import aes
from pyrogram.crypto.mtproto import kdf
from pyrogram.raw.core import Long, Message
from pyrogram.session import Session

AUTH_KEY = bytes(range(256))
AUTH_KEY_ID = sha1(AUTH_KEY).digest()[-8:]
SIZES = [16, 200, 4000, 30000, 65000, 70000, 200000]  # around Session.INLINE_CRYPTO_SIZE


def decrypt_request(payload):
    """The plaintext of a payload a client sent, checked."""
    assert payload[:8] == AUTH_KEY_ID
    msg_key = payload[8:24]
    aes_key, aes_iv = kdf(AUTH_KEY, msg_key, True)
    plain = aes.ige256_decrypt(payload[24:], aes_key, aes_iv)
    assert sha256(AUTH_KEY[88:120] + plain).digest()[8:24] == msg_key
    return plain


def msg_id_of(payload):
    return int.from_bytes(decrypt_request(payload)[16:24], "little")


class OrderedConnection:
    """Writes in lock order, like TCP.send, and yields in between like drain() does."""

    def __init__(self, rng):
        self.rng = rng
        self.lock = asyncio.Lock()
        self.sent = []

    async def send(self, payload):
        async with self.lock:
            self.sent.append(payload)
            await asyncio.sleep(self.rng.choice([0, 0, 0.0005]))


async def send_concurrently(session, rng, count):
    queries = [raw.functions.upload.SaveFilePart(file_id=i, file_part=0, bytes=rng.randbytes(rng.choice(SIZES)))
               for i in range(count)]
    tasks = []
    for query in queries:
        tasks.append(asyncio.get_running_loop().create_task(session.send(query, wait_response=False)))
        if rng.random() < 0.3:
            await asyncio.sleep(rng.choice([0, 0.0002]))
    await asyncio.gather(*tasks)
    return queries


@pytest.mark.parametrize("seed", range(8))
def test_messages_are_sent_in_msg_id_order(seed):
    rng = random.Random(seed)

    async def main():
        session = Session(None, 2, AUTH_KEY, False)
        session.connection = OrderedConnection(rng)
        await send_concurrently(session, rng, 120)
        return session

    session = asyncio.run(main())
    msg_ids = [msg_id_of(p) for p in session.connection.sent]
    assert len(msg_ids) == 120 and msg_ids == sorted(msg_ids) and len(set(msg_ids)) == 120
    assert session.packing == 0


class FakeWriter:
    def __init__(self, rng):
        self.rng = rng
        self.data = bytearray()

    def write(self, data):
        self.data += data

    async def drain(self):
        await asyncio.sleep(self.rng.choice([0, 0, 0.0005]))


class TransportConnection:
    def __init__(self, transport):
        self.transport = transport

    async def send(self, payload):
        await self.transport.send(payload)


def frames(transport_class, stream):
    """Decrypt an obfuscated stream as the server does, and split it into packets."""
    nonce = stream[:64]
    state = (bytes(nonce[8:40]), bytearray(nonce[40:56]), bytearray(1))
    plain = aes.ctr256_decrypt(stream, *state)  # The nonce is part of the keystream too
    data = BytesIO(plain[64:])
    packets = []
    while data.tell() < len(plain) - 64:
        if transport_class is TCPAbridgedO:
            length = data.read(1)[0]
            if length == 0x7f:
                length = int.from_bytes(data.read(3), "little")
            length *= 4
        else:
            length = int.from_bytes(data.read(4), "little")
        packets.append(data.read(length))
    return packets


@pytest.mark.parametrize("transport_class", [TCPAbridgedO, TCPIntermediateO])
@pytest.mark.parametrize("seed", range(4))
def test_obfuscated_keystream_follows_the_send_order(monkeypatch, transport_class, seed):
    rng = random.Random(seed)

    async def fake_connect(self, address):
        self.writer = FakeWriter(rng)

    monkeypatch.setattr(TCP, "connect", fake_connect)

    async def main():
        transport = transport_class(ipv6=False, proxy=None)
        await transport.connect(("127.0.0.1", 443))
        session = Session(None, 2, AUTH_KEY, False)
        session.connection = TransportConnection(transport)
        queries = await send_concurrently(session, rng, 60)
        return transport.writer.data, queries

    stream, queries = asyncio.run(main())
    packets = frames(transport_class, bytes(stream))
    assert len(packets) == 60
    msg_ids = [msg_id_of(p) for p in packets]  # garbage (and a failed check) if the keystream order broke
    assert msg_ids == sorted(msg_ids)


class FakeClient:
    def __init__(self):
        self.updates = []

    async def handle_updates(self, update):
        self.updates.append(update.message)


def server_packet(session, body, msg_id):
    data = Long(1) + session.session_id + Message(body, msg_id, 1, len(body.write())).write()
    padding = bytes(-(len(data) + 12) % 16 + 12)
    msg_key = sha256(AUTH_KEY[96:128] + data + padding).digest()[8:24]
    aes_key, aes_iv = kdf(AUTH_KEY, msg_key, False)
    return AUTH_KEY_ID + msg_key + aes.ige256_encrypt(data + padding, aes_key, aes_iv)


@pytest.mark.parametrize("seed", range(6))
def test_received_messages_are_handled_in_order(seed):
    rng = random.Random(seed)

    async def main():
        client = FakeClient()
        session = Session(client, 2, AUTH_KEY, False)
        session.connection = OrderedConnection(rng)  # for the acks
        base = (int(time.time()) << 32) | 1
        texts = [f"{i}:" + "x" * rng.choice(SIZES) for i in range(80)]
        packets = [server_packet(session, raw.types.UpdateShortMessage(
            id=i, user_id=7, message=text, pts=i, pts_count=1, date=1), base + 4 * i) for i, text in enumerate(texts)]
        tasks = []
        for packet in packets:  # As recv_worker does: a task per packet, in the order they come
            tasks.append(session.loop.create_task(session.handle_packet(packet)))
            if rng.random() < 0.3:
                await asyncio.sleep(0)
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.01)
        return client.updates, texts, session

    updates, texts, session = asyncio.run(main())
    assert updates == texts and session.unpacking == 0
