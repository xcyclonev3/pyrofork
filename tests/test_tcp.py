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

"""TCP transports receiving packets from a local server."""

import asyncio
import random

import pytest

from pyrogram.connection.transport import TCPAbridged, TCPIntermediate
from pyrogram.connection.transport.tcp.tcp import TCP


class OldTCPAbridged(TCPAbridged):
    async def recv_bytes(self, length):
        """TCP.recv as it was: a new bytes object and asyncio.wait_for for every read."""
        data = b""

        while len(data) < length:
            try:
                chunk = await asyncio.wait_for(self.reader.read(length - len(data)), TCP.TIMEOUT)
            except (OSError, asyncio.TimeoutError):
                return None
            else:
                if chunk:
                    data += chunk
                else:
                    return None

        return data

    async def recv(self, length: int = 0):
        length = await self.recv_bytes(1)
        if length is None:
            return None
        if length == b"\x7f":
            length = await self.recv_bytes(3)
            if length is None:
                return None
        return await self.recv_bytes(int.from_bytes(length, "little") * 4)


def abridged(payload):
    n = len(payload) // 4
    return (bytes([n]) if n <= 126 else b"\x7f" + n.to_bytes(3, "little")) + payload


async def serve(handler):
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def receive_all(transport_class, stream, pieces, count):
    """Send stream split into pieces (with pauses, so that the reads get parts of packets), read count packets."""
    async def handler(reader, writer):
        await reader.readexactly(1)  # 0xef
        for piece in pieces:
            writer.write(piece)
            await writer.drain()
            await asyncio.sleep(0)
        await asyncio.sleep(0.5)
        writer.close()

    server, port = await serve(handler)
    tcp = transport_class(ipv6=False, proxy=None)
    await tcp.connect(("127.0.0.1", port))
    packets = [await tcp.recv() for _ in range(count)]
    await tcp.close()
    server.close()
    await server.wait_closed()
    return packets


@pytest.mark.parametrize("seed", range(6))
def test_recv_matches_the_old_recv(seed):
    rng = random.Random(seed)
    payloads = [rng.randbytes(4 * rng.choice([1, 2, 16, 126, 127, 1000, 40000, 300000])) for _ in range(25)]
    stream = b"".join(abridged(p) for p in payloads)
    cuts = sorted(rng.sample(range(1, len(stream)), 60))
    pieces = [stream[a:b] for a, b in zip([0] + cuts, cuts + [len(stream)])]

    async def main():
        new = await receive_all(TCPAbridged, stream, pieces, len(payloads))
        old = await receive_all(OldTCPAbridged, stream, pieces, len(payloads))
        return new, old

    new, old = asyncio.run(main())
    assert new == old == payloads
    assert all(type(p) is bytes for p in new)


class Stalling:
    """A server that sends the start of a packet, then nothing."""

    def __init__(self, data):
        self.data = data

    async def __call__(self, reader, writer):
        await reader.read(4)
        writer.write(self.data)
        await writer.drain()
        await asyncio.sleep(5)
        writer.close()


def test_recv_times_out_without_data(monkeypatch):
    monkeypatch.setattr(TCP, "TIMEOUT", 0.2)

    async def main():
        server, port = await serve(Stalling(len(b"x" * 40).to_bytes(4, "little") + b"x" * 10))
        tcp = TCPIntermediate(ipv6=False, proxy=None)
        await tcp.connect(("127.0.0.1", port))
        loop = asyncio.get_running_loop()
        start = loop.time()
        assert await tcp.recv() is None
        elapsed = loop.time() - start
        # The timeout is over: the task goes on normally
        await asyncio.sleep(0.01)
        task = asyncio.current_task()
        if hasattr(task, "cancelling"):
            assert task.cancelling() == 0
        await tcp.close()
        server.close()
        return elapsed

    assert 0.15 < asyncio.run(main()) < 2


def test_recv_cancelled_from_outside(monkeypatch):
    async def main():
        server, port = await serve(Stalling(b""))
        tcp = TCPIntermediate(ipv6=False, proxy=None)
        await tcp.connect(("127.0.0.1", port))
        task = asyncio.get_running_loop().create_task(tcp.recv())
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await tcp.close()
        server.close()
        return task

    assert asyncio.run(main()).cancelled()


def test_recv_end_of_stream_in_a_packet():
    async def main():
        async def handler(reader, writer):
            await reader.read(1)
            writer.write(abridged(b"abcd" * 10)[:20])
            await writer.drain()
            writer.close()

        server, port = await serve(handler)
        tcp = TCPAbridged(ipv6=False, proxy=None)
        await tcp.connect(("127.0.0.1", port))
        packet = await tcp.recv()
        await tcp.close()
        server.close()
        return packet

    assert asyncio.run(main()) is None


class BrokenReader:
    async def read(self, n):
        raise ConnectionResetError


def test_recv_length_zero_and_errors():
    async def main():
        tcp = TCP(ipv6=False, proxy=None)
        tcp.reader = BrokenReader()
        return await tcp.recv(0), await tcp.recv(4)

    assert asyncio.run(main()) == (b"", None)
