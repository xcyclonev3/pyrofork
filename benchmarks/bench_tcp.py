#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

"""TCPAbridged.recv throughput against a local loopback server sending framed packets."""
import asyncio
import os
import sys

import common  # noqa: F401  (sets sys.path)
from pyrogram.connection.transport import TCPAbridged, TCPIntermediate


async def serve(sizes, framing):
    payloads = {s: os.urandom(s) for s in set(sizes)}

    async def handle(reader, writer):
        await reader.readexactly(1 if framing == "abridged" else 4)
        for s in sizes:
            p = payloads[s]
            if framing == "abridged":
                n = len(p) // 4
                header = bytes([n]) if n <= 126 else b"\x7f" + n.to_bytes(3, "little")
            else:
                header = len(p).to_bytes(4, "little")
            writer.write(header + p)
            if writer.transport.get_write_buffer_size() > 4 << 20:
                await writer.drain()
        await writer.drain()
        await asyncio.sleep(1)
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def run(size, count, framing):
    sizes = [size] * count
    server, port = await serve(sizes, framing)
    tcp = (TCPAbridged if framing == "abridged" else TCPIntermediate)(ipv6=False, proxy=None)
    await tcp.connect(("127.0.0.1", port))
    loop = asyncio.get_running_loop()
    start = loop.time()
    for _ in range(count):
        data = await tcp.recv()
        assert data is not None and len(data) == size
    elapsed = loop.time() - start
    await tcp.close()
    server.close()
    await server.wait_closed()
    return elapsed


async def main():
    for size, count in ((64, 20000), (4096, 10000), (65536, 2000), (1048576, 150)):
        best = min([await run(size, count, "abridged") for _ in range(3)])
        print(f"abridged {size:>8} B x {count:>5}: {best / count * 1e6:8.1f} us/packet, "
              f"{size * count / best / 1e6:8.1f} MB/s")

if __name__ == "__main__":
    if os.environ.get("UVLOOP"):
        import uvloop
        uvloop.install()
    asyncio.run(main())
