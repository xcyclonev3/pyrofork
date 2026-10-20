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

"""mtproto.pack/unpack per payload size, and the cost of the crypto executor hop."""
import asyncio
import os
from io import BytesIO

from common import AUTH_KEY, AUTH_KEY_ID, SESSION_ID, server_pack, timeit

import pyrogram
from pyrogram import raw
from pyrogram.crypto import mtproto
from pyrogram.raw.core import Message

print(f"tgcrypto: {'yes' if hasattr(pyrogram.crypto.aes, 'tgcrypto') else 'no'}")
for size in (64, 512, 4096, 65536, 524288, 1048576):
    body = raw.functions.upload.SaveFilePart(file_id=1, file_part=0, bytes=os.urandom(size))
    msg = Message(body, 0x5f000000_00000004, 1, len(body.write()))
    t_pack = timeit(lambda: mtproto.pack(msg, 1, SESSION_ID, AUTH_KEY, AUTH_KEY_ID))
    packet = server_pack(raw.types.RpcResult(req_msg_id=4, result=raw.types.upload.File(
        type=raw.types.storage.FilePartial(), mtime=0, bytes=os.urandom(size))))
    t_unpack = timeit(lambda: mtproto.unpack(BytesIO(packet), SESSION_ID, AUTH_KEY, AUTH_KEY_ID))
    print(f"size {size:>8}: pack {t_pack * 1e6:9.1f} us ({size / t_pack / 1e6:7.1f} MB/s) | "
          f"unpack {t_unpack * 1e6:9.1f} us ({size / t_unpack / 1e6:7.1f} MB/s)")


async def hop():
    loop = asyncio.get_running_loop()
    body = raw.functions.Ping(ping_id=1)
    msg = Message(body, 0x5f000000_00000004, 1, len(body.write()))
    n = 3000
    start = loop.time()
    for _ in range(n):
        await loop.run_in_executor(pyrogram.crypto_executor, mtproto.pack, msg, 1, SESSION_ID, AUTH_KEY, AUTH_KEY_ID)
    via_executor = (loop.time() - start) / n
    start = loop.time()
    for _ in range(n):
        mtproto.pack(msg, 1, SESSION_ID, AUTH_KEY, AUTH_KEY_ID)
    inline = (loop.time() - start) / n
    print(f"Ping pack: inline {inline * 1e6:.1f} us, via crypto_executor {via_executor * 1e6:.1f} us")

asyncio.run(hop())
