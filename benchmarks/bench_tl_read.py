#  Pyrogram - Telegram MTProto API Client Library for Python
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

"""TLObject.read of an update, a 100 message history and a 1000 message history."""
from io import BytesIO

from common import timeit

from pyrogram import raw
from pyrogram.raw.core import TLObject
from tl_samples import HISTORY, UPDATE, channel, message, user

BIG = raw.types.messages.ChannelMessages(pts=1, count=1000, messages=[message(i) for i in range(1, 1001)],
                                         topics=[], chats=[channel(777)], users=[user(i) for i in range(1, 201)])
for name, obj in (("update (548 B)", UPDATE), ("history 100 (30 KB)", HISTORY), ("history 1000 (301 KB)", BIG)):
    data = obj.write()
    print(f"{name:<24} read {timeit(lambda: TLObject.read(BytesIO(data)), seconds=1.5) * 1e6:9.1f} us")
