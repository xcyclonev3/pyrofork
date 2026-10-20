#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
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

from typing import Tuple

from pyrogram.raw.core import Message, MsgContainer, TLObject
from pyrogram.raw.functions import Ping
from pyrogram.raw.types import MsgsAck, HttpWait
from .msg_id import MsgId
from .seq_no import SeqNo

not_content_related = (Ping, HttpWait, MsgsAck, MsgContainer)


class MsgFactory:
    def __init__(self):
        self.seq_no = SeqNo()

    def __call__(self, body: TLObject) -> Message:
        return self.new(body)[0]

    def new(self, body: TLObject) -> Tuple[Message, bytes]:
        """A message for body, and body serialized: its length needs it, and packing the message can reuse it
        instead of serializing body a second time."""
        msg_id = MsgId()
        seq_no = self.seq_no(not isinstance(body, not_content_related))
        data = body.write()

        return Message(body, msg_id, seq_no, len(data)), data
