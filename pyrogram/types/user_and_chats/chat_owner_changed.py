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


from typing import Dict

import pyrogram
from pyrogram import raw, types
from ..object import Object


class ChatOwnerChanged(Object):
    """A service message about the change of the owner of the chat.

    Parameters:
        new_owner (:obj:`~pyrogram.types.User`):
            The new owner of the chat.
    """

    def __init__(self, *, client: "pyrogram.Client" = None, new_owner: "types.User"):
        super().__init__(client)

        self.new_owner = new_owner

    @staticmethod
    def _parse(
        client,
        action: "raw.types.MessageActionChangeCreator",
        users: Dict[int, "raw.base.User"]
    ) -> "ChatOwnerChanged":
        return ChatOwnerChanged(
            new_owner=types.User._parse(client, users.get(action.new_creator_id)),
            client=client
        )
