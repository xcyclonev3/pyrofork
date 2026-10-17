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


class ChatOwnerLeft(Object):
    """A service message about the owner of the chat leaving it.

    Parameters:
        new_owner (:obj:`~pyrogram.types.User`, *optional*):
            The user who will become the new owner of the chat if the previous owner doesn't come back.
    """

    def __init__(self, *, client: "pyrogram.Client" = None, new_owner: "types.User" = None):
        super().__init__(client)

        self.new_owner = new_owner

    @staticmethod
    def _parse(
        client,
        action: "raw.types.MessageActionNewCreatorPending",
        users: Dict[int, "raw.base.User"]
    ) -> "ChatOwnerLeft":
        return ChatOwnerLeft(
            new_owner=types.User._parse(client, users.get(action.new_creator_id)) if action.new_creator_id else None,
            client=client
        )
