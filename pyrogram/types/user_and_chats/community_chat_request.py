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


from datetime import datetime
from typing import Dict, Optional

import pyrogram
from pyrogram import raw, types, utils
from ..object import Object


class CommunityChatRequest(Object):
    """A request to add a chat to a community.

    Parameters:
        chat (:obj:`~pyrogram.types.Chat`):
            The chat to be added to the community.

        requested_by (:obj:`~pyrogram.types.User`, *optional*):
            The user who sent the request.

        date (:py:obj:`~datetime.datetime`, *optional*):
            Date the request was sent.

        is_hidden (``bool``, *optional*):
            True, if the chat will be hidden in the list of community chats.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        chat: "types.Chat",
        requested_by: "types.User" = None,
        date: datetime = None,
        is_hidden: bool = None
    ):
        super().__init__(client)

        self.chat = chat
        self.requested_by = requested_by
        self.date = date
        self.is_hidden = is_hidden

    @staticmethod
    def _parse(
        client,
        request: "raw.types.CommunityPeerRequest",
        users: Dict[int, "raw.base.User"],
        chats: Dict[int, "raw.base.Chat"]
    ) -> Optional["CommunityChatRequest"]:
        chat = types.Chat._parse_peer(client, request.peer, users, chats)

        if chat is None:
            return None

        return CommunityChatRequest(
            chat=chat,
            requested_by=types.User._parse(client, users.get(request.requested_by)),
            date=utils.timestamp_to_datetime(request.date),
            is_hidden=not request.visible,
            client=client
        )
