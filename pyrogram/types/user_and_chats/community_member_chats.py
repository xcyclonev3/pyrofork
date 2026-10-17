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


from typing import List

import pyrogram
from pyrogram import raw, types
from ..object import Object


class CommunityMemberChats(Object):
    """The chats of a community that a member takes part in.

    Parameters:
        owned_chats (List of :obj:`~pyrogram.types.Chat`):
            Chats of the community owned by the member.

        joined_chats (List of :obj:`~pyrogram.types.Chat`):
            Chats of the community the member joined.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        owned_chats: List["types.Chat"],
        joined_chats: List["types.Chat"]
    ):
        super().__init__(client)

        self.owned_chats = owned_chats
        self.joined_chats = joined_chats

    @staticmethod
    def _parse(client, r: "raw.types.communities.ParticipantJoinedChats") -> "CommunityMemberChats":
        chats = {c.id: c for c in r.chats}

        def parse(chat_ids):
            return types.List(
                types.Chat._parse_chat(client, chats[chat_id]) for chat_id in chat_ids if chat_id in chats
            )

        return CommunityMemberChats(
            owned_chats=parse(r.creator_chat_ids),
            joined_chats=parse(r.joined_chat_ids),
            client=client
        )
