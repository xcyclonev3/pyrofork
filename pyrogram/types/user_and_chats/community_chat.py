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


from typing import Dict, Optional

import pyrogram
from pyrogram import raw, types
from ..object import Object


class CommunityChat(Object):
    """A chat belonging to a community.

    Parameters:
        chat (:obj:`~pyrogram.types.Chat`):
            The chat: a supergroup, a channel or a bot.

        can_view_history (``bool``):
            True, if the message history of the chat can be viewed.

        is_hidden (``bool``):
            True, if the chat is hidden in the list of community chats.
            Only community administrators can see hidden chats.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        chat: "types.Chat",
        can_view_history: bool = None,
        is_hidden: bool = None
    ):
        super().__init__(client)

        self.chat = chat
        self.can_view_history = can_view_history
        self.is_hidden = is_hidden

    @staticmethod
    def _parse(
        client,
        community_peer: "raw.types.CommunityPeer",
        users: Dict[int, "raw.base.User"],
        chats: Dict[int, "raw.base.Chat"]
    ) -> Optional["CommunityChat"]:
        chat = types.Chat._parse_peer(client, community_peer.peer, users, chats)

        if chat is None:
            return None

        return CommunityChat(
            chat=chat,
            can_view_history=community_peer.can_view_history,
            # The visibility is sent to community administrators only
            is_hidden=community_peer.visible is False,
            client=client
        )
