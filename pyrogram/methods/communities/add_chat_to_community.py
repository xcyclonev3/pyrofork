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


from typing import Union

import pyrogram
from pyrogram import raw


class AddChatToCommunity:
    async def add_chat_to_community(
        self: "pyrogram.Client",
        community_id: int,
        chat_id: Union[int, str],
        is_hidden: bool = None
    ) -> bool:
        """Add a chat to a community.

        .. include:: /_includes/usable-by/users.rst

        The chat must be owned by you (a supergroup, a channel or a bot) and must not belong to another community.
        If you are not allowed to change the chats of the community (see *can_edit_chat_list* in
        :obj:`~pyrogram.types.ChatPrivileges` and :obj:`~pyrogram.types.ChatPermissions`), a request to add the chat
        is sent to the community administrators instead, and the error ``COMMUNITY_REQUEST_CREATED`` is raised.

        Parameters:
            community_id (``int``):
                Unique identifier of the community.

            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat to add.

            is_hidden (``bool``, *optional*):
                Pass True to hide the chat in the list of community chats.
                Only community administrators can see hidden chats.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.add_chat_to_community(community_id, chat_id)
        """
        return await self.invoke(
            raw.functions.communities.TogglePeerLink(
                community=await self.resolve_peer(community_id),
                peer=await self.resolve_peer(chat_id),
                visible=not is_hidden,
                hidden=is_hidden
            )
        )
