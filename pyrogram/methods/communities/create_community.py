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


from typing import Optional, Union

import pyrogram
from pyrogram import raw, types


class CreateCommunity:
    async def create_community(
        self: "pyrogram.Client",
        title: str,
        chat_id: Union[int, str],
        description: str = None,
        is_chat_hidden: bool = None
    ) -> Optional["types.Chat"]:
        """Create a community: a group of supergroups, channels and chats with bots.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            title (``str``):
                Name of the community.

            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the first chat of the community.
                The chat must be owned by you: a bot, a basic group, a supergroup or a channel.
                Basic groups are upgraded to supergroups.

            description (``str``, *optional*):
                Description of the community.

            is_chat_hidden (``bool``, *optional*):
                Pass True to hide the chat in the list of community chats.
                Only community administrators can see hidden chats.

        Returns:
            :obj:`~pyrogram.types.Chat`: On success, the new community is returned.

        Example:
            .. code-block:: python

                community = await app.create_community("My community", chat_id)
        """
        r = await self.invoke(
            raw.functions.communities.Create(
                title=title,
                peer=await self.resolve_peer(chat_id),
                hidden=is_chat_hidden,
                about=description
            )
        )

        for chat in getattr(r, "chats", []):
            if isinstance(chat, raw.types.Community):
                return types.Chat._parse_community_chat(self, chat)
