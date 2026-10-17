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
from pyrogram import raw, types


class GetCommunityMemberChats:
    async def get_community_member_chats(
        self: "pyrogram.Client",
        community_id: int,
        user_id: Union[int, str]
    ) -> "types.CommunityMemberChats":
        """Get the chats of a community that a member owns or joined.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            community_id (``int``):
                Unique identifier of the community.

            user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the member.

        Returns:
            :obj:`~pyrogram.types.CommunityMemberChats`: On success, the chats of the member are returned.

        Example:
            .. code-block:: python

                chats = await app.get_community_member_chats(community_id, user_id)
                print([chat.title for chat in chats.owned_chats])
        """
        r = await self.invoke(
            raw.functions.communities.GetParticipantJoinedChats(
                community=await self.resolve_peer(community_id),
                participant=await self.resolve_peer(user_id)
            )
        )

        return types.CommunityMemberChats._parse(self, r)
