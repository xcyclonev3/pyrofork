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


class BanCommunityMember:
    async def ban_community_member(
        self: "pyrogram.Client",
        community_id: int,
        user_id: Union[int, str]
    ) -> bool:
        """Ban a user from a community.

        The user is banned from all the chats of the community.

        .. include:: /_includes/usable-by/users.rst

        You must be an administrator of the community with the *can_restrict_members* privilege.

        Parameters:
            community_id (``int``):
                Unique identifier of the community.

            user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target user or chat.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.ban_community_member(community_id, user_id)
        """
        return await self.invoke(
            raw.functions.communities.ToggleParticipantBanned(
                community=await self.resolve_peer(community_id),
                participant=await self.resolve_peer(user_id),
                unban=False
            )
        )
