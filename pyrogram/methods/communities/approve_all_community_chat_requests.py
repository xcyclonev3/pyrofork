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


import pyrogram
from pyrogram import raw


class ApproveAllCommunityChatRequests:
    async def approve_all_community_chat_requests(
        self: "pyrogram.Client",
        community_id: int
    ) -> bool:
        """Approve all pending requests to add chats to a community.

        .. include:: /_includes/usable-by/users.rst

        You must be an administrator of the community with the *can_edit_chat_list* privilege.

        Parameters:
            community_id (``int``):
                Unique identifier of the community.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.approve_all_community_chat_requests(community_id)
        """
        return await self.invoke(
            raw.functions.communities.ToggleAllPeerLinkRequestApproval(
                community=await self.resolve_peer(community_id),
                reject=False
            )
        )
