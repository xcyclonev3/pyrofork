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


class ApproveCommunityChatRequest:
    async def approve_community_chat_request(
        self: "pyrogram.Client",
        community_id: int,
        chat_id: Union[int, str]
    ) -> bool:
        """Approve a request to add a chat to a community.

        .. include:: /_includes/usable-by/users.rst

        You must be an administrator of the community with the *can_edit_chat_list* privilege.

        Parameters:
            community_id (``int``):
                Unique identifier of the community.

            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat of the request.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.approve_community_chat_request(community_id, chat_id)
        """
        return await self.invoke(
            raw.functions.communities.TogglePeerLinkRequestApproval(
                community=await self.resolve_peer(community_id),
                peer=await self.resolve_peer(chat_id),
                reject=False
            )
        )
