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


from typing import AsyncGenerator, Optional

import pyrogram
from pyrogram import raw, types


class GetCommunityChatRequests:
    async def get_community_chat_requests(
        self: "pyrogram.Client",
        community_id: int,
        limit: int = 0
    ) -> Optional[AsyncGenerator["types.CommunityChatRequest", None]]:
        """Get the pending requests to add chats to a community.

        .. include:: /_includes/usable-by/users.rst

        You must be an administrator of the community with the *can_edit_chat_list* privilege.

        Parameters:
            community_id (``int``):
                Unique identifier of the community.

            limit (``int``, *optional*):
                Limits the number of requests to be retrieved.
                By default, no limit is applied and all requests are returned.

        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.CommunityChatRequest` objects.

        Example:
            .. code-block:: python

                async for request in app.get_community_chat_requests(community_id):
                    print(request.chat.title, request.requested_by.first_name)
        """
        current = 0
        total = abs(limit) or (1 << 31) - 1
        limit = min(100, total)
        offset = ""

        while True:
            r = await self.invoke(
                raw.functions.communities.GetPeerLinkRequests(
                    community=await self.resolve_peer(community_id),
                    offset=offset,
                    limit=limit
                ),
                sleep_threshold=60
            )

            if not r.requests:
                return

            users = {u.id: u for u in r.users}
            chats = {c.id: c for c in r.chats}

            for request in r.requests:
                parsed = types.CommunityChatRequest._parse(self, request, users, chats)

                if parsed is None:
                    continue

                yield parsed

                current += 1

                if current >= total:
                    return

            if not r.next_offset:
                return

            offset = r.next_offset
