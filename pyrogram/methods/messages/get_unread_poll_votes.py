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

from typing import AsyncGenerator, Optional, Union

import pyrogram
from pyrogram import raw, types, utils


class GetUnreadPollVotes:
    async def get_unread_poll_votes(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_thread_id: int = None,
        limit: int = 0
    ) -> Optional[AsyncGenerator["types.Message", None]]:
        """Get the poll messages of a chat that received votes the current user didn't see yet.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            message_thread_id (``int``, *optional*):
                Unique identifier of a forum topic, to get only the messages of that topic.

            limit (``int``, *optional*):
                Limits the number of messages to be retrieved.
                By default, no limit is applied and all messages are returned.

        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.Message` objects.

        Example:
            .. code-block:: python

                async for message in app.get_unread_poll_votes(chat_id):
                    print(message.poll.question)
        """
        peer = await self.resolve_peer(chat_id)

        current = 0
        total = abs(limit) or (1 << 31) - 1
        limit = min(100, total)
        offset_id = 0

        while True:
            r = await self.invoke(
                raw.functions.messages.GetUnreadPollVotes(
                    peer=peer,
                    top_msg_id=message_thread_id,
                    offset_id=offset_id,
                    add_offset=0,
                    limit=limit,
                    max_id=0,
                    min_id=0
                ),
                sleep_threshold=60
            )

            messages = await utils.parse_messages(self, r, replies=0)

            if not messages:
                return

            offset_id = messages[-1].id

            for message in messages:
                yield message

                current += 1

                if current >= total:
                    return
