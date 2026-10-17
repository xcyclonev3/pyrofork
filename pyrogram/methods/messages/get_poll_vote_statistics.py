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


class GetPollVoteStatistics:
    async def get_poll_vote_statistics(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        is_dark: Optional[bool] = None
    ) -> "types.PollVoteStatistics":
        """Get statistics about the votes of a poll.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat with the poll.

            message_id (``int``):
                Identifier of the message containing the poll.

            is_dark (``bool``, *optional*):
                Pass True if a dark theme is used by the application, to get the colors of the graph for it.

        Returns:
            :obj:`~pyrogram.types.PollVoteStatistics`: On success, the statistics of the poll votes are returned.

        Example:
            .. code-block:: python

                statistics = await app.get_poll_vote_statistics(chat_id, message_id)
                print(statistics.vote_graph)
        """
        r = await self.invoke(
            raw.functions.stats.GetPollStats(
                peer=await self.resolve_peer(chat_id),
                msg_id=message_id,
                dark=is_dark
            )
        )

        return types.PollVoteStatistics._parse(self, r)
