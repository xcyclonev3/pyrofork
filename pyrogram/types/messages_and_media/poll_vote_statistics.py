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
from pyrogram import raw, types
from ..object import Object


class PollVoteStatistics(Object):
    """Detailed statistics about the votes of a poll, returned by :meth:`~pyrogram.Client.get_poll_vote_statistics`.

    Parameters:
        vote_graph (:obj:`~pyrogram.types.StatisticalGraph`):
            A graph containing the distribution of the votes in the poll.
    """

    def __init__(
        self,
        *,
        vote_graph: "types.StatisticalGraph",
        client: "pyrogram.Client" = None
    ):
        super().__init__(client)

        self.vote_graph = vote_graph

    @staticmethod
    def _parse(client: "pyrogram.Client", poll_stats: "raw.types.stats.PollStats") -> "PollVoteStatistics":
        return PollVoteStatistics(
            vote_graph=types.StatisticalGraph._parse(client, poll_stats.votes_graph),
            client=client
        )
