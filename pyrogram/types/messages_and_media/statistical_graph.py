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


class StatisticalGraph(Object):
    """Describes a statistical graph.

    It should be one of:

    - :obj:`~pyrogram.types.StatisticalGraphData`
    - :obj:`~pyrogram.types.StatisticalGraphAsync`
    - :obj:`~pyrogram.types.StatisticalGraphError`
    """

    def __init__(self, *, client: "pyrogram.Client" = None):
        super().__init__(client)

    @staticmethod
    def _parse(client: "pyrogram.Client", graph: "raw.base.StatsGraph") -> "StatisticalGraph":
        if isinstance(graph, raw.types.StatsGraphAsync):
            return types.StatisticalGraphAsync(token=graph.token, client=client)

        if isinstance(graph, raw.types.StatsGraphError):
            return types.StatisticalGraphError(error_message=graph.error, client=client)

        return types.StatisticalGraphData(json_data=graph.json.data, zoom_token=graph.zoom_token, client=client)
