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

from typing import Optional

import pyrogram
from .statistical_graph import StatisticalGraph


class StatisticalGraphData(StatisticalGraph):
    """The data of a statistical graph.

    Parameters:
        json_data (``str``):
            The graph data in JSON format.

        zoom_token (``str``, *optional*):
            A token which can be used to receive a zoomed in graph.
    """

    def __init__(
        self,
        *,
        json_data: str,
        zoom_token: Optional[str] = None,
        client: "pyrogram.Client" = None
    ):
        super().__init__(client=client)

        self.json_data = json_data
        self.zoom_token = zoom_token
