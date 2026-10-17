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
from .statistical_graph import StatisticalGraph


class StatisticalGraphError(StatisticalGraph):
    """An error message to be shown to the user instead of a statistical graph.

    Parameters:
        error_message (``str``):
            The error message.
    """

    def __init__(
        self,
        *,
        error_message: str,
        client: "pyrogram.Client" = None
    ):
        super().__init__(client=client)

        self.error_message = error_message
