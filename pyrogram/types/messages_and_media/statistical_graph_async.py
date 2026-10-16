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


class StatisticalGraphAsync(StatisticalGraph):
    """A statistical graph whose data must be loaded asynchronously.

    The data can be loaded with the raw function :obj:`~pyrogram.raw.functions.stats.LoadAsyncGraph`.

    Parameters:
        token (``str``):
            The token to use for loading the data.
    """

    def __init__(
        self,
        *,
        token: str,
        client: "pyrogram.Client" = None
    ):
        super().__init__(client=client)

        self.token = token
