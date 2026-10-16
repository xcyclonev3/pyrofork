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

from typing import List

import pyrogram
from pyrogram import raw, types
from ..object import Object


class TonConnectRequests(Object):
    """A TON Connect session with its pending requests.

    Parameters:
        session (:obj:`~pyrogram.types.TonConnectSession`):
            The session.

        requests (List of :obj:`~pyrogram.types.TonConnectRequest`):
            The pending requests of the session.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        session: "types.TonConnectSession",
        requests: List["types.TonConnectRequest"]
    ):
        super().__init__(client)

        self.session = session
        self.requests = requests

    @staticmethod
    def _parse(client, pending: "raw.types.wallet.TonConnectPending") -> "TonConnectRequests":
        return TonConnectRequests(
            session=types.TonConnectSession._parse(client, pending.session),
            requests=types.List(types.TonConnectRequest._parse(client, request) for request in pending.requests),
            client=client
        )
