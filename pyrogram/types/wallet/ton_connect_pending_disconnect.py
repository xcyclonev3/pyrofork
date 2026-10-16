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
from pyrogram import raw
from ..object import Object


class TonConnectPendingDisconnect(Object):
    """TON Connect sessions which must be disconnected in a regular way, with
    :meth:`~pyrogram.Client.disconnect_ton_connect_session`.

    Parameters:
        session_ids (List of ``int``):
            Identifiers of the sessions.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        session_ids: List[int]
    ):
        super().__init__(client)

        self.session_ids = session_ids

    @staticmethod
    def _parse(
        client,
        update: "raw.types.UpdateWalletTonConnectPendingDisconnect"
    ) -> "TonConnectPendingDisconnect":
        return TonConnectPendingDisconnect(
            session_ids=update.session_ids,
            client=client
        )
