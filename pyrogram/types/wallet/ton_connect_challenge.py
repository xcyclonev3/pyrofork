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
from pyrogram import raw
from ..object import Object


class TonConnectChallenge(Object):
    """A TON Connect challenge, to answer with :meth:`~pyrogram.Client.send_ton_connect_session_connect_result`.

    Parameters:
        challenge (``bytes``):
            The challenge (104 bytes, according to TDLib).

        event_id (``int``):
            Unique identifier of the first outgoing event of the session.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        challenge: bytes,
        event_id: int
    ):
        super().__init__(client)

        self.challenge = challenge
        self.event_id = event_id

    @staticmethod
    def _parse(client, challenge: "raw.types.wallet.TonConnectChallenge") -> "TonConnectChallenge":
        return TonConnectChallenge(
            challenge=challenge.challenge,
            event_id=challenge.event_id,
            client=client
        )
