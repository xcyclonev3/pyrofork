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


class TonWalletSecretPhraseParts(Object):
    """Information needed to fetch the encrypted parts of the secret phrase of the TON wallet.

    Parameters:
        token (``str``):
            Token to pass to :meth:`~pyrogram.Client.get_ton_wallet_encrypted_secret_phrase_part`.

        dc_ids (List of ``int``):
            Identifiers of the data centers of the parts.
    """

    # With the session, the token gives the encrypted parts of the secret phrase: keep it out of logs
    MASKED_ATTRIBUTES = ("token",)

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        token: str,
        dc_ids: List[int]
    ):
        super().__init__(client)

        self.token = token
        self.dc_ids = dc_ids

    @staticmethod
    def _parse(client, parts: "raw.types.wallet.SecretPhraseParts") -> "TonWalletSecretPhraseParts":
        return TonWalletSecretPhraseParts(
            token=parts.token,
            dc_ids=parts.dcs,
            client=client
        )
