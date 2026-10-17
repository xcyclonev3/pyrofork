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


class TonWalletBackupHolderDc(Object):
    """A data center which holds a part of the backup of the secret phrase of the TON wallet.

    Parameters:
        dc_id (``int``):
            Identifier of the data center.

        public_key (``bytes``):
            Public key of the data center, for which the part of the backup is encrypted.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        dc_id: int,
        public_key: bytes
    ):
        super().__init__(client)

        self.dc_id = dc_id
        self.public_key = public_key

    @staticmethod
    def _parse(client, holder_dc: "raw.types.wallet.HolderDc") -> "TonWalletBackupHolderDc":
        return TonWalletBackupHolderDc(
            dc_id=holder_dc.dc,
            public_key=holder_dc.public_key,
            client=client
        )
