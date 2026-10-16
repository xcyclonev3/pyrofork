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


class GetTonWalletBackupHolderDcs:
    async def get_ton_wallet_backup_holder_dcs(
        self: "pyrogram.Client"
    ) -> List["types.TonWalletBackupHolderDc"]:
        """Get the data centers which hold the parts of the backup of the secret phrase of the TON wallet.

        Use them to encrypt the parts passed to :meth:`~pyrogram.Client.enable_ton_wallet_backup`.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            List of :obj:`~pyrogram.types.TonWalletBackupHolderDc`: On success, the list of data centers is returned.

        Example:
            .. code-block:: python

                for dc in await app.get_ton_wallet_backup_holder_dcs():
                    print(dc.dc_id, dc.public_key.hex())
        """
        r = await self.invoke(raw.functions.wallet.GetBackupHolderDcs())

        return types.List(types.TonWalletBackupHolderDc._parse(self, holder_dc) for holder_dc in r)
