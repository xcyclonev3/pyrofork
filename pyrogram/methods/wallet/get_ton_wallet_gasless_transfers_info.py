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
from pyrogram import raw, types


class GetTonWalletGaslessTransfersInfo:
    async def get_ton_wallet_gasless_transfers_info(
        self: "pyrogram.Client"
    ) -> Optional["types.TonWalletGaslessTransfersInfo"]:
        """Get the state of the gasless transfers of the current user, whose fee is paid by Telegram.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            :obj:`~pyrogram.types.TonWalletGaslessTransfersInfo`: On success, the state of the gasless transfers is
            returned. None is returned if the server sends no state.

        Example:
            .. code-block:: python

                info = await app.get_ton_wallet_gasless_transfers_info()
                print(info.left_count)
        """
        r = await self.invoke(raw.functions.wallet.GetGaslessInfo())

        # The result is delivered as an updateWalletGaslessInfo inside the returned Updates
        if isinstance(r, (raw.types.Updates, raw.types.UpdatesCombined)):
            updates = r.updates
        else:
            updates = [getattr(r, "update", None)]

        for update in updates:
            if isinstance(update, raw.types.UpdateWalletGaslessInfo):
                return types.TonWalletGaslessTransfersInfo._parse(self, update)
