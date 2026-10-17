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
from pyrogram import raw, types


class GetTonWalletState:
    async def get_ton_wallet_state(
        self: "pyrogram.Client"
    ) -> "types.TonWalletState":
        """Get the state of the TON wallet of the current user.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            :obj:`~pyrogram.types.TonWalletState`: On success, the state of the wallet is returned.

        Example:
            .. code-block:: python

                state = await app.get_ton_wallet_state()

                if state.address:
                    print(state.address, state.balance)
        """
        r = await self.invoke(raw.functions.wallet.GetState())

        return types.TonWalletState._parse(self, r)
