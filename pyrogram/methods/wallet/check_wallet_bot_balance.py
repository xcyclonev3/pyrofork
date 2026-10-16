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


class CheckWalletBotBalance:
    async def check_wallet_bot_balance(
        self: "pyrogram.Client"
    ) -> "types.WalletBotBalance":
        """Check the balance of the current user in the bot https://t.me/walt.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            :obj:`~pyrogram.types.WalletBotBalance`: On success, the balance information is returned.

        Example:
            .. code-block:: python

                balance = await app.check_wallet_bot_balance()

                if balance.has_balance:
                    print(balance.url)
        """
        r = await self.invoke(raw.functions.wallet.GetExistingWaltBalance())

        return types.WalletBotBalance._parse(self, r)
