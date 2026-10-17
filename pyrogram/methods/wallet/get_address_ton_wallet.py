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


class GetAddressTonWallet:
    async def get_address_ton_wallet(
        self: "pyrogram.Client",
        address: str
    ) -> Optional["types.UserTonWalletAddress"]:
        """Get information about the TON wallet with the given address.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            address (``str``):
                Address of the wallet in the TON blockchain.

        Returns:
            :obj:`~pyrogram.types.UserTonWalletAddress`: On success, information about the wallet is returned; its
            *user* is set if the wallet belongs to a known Telegram user. None, if the address wasn't found.

        Example:
            .. code-block:: python

                wallet = await app.get_address_ton_wallet("UQ...")

                if wallet and wallet.user:
                    print(wallet.user.first_name)
        """
        r = await self.invoke(
            raw.functions.wallet.GetUserAddresses(
                id=[],
                addresses=[address]
            )
        )

        users = {i.id: i for i in r.users}

        for wallet_address in r.addresses:
            return types.UserTonWalletAddress._parse(self, wallet_address, users)
