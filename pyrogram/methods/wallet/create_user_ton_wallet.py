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

from typing import Optional, Union

import pyrogram
from pyrogram import raw, types


class CreateUserTonWallet:
    async def create_user_ton_wallet(
        self: "pyrogram.Client",
        user_id: Union[int, str]
    ) -> Optional["types.UserTonWalletAddress"]:
        """Create a TON wallet for a regular user.

        The current user must have a positive TON wallet balance.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target user.
                For a contact that exists in your Telegram address book you can use his phone number (str).

        Returns:
            :obj:`~pyrogram.types.UserTonWalletAddress`: On success, the address of the wallet of the user is returned.

        Example:
            .. code-block:: python

                address = await app.create_user_ton_wallet("username")
                print(address.wallet_address)
        """
        peer = await self.resolve_peer(user_id)

        r = await self.invoke(
            raw.functions.wallet.GetUserAddresses(
                id=[peer],
                addresses=[],
                force=True
            )
        )

        users = {i.id: i for i in r.users}

        for address in r.addresses:
            return types.UserTonWalletAddress._parse(self, address, users, getattr(peer, "user_id", None))
