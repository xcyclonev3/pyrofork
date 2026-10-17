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

import asyncio
from typing import Iterable, List, Optional, Union

import pyrogram
from pyrogram import raw, types
from pyrogram.types.rich_messages.input_rich_message import get_input_user


class GetUserTonWalletAddresses:
    async def get_user_ton_wallet_addresses(
        self: "pyrogram.Client",
        user_ids: Union[int, str, Iterable[Union[int, str]]]
    ) -> Union[Optional["types.UserTonWalletAddress"], List["types.UserTonWalletAddress"]]:
        """Get the addresses of the TON wallets of users.

        Users without a TON wallet are skipped in the result.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            user_ids (``int`` | ``str`` | Iterable of ``int`` or ``str``):
                A list of User identifiers (id or username) or a single user id/username.
                For a contact that exists in your Telegram address book you can use his phone number (str).

        Returns:
            :obj:`~pyrogram.types.UserTonWalletAddress` | List of :obj:`~pyrogram.types.UserTonWalletAddress`: In case
            *user_ids* was not a list, a single address is returned (None, if the user has no TON wallet), otherwise
            a list of addresses is returned.

        Example:
            .. code-block:: python

                # Get the wallet address of a single user
                address = await app.get_user_ton_wallet_addresses("username")

                # Get the wallet addresses of several users
                addresses = await app.get_user_ton_wallet_addresses([user_id1, user_id2])
        """
        is_iterable = not isinstance(user_ids, (int, str))
        user_ids = list(user_ids) if is_iterable else [user_ids]
        peers = [get_input_user(peer) for peer in await asyncio.gather(*[self.resolve_peer(i) for i in user_ids])]

        r = await self.invoke(
            raw.functions.wallet.GetUserAddresses(
                id=list(peers),
                addresses=[]
            )
        )

        users = {i.id: i for i in r.users}

        addresses = types.List(
            types.UserTonWalletAddress._parse(self, address, users)
            for address in r.addresses
        )

        return addresses if is_iterable else (addresses[0] if addresses else None)
