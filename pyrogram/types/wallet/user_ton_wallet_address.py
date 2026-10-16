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

from typing import Dict, Optional

import pyrogram
from pyrogram import raw, types
from ..object import Object


class UserTonWalletAddress(Object):
    """Address of the TON wallet of a user.

    Parameters:
        wallet_address (``str``):
            Address of the wallet in the TON blockchain.

        public_key (``bytes``):
            Public key of the wallet.

        user (:obj:`~pyrogram.types.User`, *optional*):
            The user who owns the wallet. None, if unknown.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        wallet_address: str,
        public_key: bytes,
        user: Optional["types.User"] = None
    ):
        super().__init__(client)

        self.wallet_address = wallet_address
        self.public_key = public_key
        self.user = user

    @staticmethod
    def _parse(
        client,
        address: "raw.types.WalletUserAddress",
        users: Dict[int, "raw.base.User"] = None,
        user_id: Optional[int] = None
    ) -> "UserTonWalletAddress":
        # user_id is the fallback owner, for results that answer a request about a known user
        user_id = address.user_id or user_id

        return UserTonWalletAddress(
            wallet_address=address.address,
            public_key=address.public_key,
            user=types.User._parse(client, (users or {}).get(user_id)) if user_id else None,
            client=client
        )
