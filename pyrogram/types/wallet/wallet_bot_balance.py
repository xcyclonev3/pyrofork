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
from pyrogram import raw
from ..object import Object


class WalletBotBalance(Object):
    """Balance of the current user in the bot https://t.me/walt.

    Parameters:
        has_balance (``bool``):
            True, if the current user has a positive balance in the bot.

        url (``str``, *optional*):
            The URL to open to launch the bot, if any.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        has_balance: bool,
        url: Optional[str] = None
    ):
        super().__init__(client)

        self.has_balance = has_balance
        self.url = url

    @staticmethod
    def _parse(client, balance: "raw.types.wallet.ExistingBalance") -> "WalletBotBalance":
        return WalletBotBalance(
            has_balance=balance.has_balance,
            url=balance.url or None,
            client=client
        )
