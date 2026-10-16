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


class TonWalletTransferResult(Object):
    """Result of a transfer of cryptocurrency from the TON wallet of the current user.

    Parameters:
        msg_hash (``str``):
            Hash of the sent message. Use :meth:`~pyrogram.Client.get_ton_wallet_transaction_by_msg_hash` to poll
            the transaction.

        is_gasless (``bool``):
            True, if the transfer is gasless (its fee is paid by Telegram).

        transaction (:obj:`~pyrogram.types.TonWalletTransaction`, *optional*):
            The completed transaction. None, if the transaction is still pending.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        msg_hash: str,
        is_gasless: bool,
        transaction: Optional["types.TonWalletTransaction"] = None
    ):
        super().__init__(client)

        self.msg_hash = msg_hash
        self.is_gasless = is_gasless
        self.transaction = transaction

    @staticmethod
    def _parse(
        client,
        update: "raw.types.UpdateSentWalletTransaction",
        users: Dict[int, "raw.base.User"] = None
    ) -> "TonWalletTransferResult":
        return TonWalletTransferResult(
            msg_hash=update.msg_hash,
            is_gasless=update.gasless,
            transaction=types.TonWalletTransaction._parse(client, update.transaction, users),
            client=client
        )
