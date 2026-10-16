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


class TonWalletTransfer(Object):
    """A transfer of cryptocurrency with the TON wallet of the current user, shown as a service message.

    Parameters:
        amount (``int``):
            The amount of the transfer, in the smallest units of the cryptocurrency.

        peer_address (``str``):
            Address of the peer of the transfer in the TON blockchain.

        is_comment_encrypted (``bool``):
            True, if the comment is encrypted and must be decrypted with the private key of the receiver or of the
            sender.

        transaction_id (``str``, *optional*):
            Unique identifier of the transaction; None, if the transfer isn't sent yet.
            Use :meth:`~pyrogram.Client.get_ton_wallet_transaction` to get information about the transaction.

        comment (``str``, *optional*):
            Comment added to the transfer.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        amount: int,
        peer_address: str,
        is_comment_encrypted: bool,
        transaction_id: Optional[str] = None,
        comment: Optional[str] = None
    ):
        super().__init__(client)

        self.amount = amount
        self.peer_address = peer_address
        self.is_comment_encrypted = is_comment_encrypted
        self.transaction_id = transaction_id
        self.comment = comment

    @staticmethod
    def _parse(client, action: "raw.types.MessageActionGramTransfer") -> "TonWalletTransfer":
        return TonWalletTransfer(
            amount=action.amount,
            peer_address=action.peer_address,
            is_comment_encrypted=action.comment_encrypted,
            transaction_id=action.transaction_id or None,
            comment=action.comment,
            client=client
        )
