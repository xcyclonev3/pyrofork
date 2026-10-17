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

from datetime import datetime
from typing import Dict, Optional

import pyrogram
from pyrogram import enums, raw, types, utils
from ..object import Object


class TonWalletTransaction(Object):
    """A transaction of the TON wallet of the current user.

    Parameters:
        id (``str``):
            Unique identifier of the transaction.

        type (:obj:`~pyrogram.enums.TonWalletTransactionType`):
            Type of the transaction.

        amount (``int``):
            Amount of the transaction, in the smallest units of the cryptocurrency; negative for outgoing
            transactions.

        fee_amount (``int``):
            Fee paid for the transaction, in the smallest units of the cryptocurrency.

        date (:py:obj:`~datetime.datetime`):
            Date the transaction was completed.

        is_outgoing (``bool``):
            True, if the transaction is outgoing.

        is_gasless (``bool``):
            True, if the fee of the transaction was paid by Telegram.

        is_failed (``bool``):
            True, if the transaction failed.

        is_comment_encrypted (``bool``):
            True, if the comment is encrypted and must be decrypted with the private key of the receiver or of the
            sender.

        peer_address (``str``, *optional*):
            Address of the peer of the transaction in the TON blockchain.
            None, if the peer is not supported by the current version of Telegram.

        peer_domain (``str``, *optional*):
            Domain assigned to the address of the peer, if any.

        peer_user (:obj:`~pyrogram.types.User`, *optional*):
            The Telegram user who is the peer of the transaction, if known.

        on_ramp_provider_name (``str``, *optional*):
            Name of the on-ramp provider. Only for deposits bought from an on-ramp provider.

        comment (``str``, *optional*):
            Comment added to the transaction.

        tx_hash (``str``, *optional*):
            Hash of the transaction in the TON blockchain.

        nft (:obj:`~pyrogram.types.TonNft`, *optional*):
            The transferred NFT. Only for NFT transfers.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        id: str,
        type: "enums.TonWalletTransactionType",
        amount: int,
        fee_amount: int,
        date: datetime,
        is_outgoing: bool,
        is_gasless: bool,
        is_failed: bool,
        is_comment_encrypted: bool,
        peer_address: Optional[str] = None,
        peer_domain: Optional[str] = None,
        peer_user: Optional["types.User"] = None,
        on_ramp_provider_name: Optional[str] = None,
        comment: Optional[str] = None,
        tx_hash: Optional[str] = None,
        nft: Optional["types.TonNft"] = None
    ):
        super().__init__(client)

        self.id = id
        self.type = type
        self.amount = amount
        self.fee_amount = fee_amount
        self.date = date
        self.is_outgoing = is_outgoing
        self.is_gasless = is_gasless
        self.is_failed = is_failed
        self.is_comment_encrypted = is_comment_encrypted
        self.peer_address = peer_address
        self.peer_domain = peer_domain
        self.peer_user = peer_user
        self.on_ramp_provider_name = on_ramp_provider_name
        self.comment = comment
        self.tx_hash = tx_hash
        self.nft = nft

    @staticmethod
    def _parse(
        client,
        transaction: "raw.types.WalletTransaction",
        users: Dict[int, "raw.base.User"] = None
    ) -> Optional["TonWalletTransaction"]:
        if transaction is None:
            return None

        peer = transaction.peer
        peer_user = None

        if isinstance(peer, raw.types.WalletTransactionPeerUser):
            peer_user = types.User._parse(client, (users or {}).get(peer.user_id))

        on_ramp_provider_name = getattr(peer, "provider_name", None)

        # Same precedence as TDLib's get_ton_wallet_transaction_object()
        if transaction.nft is not None:
            transaction_type = enums.TonWalletTransactionType.NFT_TRANSFER
        elif on_ramp_provider_name:
            transaction_type = enums.TonWalletTransactionType.ON_RAMP_DEPOSIT
        elif transaction.key_change:
            transaction_type = enums.TonWalletTransactionType.KEY_CHANGE
        else:
            transaction_type = enums.TonWalletTransactionType.TRANSFER

        amount = transaction.amount

        if not transaction.incoming and amount > 0:
            amount = -amount

        return TonWalletTransaction(
            id=transaction.id,
            type=transaction_type,
            amount=amount,
            fee_amount=transaction.fee,
            date=utils.timestamp_to_datetime(transaction.date),
            is_outgoing=not transaction.incoming,
            is_gasless=transaction.gasless,
            is_failed=transaction.failed,
            is_comment_encrypted=transaction.comment_encrypted,
            peer_address=getattr(peer, "address", None),
            peer_domain=getattr(peer, "domain", None),
            peer_user=peer_user,
            on_ramp_provider_name=on_ramp_provider_name,
            comment=transaction.comment,
            tx_hash=transaction.tx_hash,
            nft=types.TonNft._parse(client, transaction.nft),
            client=client
        )
