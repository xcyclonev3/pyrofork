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

from pyrogram import raw
from ..object import Object


class TonWalletOwnershipProof(Object):
    """A proof of the ownership of a TON wallet.

    The signature is created by signing the challenge returned by
    :meth:`~pyrogram.Client.get_ton_wallet_ownership_proof_challenge` with the private key of the wallet.
    Pyrofork doesn't implement TON cryptography: the signature must be computed by your own code.

    Parameters:
        public_key (``bytes``):
            Public key of the wallet (32 bytes).

        timestamp (``int``):
            The Unix timestamp which was used to create the proof.

        signature (``bytes``):
            The signature proving the ownership (64 bytes).
    """

    def __init__(
        self,
        *,
        public_key: bytes,
        timestamp: int,
        signature: bytes
    ):
        super().__init__()

        self.public_key = public_key
        self.timestamp = timestamp
        self.signature = signature

    def write(self) -> "raw.types.WalletOwnershipProof":
        # Same checks as TDLib's TonWalletOwnershipProof::get_ton_wallet_ownership_proof()
        if len(self.public_key) != 32:
            raise ValueError("Invalid public key specified: it must be 32 bytes long")

        if len(self.signature) != 64:
            raise ValueError("Invalid signature specified: it must be 64 bytes long")

        return raw.types.WalletOwnershipProof(
            timestamp=self.timestamp,
            signature=self.signature
        )
