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

from typing import List, Optional

import pyrogram
from pyrogram import raw, types


class EnableTonWalletBackup:
    async def enable_ton_wallet_backup(
        self: "pyrogram.Client",
        encrypted_parts: List[bytes],
        proof: Optional["types.TonWalletOwnershipProof"] = None
    ) -> "types.TonWalletState":
        """Enable the backup of the key of the TON wallet of the current user.

        Works only if :obj:`~pyrogram.types.TonWalletState` *can_enable_backup* is True.

        .. warning::

            The backup contains the secret phrase of the wallet, which gives full control over its funds.
            Pyrofork doesn't implement the encryption of the backup, the parts must be produced by your own code.
            TDLib (``TonWalletManager::enable_ton_wallet_backup``) pads the secret phrase with spaces to 215 bytes,
            splits it into three random parts whose XOR is the padded phrase and encrypts each part for the public
            key of one of the data centers returned by :meth:`~pyrogram.Client.get_ton_wallet_backup_holder_dcs`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            encrypted_parts (List of ``bytes``):
                The encrypted parts of the secret phrase, one for each data center returned by
                :meth:`~pyrogram.Client.get_ton_wallet_backup_holder_dcs`, in the same order.

            proof (:obj:`~pyrogram.types.TonWalletOwnershipProof`, *optional*):
                Proof of the ownership of the wallet. Use
                :meth:`~pyrogram.Client.get_ton_wallet_ownership_proof_challenge` to get a challenge for the proof.
                TDLib always sends it.

        Returns:
            :obj:`~pyrogram.types.TonWalletState`: On success, the new state of the wallet is returned.

        Example:
            .. code-block:: python

                dcs = await app.get_ton_wallet_backup_holder_dcs()
                # parts and proof are computed by your own wallet code
                state = await app.enable_ton_wallet_backup(parts, proof)
        """
        r = await self.invoke(
            raw.functions.wallet.EnableBackup(
                parts=list(encrypted_parts),
                new_public_key=proof.public_key if proof else None,
                proof=proof.write() if proof else None
            )
        )

        return types.TonWalletState._parse(self, r)
