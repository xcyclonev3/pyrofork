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

import pyrogram
from pyrogram import raw, types


class DisableTonWalletBackupWithProof:
    async def disable_ton_wallet_backup_with_proof(
        self: "pyrogram.Client",
        proof: "types.TonWalletOwnershipProof"
    ) -> "types.TonWalletState":
        """Disable the backup of the key of the TON wallet of the current user, proving the ownership of the wallet
        instead of using the 2-step verification password.

        Works only if :obj:`~pyrogram.types.TonWalletState` *is_backup_enabled* is True.

        .. warning::

            The backup is deleted from Telegram servers: if the secret phrase of the wallet isn't saved elsewhere,
            the funds of the wallet may become inaccessible.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            proof (:obj:`~pyrogram.types.TonWalletOwnershipProof`):
                Proof of the ownership of the wallet. Use
                :meth:`~pyrogram.Client.get_ton_wallet_ownership_proof_challenge` to get a challenge for the proof.

        Returns:
            :obj:`~pyrogram.types.TonWalletState`: On success, the new state of the wallet is returned.

        Example:
            .. code-block:: python

                challenge = await app.get_ton_wallet_ownership_proof_challenge()
                # sign the challenge with your own wallet code
                proof = types.TonWalletOwnershipProof(public_key=public_key, timestamp=timestamp, signature=signature)
                state = await app.disable_ton_wallet_backup_with_proof(proof)
        """
        r = await self.invoke(
            raw.functions.wallet.DisableBackup(
                new_public_key=proof.public_key,
                proof=proof.write()
            )
        )

        return types.TonWalletState._parse(self, r)
