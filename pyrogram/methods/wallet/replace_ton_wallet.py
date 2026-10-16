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
from pyrogram import raw, types, utils


class ReplaceTonWallet:
    async def replace_ton_wallet(
        self: "pyrogram.Client",
        proof: "types.TonWalletOwnershipProof",
        anchor_public_key: Optional[bytes] = None,
        password: Optional[str] = None
    ) -> "types.TonWalletState":
        """Replace the current TON wallet of the current user with another existing v6r2 wallet, or change the private
        key of the current TON wallet.

        .. warning::

            This changes the wallet, or the key of the wallet, used by Telegram for the current user. Make sure the
            secret phrases of both the old and the new key are saved: losing them can make funds inaccessible.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            proof (:obj:`~pyrogram.types.TonWalletOwnershipProof`):
                Proof of the ownership of the new private key of the wallet; its *public_key* is the new public key.
                Use :meth:`~pyrogram.Client.get_ton_wallet_ownership_proof_challenge` to get a challenge for the proof.

            anchor_public_key (``bytes``, *optional*):
                Original public key of the new wallet.

            password (``str``, *optional*):
                The 2-step verification password of the current user.
                As TDLib documents, omit it unless the request without a password fails with ``PASSWORD_MISSING``.

        Returns:
            :obj:`~pyrogram.types.TonWalletState`: On success, the state of the new wallet is returned.

        Example:
            .. code-block:: python

                proof = types.TonWalletOwnershipProof(public_key=public_key, timestamp=timestamp, signature=signature)
                state = await app.replace_ton_wallet(proof, password="password")
        """
        r = await self.invoke(
            raw.functions.wallet.ReplaceWallet(
                wallet=raw.types.InputWalletImported(
                    public_key=proof.public_key,
                    anchor_public_key=anchor_public_key or None,
                    proof=proof.write()
                ),
                password=utils.compute_password_check(
                    await self.invoke(raw.functions.account.GetPassword()), password
                ) if password else None
            )
        )

        return types.TonWalletState._parse(self, r)
