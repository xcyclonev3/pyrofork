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


class GetTonWalletOwnershipProofChallenge:
    async def get_ton_wallet_ownership_proof_challenge(
        self: "pyrogram.Client"
    ) -> "types.TonWalletOwnershipProofChallenge":
        """Get a challenge which can be used to prove the ownership of a TON wallet.

        Sign it with the private key of the wallet and pass the result as a
        :obj:`~pyrogram.types.TonWalletOwnershipProof` to the methods which require a proof.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            :obj:`~pyrogram.types.TonWalletOwnershipProofChallenge`: On success, the challenge is returned.

        Example:
            .. code-block:: python

                challenge = await app.get_ton_wallet_ownership_proof_challenge()
                print(challenge.payload, challenge.domain)
        """
        r = await self.invoke(raw.functions.wallet.GetProofChallenge())

        return types.TonWalletOwnershipProofChallenge._parse(self, r)
