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
from pyrogram import raw
from ..messages.inline_session import get_session


class GetTonWalletEncryptedSecretPhrasePart:
    async def get_ton_wallet_encrypted_secret_phrase_part(
        self: "pyrogram.Client",
        token: str,
        public_key: bytes,
        dc_id: int
    ) -> bytes:
        """Fetch an encrypted part of the secret phrase of the TON wallet of the current user from a data center.

        .. warning::

            The parts reveal the secret phrase of the wallet, which gives full control over its funds.
            See :meth:`~pyrogram.Client.export_ton_wallet_secret_phrase` for how TDLib decrypts and combines them;
            Pyrofork doesn't implement this cryptography.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            token (``str``):
                The token returned by :meth:`~pyrogram.Client.export_ton_wallet_secret_phrase`.

            public_key (``bytes``):
                Public key for which the part is encrypted. TDLib uses a new temporary key pair for each part.

            dc_id (``int``):
                Identifier of the data center which holds the part, as returned by
                :meth:`~pyrogram.Client.get_ton_wallet_backup_holder_dcs`. Like TDLib, the request is sent to that
                data center.

        Returns:
            ``bytes``: On success, the encrypted part is returned. TDLib expects 288 bytes: a 32-byte public key used
            for the key agreement, followed by the encrypted part.

        Example:
            .. code-block:: python

                data = await app.get_ton_wallet_encrypted_secret_phrase_part(token, public_key, dc_id)
        """
        session = await get_session(self, dc_id)

        r = await session.invoke(
            raw.functions.wallet.FetchEncryptedSecretPhrasePart(
                token=token,
                public_key=public_key
            )
        )

        return r.data
