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


class DisableTonWalletBackup:
    async def disable_ton_wallet_backup(
        self: "pyrogram.Client",
        password: Optional[str] = None
    ) -> "types.TonWalletState":
        """Disable the backup of the key of the TON wallet of the current user.

        Works only if :obj:`~pyrogram.types.TonWalletState` *is_backup_enabled* is True.

        .. warning::

            The backup is deleted from Telegram servers: if the secret phrase of the wallet isn't saved elsewhere,
            the funds of the wallet may become inaccessible.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            password (``str``, *optional*):
                The 2-step verification password of the current user.
                As TDLib documents, omit it unless the request without a password fails with ``PASSWORD_MISSING``.

        Returns:
            :obj:`~pyrogram.types.TonWalletState`: On success, the new state of the wallet is returned.

        Example:
            .. code-block:: python

                state = await app.disable_ton_wallet_backup()
        """
        r = await self.invoke(
            raw.functions.wallet.DisableBackup(
                password=utils.compute_password_check(
                    await self.invoke(raw.functions.account.GetPassword()), password
                ) if password else None
            )
        )

        return types.TonWalletState._parse(self, r)
