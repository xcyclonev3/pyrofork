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


class TonWalletState(Object):
    """State of the TON wallet of the current user.

    Parameters:
        is_being_created (``bool``):
            True, if the wallet is being created.

        is_backup_enabled (``bool``):
            True, if the backup of the wallet key is enabled.

        can_enable_backup (``bool``):
            True, if the backup of the wallet key can be enabled with
            :meth:`~pyrogram.Client.enable_ton_wallet_backup`.

        can_export_phrase (``bool``):
            True, if the secret phrase of the wallet can be exported with
            :meth:`~pyrogram.Client.export_ton_wallet_secret_phrase`.

        address (``str``, *optional*):
            Address of the wallet in the TON blockchain. None, if the user has no wallet.

        public_key (``bytes``, *optional*):
            Public key of the wallet. None, if the user has no wallet.

        balance (``int``, *optional*):
            Balance of the wallet, in the smallest units of the cryptocurrency. None, if the user has no wallet.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        is_being_created: bool,
        is_backup_enabled: bool,
        can_enable_backup: bool,
        can_export_phrase: bool,
        address: Optional[str] = None,
        public_key: Optional[bytes] = None,
        balance: Optional[int] = None
    ):
        super().__init__(client)

        self.is_being_created = is_being_created
        self.is_backup_enabled = is_backup_enabled
        self.can_enable_backup = can_enable_backup
        self.can_export_phrase = can_export_phrase
        self.address = address
        self.public_key = public_key
        self.balance = balance

    @staticmethod
    def _parse(client, state: "raw.base.WalletState") -> "TonWalletState":
        if isinstance(state, raw.types.WalletStateEmpty):
            return TonWalletState(
                is_being_created=state.creating,
                is_backup_enabled=False,
                can_enable_backup=False,
                can_export_phrase=False,
                client=client
            )

        return TonWalletState(
            is_being_created=False,
            is_backup_enabled=state.backup_enabled,
            can_enable_backup=state.can_enable_backup,
            can_export_phrase=state.can_export_phrase,
            address=state.address,
            public_key=state.public_key,
            balance=state.balance,
            client=client
        )
