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

from typing import Optional, Union

import pyrogram
from pyrogram import raw, types
from pyrogram.types.rich_messages.input_rich_message import get_input_user


class SendTonWalletTransfer:
    async def send_ton_wallet_transfer(
        self: "pyrogram.Client",
        regular_transfer_data: bytes,
        gasless_transfer_data: Optional[bytes] = None,
        user_id: Union[int, str] = None
    ) -> Optional["types.TonWalletTransferResult"]:
        """Send cryptocurrency from the TON wallet of the current user.

        .. warning::

            This method moves funds. The transfer requests must be built and signed on the client side with the private
            key of the wallet; Pyrofork only submits them and doesn't implement TON cryptography. A sent transfer
            can't be canceled.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            regular_transfer_data (``bytes``):
                Serialized data of the regular transfer request.
                TDLib documents the minimum transfer amount as the option ``ton_wallet_transfer_amount_min``.

            gasless_transfer_data (``bytes``, *optional*):
                Serialized data of the gasless transfer request, whose fee is paid by Telegram.
                Omit it to force a regular transfer. TDLib documents the minimum amount of a gasless transfer as the
                option ``ton_wallet_gasless_transfer_amount_min``.

            user_id (``int`` | ``str``, *optional*):
                Unique identifier (int) or username (str) of the user who receives the transfer, if known.

        Returns:
            :obj:`~pyrogram.types.TonWalletTransferResult`: On success, the result of the transfer is returned.
            None is returned if the server doesn't report the sent transfer: it may have been made anyway, check
            :meth:`~pyrogram.Client.get_ton_wallet_transactions` before sending it again.

        Example:
            .. code-block:: python

                # regular_data and gasless_data are signed transfer requests built by your TON wallet code
                result = await app.send_ton_wallet_transfer(regular_data, gasless_data, user_id="username")

                if result.transaction is None:
                    transaction = await app.get_ton_wallet_transaction_by_msg_hash(result.msg_hash)
        """
        r = await self.invoke(
            raw.functions.wallet.SendTransfer(
                data_normal=regular_transfer_data,
                data_gasless=gasless_transfer_data or None,
                user_id=(
                    get_input_user(await self.resolve_peer(user_id))
                    if user_id is not None
                    else raw.types.InputUserEmpty()
                ),
                random_id=self.rnd_id()
            )
        )

        if isinstance(r, (raw.types.Updates, raw.types.UpdatesCombined)):
            updates, users = r.updates, {i.id: i for i in r.users}
        else:
            updates, users = [getattr(r, "update", None)], {}

        for update in updates:
            if isinstance(update, raw.types.UpdateSentWalletTransaction):
                return types.TonWalletTransferResult._parse(self, update, users)
