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

from typing import Iterable, List, Optional, Union

import pyrogram
from pyrogram import raw, types


class GetTonWalletTransactionByMsgHash:
    async def get_ton_wallet_transaction_by_msg_hash(
        self: "pyrogram.Client",
        msg_hash: Union[str, Iterable[str]]
    ) -> Union[Optional["types.TonWalletTransaction"], List["types.TonWalletTransaction"]]:
        """Get transactions of the TON wallet of the current user by the hashes of their messages.

        Use it to poll the result of :meth:`~pyrogram.Client.send_ton_wallet_transfer`. Telegram returns an error if
        the transaction isn't finalized yet.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            msg_hash (``str`` | Iterable of ``str``):
                A message hash, as returned in :obj:`~pyrogram.types.TonWalletTransferResult`, or an iterable of message
                hashes.

        Returns:
            :obj:`~pyrogram.types.TonWalletTransaction` | List of :obj:`~pyrogram.types.TonWalletTransaction`: In case
            *msg_hash* was a single hash, a single transaction is returned (None, if it wasn't found), otherwise a
            list of the found transactions is returned.

        Example:
            .. code-block:: python

                result = await app.send_ton_wallet_transfer(transfer_data)
                transaction = await app.get_ton_wallet_transaction_by_msg_hash(result.msg_hash)
        """
        is_iterable = not isinstance(msg_hash, str)
        hashes = list(msg_hash) if is_iterable else [msg_hash]

        r = await self.invoke(raw.functions.wallet.GetTransactionsByMsgHash(msg_hash=hashes))

        users = {i.id: i for i in r.users}

        transactions = types.List(
            types.TonWalletTransaction._parse(self, transaction, users)
            for transaction in r.transactions
        )

        return transactions if is_iterable else (transactions[0] if transactions else None)
