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

from typing import AsyncGenerator, Optional

import pyrogram
from pyrogram import raw, types


class GetTonWalletTransactions:
    async def get_ton_wallet_transactions(
        self: "pyrogram.Client",
        limit: int = 0,
        offset: str = "",
        is_inbound: Optional[bool] = None,
        is_outbound: Optional[bool] = None
    ) -> Optional[AsyncGenerator["types.TonWalletTransaction", None]]:
        """Get the transactions of the TON wallet of the current user.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            limit (``int``, *optional*):
                Limits the number of transactions to be retrieved.
                By default, no limit is applied and all transactions are returned.

            offset (``str``, *optional*):
                Offset of the first transaction to be returned, as received from a previous request.
                By default, transactions are returned starting from the first one.

            is_inbound (``bool``, *optional*):
                Pass True to get only incoming transactions.

            is_outbound (``bool``, *optional*):
                Pass True to get only outgoing transactions.

        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.TonWalletTransaction` objects.

        Example:
            .. code-block:: python

                # Get all transactions
                async for transaction in app.get_ton_wallet_transactions():
                    print(transaction.amount, transaction.peer_address)

                # Get the last 10 incoming transactions
                async for transaction in app.get_ton_wallet_transactions(limit=10, is_inbound=True):
                    print(transaction)
        """
        current = 0
        total = abs(limit) or (1 << 31) - 1
        limit = min(100, total)

        while True:
            r = await self.invoke(
                raw.functions.wallet.GetTransactions(
                    offset=offset,
                    limit=limit,
                    inbound=is_inbound,
                    outbound=is_outbound
                )
            )

            users = {i.id: i for i in r.users}

            transactions = [
                types.TonWalletTransaction._parse(self, transaction, users)
                for transaction in r.transactions
            ]

            if not transactions:
                return

            for transaction in transactions:
                yield transaction

                current += 1

                if current >= total:
                    return

            offset = r.next_offset

            if not offset:
                return
