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

from typing import List

import pyrogram
from pyrogram import raw, types


class GetCurrencyExchangeRates:
    async def get_currency_exchange_rates(
        self: "pyrogram.Client"
    ) -> List["types.CurrencyExchangeRate"]:
        """Get the exchange rates of currencies to USD.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            List of :obj:`~pyrogram.types.CurrencyExchangeRate`: On success, the exchange rates are returned.

        Example:
            .. code-block:: python

                rates = await app.get_currency_exchange_rates()

                for rate in rates:
                    print(rate.currency, rate.rate)
        """
        r = await self.invoke(raw.functions.payments.GetCurrencyRates())

        return types.List([types.CurrencyExchangeRate._parse(self, rate) for rate in r.rates])
