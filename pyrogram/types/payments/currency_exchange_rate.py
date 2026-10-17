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
from ..object import Object


class CurrencyExchangeRate(Object):
    """Describes the exchange rate of a currency to USD.

    Parameters:
        currency (``str``):
            ISO 4217 currency code of the currency.

        rate (``float``):
            Exchange rate of the currency to USD.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        currency: str,
        rate: float
    ):
        super().__init__(client)

        self.currency = currency
        self.rate = rate

    @staticmethod
    def _parse(client, currency_rate: "raw.types.CurrencyRate") -> "CurrencyExchangeRate":
        return CurrencyExchangeRate(
            currency=currency_rate.currency,
            rate=currency_rate.rate,
            client=client
        )
