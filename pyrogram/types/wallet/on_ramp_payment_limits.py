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


class OnRampPaymentLimits(Object):
    """Limits of a cryptocurrency purchase through an on-ramp provider.

    Amounts are strings, as provided by the on-ramp provider.

    Parameters:
        payment_method (``str``):
            The payment method the limits apply to.

        base_currency (``str``):
            ISO 4217 code of the currency used for the purchase.

        base_currency_min_amount (``str``):
            The minimum allowed amount of the base currency.

        base_currency_max_amount (``str``):
            The maximum allowed amount of the base currency.

        cryptocurrency_min_amount (``str``):
            The minimum allowed amount of the bought cryptocurrency.

        cryptocurrency_max_amount (``str``):
            The maximum allowed amount of the bought cryptocurrency.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        payment_method: str,
        base_currency: str,
        base_currency_min_amount: str,
        base_currency_max_amount: str,
        cryptocurrency_min_amount: str,
        cryptocurrency_max_amount: str
    ):
        super().__init__(client)

        self.payment_method = payment_method
        self.base_currency = base_currency
        self.base_currency_min_amount = base_currency_min_amount
        self.base_currency_max_amount = base_currency_max_amount
        self.cryptocurrency_min_amount = cryptocurrency_min_amount
        self.cryptocurrency_max_amount = cryptocurrency_max_amount

    @staticmethod
    def _parse(client, limits: "raw.types.OnrampLimits") -> "OnRampPaymentLimits":
        return OnRampPaymentLimits(
            payment_method=limits.payment_method,
            base_currency=limits.base_currency,
            base_currency_min_amount=limits.base_min_amount,
            base_currency_max_amount=limits.base_max_amount,
            cryptocurrency_min_amount=limits.crypto_min_amount,
            cryptocurrency_max_amount=limits.crypto_max_amount,
            client=client
        )
