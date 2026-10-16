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

from datetime import datetime

import pyrogram
from pyrogram import raw, utils
from ..object import Object


class OnRampPaymentQuote(Object):
    """A quote for a cryptocurrency purchase through an on-ramp provider.

    Amounts and prices are strings, as provided by the on-ramp provider.

    Parameters:
        payment_method (``str``):
            The payment method to be used.

        base_currency (``str``):
            ISO 4217 code of the currency used for the purchase.

        base_currency_amount (``str``):
            Paid amount of the base currency.

        cryptocurrency (``str``):
            The bought cryptocurrency.

        cryptocurrency_amount (``str``):
            Purchased amount of the cryptocurrency.

        cryptocurrency_price (``str``):
            Price of the cryptocurrency.

        fee_amount (``str``):
            Fee amount.

        extra_fee_amount (``str``):
            Extra fee amount.

        network_fee_amount (``str``):
            Network fee amount.

        total_amount (``str``):
            Total paid amount.

        expiration_date (:py:obj:`~datetime.datetime`):
            Date the quote expires.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        payment_method: str,
        base_currency: str,
        base_currency_amount: str,
        cryptocurrency: str,
        cryptocurrency_amount: str,
        cryptocurrency_price: str,
        fee_amount: str,
        extra_fee_amount: str,
        network_fee_amount: str,
        total_amount: str,
        expiration_date: datetime
    ):
        super().__init__(client)

        self.payment_method = payment_method
        self.base_currency = base_currency
        self.base_currency_amount = base_currency_amount
        self.cryptocurrency = cryptocurrency
        self.cryptocurrency_amount = cryptocurrency_amount
        self.cryptocurrency_price = cryptocurrency_price
        self.fee_amount = fee_amount
        self.extra_fee_amount = extra_fee_amount
        self.network_fee_amount = network_fee_amount
        self.total_amount = total_amount
        self.expiration_date = expiration_date

    @staticmethod
    def _parse(client, quote: "raw.types.OnrampQuote") -> "OnRampPaymentQuote":
        return OnRampPaymentQuote(
            payment_method=quote.payment_method,
            base_currency=quote.base_currency,
            base_currency_amount=quote.base_amount,
            cryptocurrency=quote.crypto_currency,
            cryptocurrency_amount=quote.crypto_amount,
            cryptocurrency_price=quote.crypto_price,
            fee_amount=quote.fee_amount,
            extra_fee_amount=quote.extra_fee_amount,
            network_fee_amount=quote.network_fee_amount,
            total_amount=quote.total_amount,
            expiration_date=utils.timestamp_to_datetime(quote.expires_date),
            client=client
        )
