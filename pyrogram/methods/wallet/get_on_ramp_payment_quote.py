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
from pyrogram import raw, types


class GetOnRampPaymentQuote:
    async def get_on_ramp_payment_quote(
        self: "pyrogram.Client",
        provider_id: str,
        cryptocurrency: str,
        base_currency: str,
        cryptocurrency_amount: Optional[str] = None,
        base_currency_amount: Optional[str] = None,
        payment_method_name: Optional[str] = None
    ) -> "types.OnRampPaymentQuote":
        """Get a quote for an on-ramp payment.

        Only for providers with :obj:`~pyrogram.types.OnRampProvider` *supports_quote*.
        Pass either *cryptocurrency_amount* or *base_currency_amount*.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            provider_id (``str``):
                Unique identifier of the on-ramp provider.

            cryptocurrency (``str``):
                The cryptocurrency that will be bought.

            base_currency (``str``):
                ISO 4217 code of the currency that will be used for the purchase.

            cryptocurrency_amount (``str``, *optional*):
                The amount of the cryptocurrency to buy.

            base_currency_amount (``str``, *optional*):
                The amount of the base currency to spend.

            payment_method_name (``str``, *optional*):
                Name of the chosen payment method, if any.

        Returns:
            :obj:`~pyrogram.types.OnRampPaymentQuote`: On success, the quote is returned.

        Example:
            .. code-block:: python

                quote = await app.get_on_ramp_payment_quote(provider.id, "TON", "USD", base_currency_amount="50")
                print(quote.cryptocurrency_amount, quote.total_amount)
        """
        r = await self.invoke(
            raw.functions.payments.GetOnrampQuote(
                provider=provider_id,
                crypto_currency=cryptocurrency,
                base_currency=base_currency,
                base_amount=base_currency_amount or None,
                crypto_amount=cryptocurrency_amount or None,
                payment_method=payment_method_name or None
            )
        )

        return types.OnRampPaymentQuote._parse(self, r)
