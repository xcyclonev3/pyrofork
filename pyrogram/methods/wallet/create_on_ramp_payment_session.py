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


class CreateOnRampPaymentSession:
    async def create_on_ramp_payment_session(
        self: "pyrogram.Client",
        provider_id: str,
        cryptocurrency: str,
        address: str,
        payment_method_name: Optional[str] = None,
        base_currency: Optional[str] = None,
        cryptocurrency_amount: Optional[str] = None,
        base_currency_amount: Optional[str] = None,
        memo: Optional[str] = None,
        theme: Optional[str] = None,
        success_return_url: Optional[str] = None,
        fail_return_url: Optional[str] = None
    ) -> "types.OnRampPaymentSession":
        """Create a session for an on-ramp payment, to buy cryptocurrency for a fiat currency.

        .. note::

            The purchase is completed by the user in the returned URL, and the bought cryptocurrency is sent to
            *address*: double-check the address.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            provider_id (``str``):
                Unique identifier of the on-ramp provider.

            cryptocurrency (``str``):
                The cryptocurrency that will be bought.

            address (``str``):
                Address to which the cryptocurrency will be sent.

            payment_method_name (``str``, *optional*):
                Name of the chosen payment method, if any.

            base_currency (``str``, *optional*):
                ISO 4217 code of the currency that will be used for the purchase, if known.

            cryptocurrency_amount (``str``, *optional*):
                The amount of the cryptocurrency to buy. Pass either it or *base_currency_amount*.

            base_currency_amount (``str``, *optional*):
                The amount of the base currency to spend. Pass either it or *cryptocurrency_amount*.

            memo (``str``, *optional*):
                Memo for the purchase.

            theme (``str``, *optional*):
                Preferred theme of the WebView: "light" or "dark". By default, the default theme is used.

            success_return_url (``str``, *optional*):
                The URL opened on successful payment.

            fail_return_url (``str``, *optional*):
                The URL opened on failed payment.

        Returns:
            :obj:`~pyrogram.types.OnRampPaymentSession`: On success, the session is returned.

        Example:
            .. code-block:: python

                session = await app.create_on_ramp_payment_session(
                    provider.id, "TON", state.address, base_currency="USD", base_currency_amount="50"
                )
                print(session.url)
        """
        r = await self.invoke(
            raw.functions.payments.CreateOnrampSession(
                provider=provider_id,
                crypto_currency=cryptocurrency,
                address=address,
                payment_method=payment_method_name or None,
                base_currency=base_currency or None,
                base_amount=base_currency_amount or None,
                memo=memo or None,
                theme=theme or None,
                success_return_url=success_return_url or None,
                fail_return_url=fail_return_url or None,
                crypto_amount=cryptocurrency_amount or None
            )
        )

        return types.OnRampPaymentSession._parse(self, r)
