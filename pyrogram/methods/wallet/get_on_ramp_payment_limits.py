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


class GetOnRampPaymentLimits:
    async def get_on_ramp_payment_limits(
        self: "pyrogram.Client",
        provider_id: str,
        cryptocurrency: str,
        base_currency: str,
        payment_method_name: Optional[str] = None
    ) -> "types.OnRampPaymentLimits":
        """Get the limits of an on-ramp payment.

        Only for providers with :obj:`~pyrogram.types.OnRampProvider` *supports_limits*.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            provider_id (``str``):
                Unique identifier of the on-ramp provider.

            cryptocurrency (``str``):
                The cryptocurrency that will be bought.

            base_currency (``str``):
                ISO 4217 code of the currency that will be used for the purchase.

            payment_method_name (``str``, *optional*):
                Name of the chosen payment method, if any.

        Returns:
            :obj:`~pyrogram.types.OnRampPaymentLimits`: On success, the limits are returned.

        Example:
            .. code-block:: python

                limits = await app.get_on_ramp_payment_limits(provider.id, "TON", "USD")
        """
        r = await self.invoke(
            raw.functions.payments.GetOnrampLimits(
                provider=provider_id,
                crypto_currency=cryptocurrency,
                base_currency=base_currency,
                payment_method=payment_method_name or None
            )
        )

        return types.OnRampPaymentLimits._parse(self, r)
