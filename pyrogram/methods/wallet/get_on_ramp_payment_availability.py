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


class GetOnRampPaymentAvailability:
    async def get_on_ramp_payment_availability(
        self: "pyrogram.Client",
        provider_id: str,
        cryptocurrency: str,
        base_currency: Optional[str] = None
    ) -> "types.OnRampPaymentAvailability":
        """Get the on-ramp payment methods available to the current user, based on the user's location.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            provider_id (``str``):
                Unique identifier of the on-ramp provider.

            cryptocurrency (``str``):
                The cryptocurrency that will be bought.

            base_currency (``str``, *optional*):
                ISO 4217 code of the currency that will be used for the purchase, if known.

        Returns:
            :obj:`~pyrogram.types.OnRampPaymentAvailability`: On success, the availability of the provider is returned.

        Example:
            .. code-block:: python

                availability = await app.get_on_ramp_payment_availability(provider.id, "TON", "USD")
        """
        r = await self.invoke(
            raw.functions.payments.GetOnrampAvailability(
                provider=provider_id,
                crypto_currency=cryptocurrency,
                base_currency=base_currency or None
            )
        )

        return types.OnRampPaymentAvailability._parse(self, r)
