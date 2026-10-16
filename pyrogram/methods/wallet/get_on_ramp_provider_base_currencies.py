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
from pyrogram import raw


class GetOnRampProviderBaseCurrencies:
    async def get_on_ramp_provider_base_currencies(
        self: "pyrogram.Client",
        provider_id: str,
        cryptocurrency: str
    ) -> List[str]:
        """Get the fiat currencies supported by an on-ramp provider to buy a cryptocurrency.

        Only for providers with :obj:`~pyrogram.types.OnRampProvider` *supports_base_currencies*.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            provider_id (``str``):
                Unique identifier of the on-ramp provider.

            cryptocurrency (``str``):
                The cryptocurrency that will be bought.

        Returns:
            List of ``str``: On success, the ISO 4217 codes of the currencies are returned.

        Example:
            .. code-block:: python

                currencies = await app.get_on_ramp_provider_base_currencies(provider.id, "TON")
        """
        r = await self.invoke(
            raw.functions.payments.GetOnrampBaseCurrencies(
                provider=provider_id,
                crypto_currency=cryptocurrency
            )
        )

        return list(r)
