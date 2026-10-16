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

from typing import List, Optional

import pyrogram
from pyrogram import raw, types


class GetOnRampProviders:
    async def get_on_ramp_providers(
        self: "pyrogram.Client",
        cryptocurrency: Optional[str] = None
    ) -> List["types.OnRampProvider"]:
        """Get the available on-ramp providers, which sell cryptocurrency for fiat currencies.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            cryptocurrency (``str``, *optional*):
                Pass a cryptocurrency to get only the providers that support it.
                By default, all providers are returned.

        Returns:
            List of :obj:`~pyrogram.types.OnRampProvider`: On success, the list of providers is returned.

        Example:
            .. code-block:: python

                for provider in await app.get_on_ramp_providers("TON"):
                    print(provider.id, provider.name)
        """
        r = await self.invoke(
            raw.functions.payments.GetOnrampProviders(
                crypto_currency=cryptocurrency or None
            )
        )

        return types.List(types.OnRampProvider._parse(self, provider) for provider in r)
