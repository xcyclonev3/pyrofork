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
from ..object import Object


class OnRampProvider(Object):
    """An on-ramp provider, which sells cryptocurrency for fiat currencies.

    Parameters:
        id (``str``):
            Unique identifier of the provider.

        name (``str``):
            Name of the provider.

        cryptocurrencies (List of ``str``):
            Cryptocurrencies supported by the provider.

        supports_base_currencies (``bool``):
            True, if the provider supports base currencies and can be used with
            :meth:`~pyrogram.Client.get_on_ramp_provider_base_currencies`.

        supports_limits (``bool``):
            True, if the provider supports :meth:`~pyrogram.Client.get_on_ramp_payment_limits`.

        supports_quote (``bool``):
            True, if the provider supports :meth:`~pyrogram.Client.get_on_ramp_payment_quote`.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        id: str,
        name: str,
        cryptocurrencies: List[str],
        supports_base_currencies: bool,
        supports_limits: bool,
        supports_quote: bool
    ):
        super().__init__(client)

        self.id = id
        self.name = name
        self.cryptocurrencies = cryptocurrencies
        self.supports_base_currencies = supports_base_currencies
        self.supports_limits = supports_limits
        self.supports_quote = supports_quote

    @staticmethod
    def _parse(client, provider: "raw.types.OnrampProviderInfo") -> "OnRampProvider":
        return OnRampProvider(
            id=provider.id,
            name=provider.name,
            cryptocurrencies=provider.crypto_currencies,
            supports_base_currencies=provider.supports_base_currencies,
            supports_limits=provider.supports_limits,
            supports_quote=provider.supports_quote,
            client=client
        )
