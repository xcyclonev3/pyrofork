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
from ..object import Object


class OnRampPaymentAvailability(Object):
    """Availability of cryptocurrency purchases through an on-ramp provider for the current user.

    Parameters:
        is_allowed (``bool``):
            True, if the current user can use the provider.

        is_purchase_allowed (``bool``):
            True, if the current user can purchase through the provider.

        methods (List of :obj:`~pyrogram.types.OnRampPaymentMethod`):
            The payment methods of the provider.

        country_code (``str``):
            Two-letter ISO 3166-1 alpha-2 code of the country of the current user.

        state (``str``, *optional*):
            Code of the state of the current user, if known.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        is_allowed: bool,
        is_purchase_allowed: bool,
        methods: List["types.OnRampPaymentMethod"],
        country_code: str,
        state: Optional[str] = None
    ):
        super().__init__(client)

        self.is_allowed = is_allowed
        self.is_purchase_allowed = is_purchase_allowed
        self.methods = methods
        self.country_code = country_code
        self.state = state

    @staticmethod
    def _parse(client, availability: "raw.types.OnrampAvailability") -> "OnRampPaymentAvailability":
        return OnRampPaymentAvailability(
            is_allowed=availability.allowed,
            is_purchase_allowed=availability.buy_allowed,
            methods=types.List(types.OnRampPaymentMethod._parse(client, method) for method in availability.methods),
            country_code=availability.country_code,
            state=availability.state,
            client=client
        )
