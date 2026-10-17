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


class OnRampPaymentSession(Object):
    """A session for a cryptocurrency purchase through an on-ramp provider.

    Parameters:
        provider_id (``str``):
            Unique identifier of the on-ramp provider.

        session_id (``str``):
            Identifier of the session.

        url (``str``):
            HTTPS URL to be opened in a WebView to complete the purchase.

        expiration_date (:py:obj:`~datetime.datetime`):
            Date the session expires.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        provider_id: str,
        session_id: str,
        url: str,
        expiration_date: datetime
    ):
        super().__init__(client)

        self.provider_id = provider_id
        self.session_id = session_id
        self.url = url
        self.expiration_date = expiration_date

    @staticmethod
    def _parse(client, session: "raw.types.OnrampSession") -> "OnRampPaymentSession":
        return OnRampPaymentSession(
            provider_id=session.provider,
            session_id=session.session_id,
            url=session.url,
            expiration_date=utils.timestamp_to_datetime(session.expires_date),
            client=client
        )
