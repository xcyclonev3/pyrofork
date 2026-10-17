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


class TonCenterStreamingApiUrl(Object):
    """An HTTP URL of the TON Center Streaming API.

    Parameters:
        url (``str``):
            The URL.

        expiration_date (:py:obj:`~datetime.datetime`):
            Date the URL expires.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        url: str,
        expiration_date: datetime
    ):
        super().__init__(client)

        self.url = url
        self.expiration_date = expiration_date

    @staticmethod
    def _parse(client, streaming_url: "raw.types.toncenter.StreamingUrl") -> "TonCenterStreamingApiUrl":
        return TonCenterStreamingApiUrl(
            url=streaming_url.url,
            expiration_date=utils.timestamp_to_datetime(streaming_url.expires),
            client=client
        )
