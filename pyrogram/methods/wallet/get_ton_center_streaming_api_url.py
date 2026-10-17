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

import pyrogram
from pyrogram import raw, types


class GetTonCenterStreamingApiUrl:
    async def get_ton_center_streaming_api_url(
        self: "pyrogram.Client"
    ) -> "types.TonCenterStreamingApiUrl":
        """Get an HTTP URL of the TON Center Streaming API.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            :obj:`~pyrogram.types.TonCenterStreamingApiUrl`: On success, the URL and its expiration date are returned.

        Example:
            .. code-block:: python

                streaming_url = await app.get_ton_center_streaming_api_url()
                print(streaming_url.url, streaming_url.expiration_date)
        """
        r = await self.invoke(raw.functions.toncenter.GetStreamingUrl())

        return types.TonCenterStreamingApiUrl._parse(self, r)
