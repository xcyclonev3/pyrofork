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

import weakref
from typing import Optional

import pyrogram
from pyrogram import raw
from ..messages.inline_session import get_session

# Like TDLib, TON Center API requests are sent to the data center which serves web files (Config.webfile_dc_id),
# fetched once per client.
_webfile_dc_ids = weakref.WeakKeyDictionary()


class SendTonCenterApiRequest:
    async def send_ton_center_api_request(
        self: "pyrogram.Client",
        endpoint: str,
        query: Optional[str] = None,
        payload: Optional[str] = None
    ) -> str:
        """Send a request to the TON Center API through Telegram.

        A GET request is sent, unless a *payload* is passed: then a POST request is sent.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            endpoint (``str``):
                The path of the requested endpoint.
                See https://toncenter.com/api/v3/index.html for the list of supported endpoints.

            query (``str``, *optional*):
                URL-encoded query of a GET request.

            payload (``str``, *optional*):
                Payload of a POST request.

        Returns:
            ``str``: On success, the response of the TON Center API is returned, as JSON text.

        Raises:
            ValueError: In case both *query* and *payload* are passed.

        Example:
            .. code-block:: python

                import json

                # GET request
                response = await app.send_ton_center_api_request(endpoint, query=urlencoded_query)
                print(json.loads(response))

                # POST request
                response = await app.send_ton_center_api_request(endpoint, payload=json.dumps(data))
        """
        if query is not None and payload is not None:
            raise ValueError("Pass either a query (GET request) or a payload (POST request), not both")

        dc_id = _webfile_dc_ids.get(self)

        if dc_id is None:
            config = await self.invoke(raw.functions.help.GetConfig())
            dc_id = _webfile_dc_ids[self] = config.webfile_dc_id

        session = await get_session(self, dc_id, is_media=False)

        r = await session.invoke(
            raw.functions.toncenter.PerformApiRequest(
                endpoint=endpoint,
                post=payload is not None,
                query=query or None,
                payload=payload or None
            )
        )

        return r.response.data
