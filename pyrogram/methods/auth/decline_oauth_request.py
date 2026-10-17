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
from pyrogram import raw


class DeclineOauthRequest:
    async def decline_oauth_request(
        self: "pyrogram.Client",
        url: str
    ) -> bool:
        """Decline an OAuth authorization request, notifying Telegram that the user refused to log in.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            url (``str``):
                The OAuth deep link, as returned in *url* of :obj:`~pyrogram.types.OauthLinkInfo`.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                await app.decline_oauth_request(link)
        """
        return await self.invoke(
            raw.functions.messages.DeclineUrlAuth(url=url)
        )
