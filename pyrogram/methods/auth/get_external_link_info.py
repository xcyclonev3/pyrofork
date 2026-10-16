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


class GetExternalLinkInfo:
    async def get_external_link_info(
        self: "pyrogram.Client",
        link: str
    ) -> "types.LoginUrlInfo":
        """Get information about an action to be done when the current user opens an HTTP link.

        Some Telegram websites (the *url_auth_domains* of the app configuration) can log the user in: if an
        authorization confirmation is needed, ask the user and then get the URL to open with
        :meth:`~pyrogram.Client.get_external_link`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            link (``str``):
                The HTTP link.

        Returns:
            :obj:`~pyrogram.types.LoginUrlInfoOpen` | :obj:`~pyrogram.types.LoginUrlInfoRequestConfirmation`: On
            success, what to do with the link is returned.

        Example:
            .. code-block:: python

                info = await app.get_external_link_info("https://example.telegram.org")
        """
        r = await self.invoke(
            raw.functions.messages.RequestUrlAuth(url=link)
        )

        return types.LoginUrlInfo._parse(self, r, link)
