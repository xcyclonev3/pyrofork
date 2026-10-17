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
from pyrogram import errors, raw, types


class GetExternalLinkInfo:
    async def get_external_link_info(
        self: "pyrogram.Client",
        link: str
    ) -> "types.LoginUrlInfo":
        """Get information about an action to be done when the current user opens an HTTP link.

        Some Telegram websites (the *url_auth_domains* of the app configuration) can log the user in: if an
        authorization confirmation is needed, ask the user and then get the URL to open with
        :meth:`~pyrogram.Client.get_external_link`.

        Unlike the official apps, which only ask Telegram about the links to those websites, the link is always sent
        to Telegram. Like in the official apps, a link that can't be used to log in is returned as an ordinary link
        to open.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            link (``str``):
                The HTTP link.

        Returns:
            :obj:`~pyrogram.types.LoginUrlInfoOpen` | :obj:`~pyrogram.types.LoginUrlInfoRequestConfirmation`: On
            success, what to do with the link is returned.

        Raises:
            UrlExpired: In case the authorization link has expired.
            UrlInvalid: In case the authorization link is invalid.

        Example:
            .. code-block:: python

                info = await app.get_external_link_info("https://example.telegram.org")
        """
        try:
            r = await self.invoke(
                raw.functions.messages.RequestUrlAuth(url=link)
            )
        except (errors.UrlExpired, errors.UrlInvalid):
            raise
        except (errors.BadRequest, errors.Forbidden):
            # As TDLib does: a link that can't be used to log in is opened as an ordinary link
            return types.LoginUrlInfoOpen(url=link, skip_confirmation=False, client=self)

        return types.LoginUrlInfo._parse(self, r, link)
