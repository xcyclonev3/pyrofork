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

from typing import Optional

import pyrogram
from pyrogram import raw, types


class GetExternalLink:
    async def get_external_link(
        self: "pyrogram.Client",
        link: str,
        allow_write_access: Optional[bool] = None,
        allow_phone_number_access: Optional[bool] = None
    ) -> Optional[str]:
        """Get an HTTP URL which automatically authorizes the current user on a website after opening an HTTP link.

        Use :meth:`~pyrogram.Client.get_external_link_info` first to find out whether a confirmation of the user is
        needed.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            link (``str``):
                The HTTP link.

            allow_write_access (``bool``, *optional*):
                Pass True if the current user allowed the bot returned by
                :meth:`~pyrogram.Client.get_external_link_info` to send them messages.

            allow_phone_number_access (``bool``, *optional*):
                Pass True to share the phone number of the current user with the bot, if it was requested.

        Returns:
            ``str`` | ``None``: On success, the URL to open is returned, or None if just a notification about the
            successful login has to be shown.

        Example:
            .. code-block:: python

                url = await app.get_external_link("https://example.telegram.org", allow_write_access=True)
        """
        r = await self.invoke(
            raw.functions.messages.AcceptUrlAuth(
                url=link,
                write_allowed=allow_write_access,
                share_phone_number=allow_phone_number_access
            )
        )

        return types.LoginUrlInfo._parse_url(r, link)
