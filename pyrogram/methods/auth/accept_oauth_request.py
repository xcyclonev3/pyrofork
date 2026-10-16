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


class AcceptOauthRequest:
    async def accept_oauth_request(
        self: "pyrogram.Client",
        url: str,
        match_code: Optional[str] = None,
        allow_write_access: Optional[bool] = None,
        allow_phone_number_access: Optional[bool] = None
    ) -> Optional[str]:
        """Accept an OAuth authorization request.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            url (``str``):
                The OAuth deep link.

            match_code (``str``, *optional*):
                The code chosen by the user among :obj:`~pyrogram.types.OauthLinkInfo` *match_codes*.
                Required if the request has match codes, even if it was already checked with
                :meth:`~pyrogram.Client.check_oauth_request_match_code`.

            allow_write_access (``bool``, *optional*):
                Pass True if the current user allowed the bot returned by :meth:`~pyrogram.Client.get_oauth_link_info`
                to send them messages.

            allow_phone_number_access (``bool``, *optional*):
                Pass True if the current user allowed the bot returned by :meth:`~pyrogram.Client.get_oauth_link_info`
                to access their phone number.

        Returns:
            ``str`` | ``None``: On success, the HTTP URL to open (in an external browser, possibly with a custom
            scheme) is returned, or None if just a notification about the successful login has to be shown.

        Example:
            .. code-block:: python

                url = await app.accept_oauth_request(link, match_code=info.match_codes[0], allow_write_access=True)
        """
        r = await self.invoke(
            raw.functions.messages.AcceptUrlAuth(
                url=url,
                match_code=match_code,
                write_allowed=allow_write_access,
                share_phone_number=allow_phone_number_access
            )
        )

        return types.LoginUrlInfo._parse_url(r, url)
