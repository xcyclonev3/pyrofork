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
from pyrogram import errors, raw, types


class GetOauthLinkInfo:
    async def get_oauth_link_info(
        self: "pyrogram.Client",
        url: str,
        in_app_origin: Optional[str] = None
    ) -> "types.OauthLinkInfo":
        """Get information about an OAuth authorization request (login with Telegram on a website or in an app).

        Only handle OAuth links received from outside of the application (e.g. opened by the operating system or
        from a push notification), not links clicked inside of it.
        Then show the request to the user and process it with
        :meth:`~pyrogram.Client.check_oauth_request_match_code`, :meth:`~pyrogram.Client.accept_oauth_request`
        or :meth:`~pyrogram.Client.decline_oauth_request`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            url (``str``):
                The whole OAuth deep link, e.g. *tg://oauth?token=...*.

            in_app_origin (``str``, *optional*):
                Origin of the in-app browser or mini app the request was received from, in the format
                *scheme://host* (or *scheme://host:port* for non-default ports).
                Omit it for requests received from outside of the application.

        Returns:
            :obj:`~pyrogram.types.OauthLinkInfo`: On success, information about the authorization request is
            returned.

        Raises:
            UrlExpired: In case the authorization request is no longer valid.

        Example:
            .. code-block:: python

                info = await app.get_oauth_link_info(link)
                print(info.domain, info.bot.username, info.match_codes)
        """
        r = await self.invoke(
            raw.functions.messages.RequestUrlAuth(
                url=url,
                in_app_origin=in_app_origin or None
            )
        )

        if not isinstance(r, raw.types.UrlAuthResultRequest):
            # As TDLib does: only a confirmation request is a valid answer for an OAuth link
            raise errors.UrlExpired(rpc_name="messages.RequestUrlAuth")

        return types.OauthLinkInfo._parse(self, r, url)
