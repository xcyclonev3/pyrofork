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
from urllib.parse import quote, unquote, unquote_plus

import pyrogram
from pyrogram import errors, raw, types


def get_oauth_link(url: str) -> str:
    """The OAuth deep link in the form TDLib sends it: only tg://oauth?token=... and
    tg://resolve?domain=oauth&startapp=... are OAuth links, and other parameters are dropped."""
    link = url.split("#", 1)[0]

    if link[:3].lower() == "tg:":
        link = link[3:]
        link = link[2:] if link.startswith("//") else link
        link = link[1:] if link.startswith("/") else link

        path, _, query = link.partition("?")
        path = unquote(path).split("/")

        while path and not path[-1]:
            path.pop()

        args = {}

        for arg in query.split("&"):
            key, _, value = arg.partition("=")
            key = unquote_plus(key)

            if key and key not in args:
                args[key] = unquote_plus(value)

        if path == ["oauth"] and args.get("token"):
            return f"tg://oauth?token={quote(args['token'], safe='')}"

        if path == ["resolve"] and args.get("domain") == "oauth" and args.get("startapp") and not args.get("post"):
            return f"tg://resolve?domain=oauth&startapp={quote(args['startapp'], safe='')}"

    raise ValueError("Invalid OAuth URL specified")


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
                The whole OAuth deep link: *tg://oauth?token=...* or *tg://resolve?domain=oauth&startapp=...*.

            in_app_origin (``str``, *optional*):
                Origin of the in-app browser or mini app the request was received from, in the format
                *scheme://host* (or *scheme://host:port* for non-default ports).
                Omit it for requests received from outside of the application.

        Returns:
            :obj:`~pyrogram.types.OauthLinkInfo`: On success, information about the authorization request is
            returned.

        Raises:
            ValueError: In case the link is not an OAuth link.
            UrlExpired: In case the authorization request is no longer valid.

        Example:
            .. code-block:: python

                info = await app.get_oauth_link_info(link)
                print(info.domain, info.bot.username, info.match_codes)
        """
        url = get_oauth_link(url)

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
