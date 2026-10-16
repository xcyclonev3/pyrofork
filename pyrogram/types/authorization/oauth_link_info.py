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

from typing import List, Optional

import pyrogram
from pyrogram import raw, types
from ..object import Object


class OauthLinkInfo(Object):
    """Information about an OAuth authorization request (login with Telegram on a website or in an app),
    returned by :meth:`~pyrogram.Client.get_oauth_link_info`.

    Show it to the user, then process the request with :meth:`~pyrogram.Client.check_oauth_request_match_code`,
    :meth:`~pyrogram.Client.accept_oauth_request` or :meth:`~pyrogram.Client.decline_oauth_request`.

    Parameters:
        url (``str``):
            The OAuth link the information is about.

        domain (``str``):
            The domain of the website on which the user authorizes.
            If the request originates from an application, *verified_app_name* (or "Unverified App" when it is
            absent) must be shown instead.

        bot (:obj:`~pyrogram.types.User`):
            The bot linked with the website.

        user_id (``int``, *optional*):
            Identifier of the user for which the request was created, if known.
            If the user is logged in the application, they must be chosen for the authorization by default.

        is_from_app (``bool``, *optional*):
            True, if the authorization request originates from an application rather than a website.

        verified_app_name (``str``, *optional*):
            Verified name of the application; if absent, "Unverified App" must be shown instead.

        request_write_access (``bool``, *optional*):
            True, if the user must be asked for the permission to the bot to send them messages.

        request_phone_number_access (``bool``, *optional*):
            True, if the user must be asked for the permission to share their phone number.

        browser (``str``, *optional*):
            The browser used for the authorization.

        platform (``str``, *optional*):
            The operating system the browser is running on.

        ip_address (``str``, *optional*):
            The IP address from which the authorization is performed, in human-readable format.

        location (``str``, *optional*):
            Human-readable description of the country and the region from which the authorization is performed,
            based on the IP address.

        match_code_first (``bool``, *optional*):
            True, if the code matching dialog must be shown first and
            :meth:`~pyrogram.Client.check_oauth_request_match_code` must be called before
            :meth:`~pyrogram.Client.accept_oauth_request`.
            Otherwise, :meth:`~pyrogram.Client.check_oauth_request_match_code` must not be called.

        match_codes (List of ``str``, *optional*):
            The codes (usually emoji) to choose from: one of them is shown on the login page and the user must select
            the same one.
    """

    def __init__(
        self,
        *,
        url: str,
        domain: str,
        bot: "types.User",
        user_id: Optional[int] = None,
        is_from_app: Optional[bool] = None,
        verified_app_name: Optional[str] = None,
        request_write_access: Optional[bool] = None,
        request_phone_number_access: Optional[bool] = None,
        browser: Optional[str] = None,
        platform: Optional[str] = None,
        ip_address: Optional[str] = None,
        location: Optional[str] = None,
        match_code_first: Optional[bool] = None,
        match_codes: Optional[List[str]] = None,
        client: "pyrogram.Client" = None
    ):
        super().__init__(client)

        self.url = url
        self.domain = domain
        self.bot = bot
        self.user_id = user_id
        self.is_from_app = is_from_app
        self.verified_app_name = verified_app_name
        self.request_write_access = request_write_access
        self.request_phone_number_access = request_phone_number_access
        self.browser = browser
        self.platform = platform
        self.ip_address = ip_address
        self.location = location
        self.match_code_first = match_code_first
        self.match_codes = match_codes

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        request: "raw.types.UrlAuthResultRequest",
        url: str
    ) -> "OauthLinkInfo":
        return OauthLinkInfo(
            url=url,
            domain=request.domain,
            bot=types.User._parse(client, request.bot),
            user_id=request.user_id_hint or None,
            is_from_app=request.is_app,
            verified_app_name=request.verified_app_name,
            request_write_access=request.request_write_access,
            request_phone_number_access=request.request_phone_number,
            browser=request.browser,
            platform=request.platform,
            ip_address=request.ip,
            location=request.region,
            match_code_first=request.match_codes_first,
            match_codes=request.match_codes or None,
            client=client
        )
