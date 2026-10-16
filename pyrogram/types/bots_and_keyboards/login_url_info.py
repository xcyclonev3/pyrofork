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
from ..object import Object


class LoginUrlInfo(Object):
    """Information about an inline button of type :obj:`~pyrogram.types.LoginUrl` or about an external link,
    returned by :meth:`~pyrogram.Client.get_login_url_info` and :meth:`~pyrogram.Client.get_external_link_info`.

    It should be one of:

    - :obj:`~pyrogram.types.LoginUrlInfoOpen`
    - :obj:`~pyrogram.types.LoginUrlInfoRequestConfirmation`
    """

    def __init__(self, *, client: "pyrogram.Client" = None):
        super().__init__(client)

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        url_auth_result: "raw.base.UrlAuthResult",
        url: str
    ) -> "LoginUrlInfo":
        if isinstance(url_auth_result, raw.types.UrlAuthResultRequest):
            return types.LoginUrlInfoRequestConfirmation(
                url=url,
                domain=url_auth_result.domain,
                bot=types.User._parse(client, url_auth_result.bot),
                request_write_access=url_auth_result.request_write_access,
                request_phone_number_access=url_auth_result.request_phone_number,
                client=client
            )

        if isinstance(url_auth_result, raw.types.UrlAuthResultAccepted):
            # The user is already authorized: the returned URL can be opened without confirmation
            return types.LoginUrlInfoOpen(url=url_auth_result.url, skip_confirmation=True, client=client)

        # urlAuthResultDefault: the URL must be opened as an ordinary link
        return types.LoginUrlInfoOpen(url=url, skip_confirmation=False, client=client)

    @staticmethod
    def _parse_url(url_auth_result: "raw.base.UrlAuthResult", url: str) -> Optional[str]:
        """The URL to open after messages.acceptUrlAuth, as TDLib returns it."""
        if isinstance(url_auth_result, raw.types.UrlAuthResultAccepted):
            # May be absent: then only a toast about the successful login has to be shown
            return url_auth_result.url

        if isinstance(url_auth_result, raw.types.UrlAuthResultDefault):
            return url

        raise ValueError("Unexpected authorization request received while accepting the authorization")
