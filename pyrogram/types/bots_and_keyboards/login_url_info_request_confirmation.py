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
from pyrogram import types
from .login_url_info import LoginUrlInfo


class LoginUrlInfoRequestConfirmation(LoginUrlInfo):
    """An authorization confirmation dialog needs to be shown to the user.

    If the user agrees, get the URL to open with :meth:`~pyrogram.Client.get_login_url` (for buttons) or
    :meth:`~pyrogram.Client.get_external_link` (for links); otherwise open *url* as an ordinary link.

    Parameters:
        url (``str``):
            An HTTP URL to be opened.

        domain (``str``):
            A domain of the URL.

        bot (:obj:`~pyrogram.types.User`):
            The bot linked with the website, which will be used for the user authorization.

        request_write_access (``bool``):
            True, if the user must be asked for the permission to the bot to send them messages.

        request_phone_number_access (``bool``):
            True, if the user must be asked for the permission to share their phone number with the bot.
    """

    def __init__(
        self,
        *,
        url: str,
        domain: str,
        bot: "types.User",
        request_write_access: bool,
        request_phone_number_access: bool,
        client: "pyrogram.Client" = None
    ):
        super().__init__(client=client)

        self.url = url
        self.domain = domain
        self.bot = bot
        self.request_write_access = request_write_access
        self.request_phone_number_access = request_phone_number_access
