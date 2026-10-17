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
from .login_url_info import LoginUrlInfo


class LoginUrlInfoOpen(LoginUrlInfo):
    """An HTTP URL needs to be opened.

    Parameters:
        skip_confirmation (``bool``):
            True, if there is no need to show an ordinary open URL confirmation.

        url (``str``, *optional*):
            The URL to open.
            None, if the user is already logged in and just a notification about the successful login has to be
            shown.
    """

    def __init__(
        self,
        *,
        url: Optional[str] = None,
        skip_confirmation: bool,
        client: "pyrogram.Client" = None
    ):
        super().__init__(client=client)

        self.url = url
        self.skip_confirmation = skip_confirmation
