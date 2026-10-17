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

from pyrogram import raw
from ..object import Object


class WebDomainException(Object):
    """An exception of the web browser settings: a website which is always opened in the external browser or always
    in the in-app browser.

    Parameters:
        url (``str``):
            URL for which the exception was added.

        domain (``str``):
            Domain of the URL. All URLs on the domain and its subdomains are subject to the exception.

        title (``str``):
            Title of the website.

        favicon_custom_emoji_id (``int``, *optional*):
            Identifier of the custom emoji with the favicon of the website, if known; otherwise the first letter of
            the domain must be shown.
    """

    def __init__(
        self,
        *,
        url: str,
        domain: str,
        title: str,
        favicon_custom_emoji_id: Optional[int] = None
    ):
        super().__init__()

        self.url = url
        self.domain = domain
        self.title = title
        self.favicon_custom_emoji_id = favicon_custom_emoji_id

    @staticmethod
    def _parse(exception: "raw.types.WebDomainException") -> "WebDomainException":
        return WebDomainException(
            url=exception.url,
            domain=exception.domain,
            title=exception.title,
            favicon_custom_emoji_id=exception.favicon
        )
