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

from pyrogram import raw, types
from ..object import Object


class WebBrowserSettings(Object):
    """Settings of the web browser used to open links.

    Parameters:
        open_external_browser (``bool``):
            True, if links are opened in an external browser by default.

        display_close_button (``bool``):
            True, if a close button must be shown in the in-app browser; for the Android app only.

        external_exceptions (List of :obj:`~pyrogram.types.WebDomainException`):
            Websites which must always be opened in an external browser.

        in_app_exceptions (List of :obj:`~pyrogram.types.WebDomainException`):
            Websites which must always be opened in the in-app browser.
    """

    def __init__(
        self,
        *,
        open_external_browser: bool,
        display_close_button: bool,
        external_exceptions: List["types.WebDomainException"],
        in_app_exceptions: List["types.WebDomainException"]
    ):
        super().__init__()

        self.open_external_browser = open_external_browser
        self.display_close_button = display_close_button
        self.external_exceptions = external_exceptions
        self.in_app_exceptions = in_app_exceptions

    @staticmethod
    def _parse(settings: "raw.base.account.WebBrowserSettings") -> Optional["WebBrowserSettings"]:
        if not isinstance(settings, raw.types.account.WebBrowserSettings):
            # account.webBrowserSettingsNotModified, received only when a non-zero hash is passed
            return None

        return WebBrowserSettings(
            open_external_browser=bool(settings.open_external_browser),
            display_close_button=bool(settings.display_close_button),
            external_exceptions=types.List(
                types.WebDomainException._parse(exception) for exception in settings.external_exceptions
            ),
            in_app_exceptions=types.List(
                types.WebDomainException._parse(exception) for exception in settings.inapp_exceptions
            )
        )
