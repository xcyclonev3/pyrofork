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
from pyrogram import raw


class AddWebBrowserSettingsException:
    async def add_web_browser_settings_exception(
        self: "pyrogram.Client",
        url: str,
        open_external_browser: bool
    ) -> bool:
        """Add a special handling for opening a website.

        There can be at most 100 exceptions in each list of exceptions.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            url (``str``):
                URL of the website. The exception applies to the whole domain of the URL and its subdomains.

            open_external_browser (``bool``):
                Pass True if the website must be opened in an external browser; pass False to open it in the in-app
                browser.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                await app.add_web_browser_settings_exception("https://example.org", open_external_browser=True)
        """
        await self.invoke(
            raw.functions.account.ToggleWebBrowserSettingsException(
                url=url,
                open_external_browser=bool(open_external_browser)
            )
        )

        return True
