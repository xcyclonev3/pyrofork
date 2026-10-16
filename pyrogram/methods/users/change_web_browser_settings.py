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
from pyrogram import raw, types


class ChangeWebBrowserSettings:
    async def change_web_browser_settings(
        self: "pyrogram.Client",
        open_external_browser: bool,
        display_close_button: bool
    ) -> "types.WebBrowserSettings":
        """Change the settings of the web browser used to open links.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            open_external_browser (``bool``):
                Pass True if links must be opened in an external browser by default.

            display_close_button (``bool``):
                Pass True if a close button must be shown in the in-app browser; for the Android app only.

        Returns:
            :obj:`~pyrogram.types.WebBrowserSettings`: On success, the new web browser settings are returned.

        Example:
            .. code-block:: python

                await app.change_web_browser_settings(open_external_browser=True, display_close_button=False)
        """
        r = await self.invoke(
            raw.functions.account.UpdateWebBrowserSettings(
                open_external_browser=open_external_browser,
                display_close_button=display_close_button
            )
        )

        return types.WebBrowserSettings._parse(r)
