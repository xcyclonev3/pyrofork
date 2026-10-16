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


class RemoveAllWebBrowserSettingsExceptions:
    async def remove_all_web_browser_settings_exceptions(
        self: "pyrogram.Client"
    ) -> "types.WebBrowserSettings":
        """Remove the special handling for opening of all websites.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            :obj:`~pyrogram.types.WebBrowserSettings`: On success, the new web browser settings are returned.

        Example:
            .. code-block:: python

                await app.remove_all_web_browser_settings_exceptions()
        """
        r = await self.invoke(
            raw.functions.account.DeleteWebBrowserSettingsExceptions()
        )

        return types.WebBrowserSettings._parse(r)
