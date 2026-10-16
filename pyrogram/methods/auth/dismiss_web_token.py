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


class DismissWebToken:
    async def dismiss_web_token(
        self: "pyrogram.Client",
        token: str
    ) -> bool:
        """Dismiss a received web authorization token without using it; for official Telegram apps only.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            token (``str``):
                The web token to dismiss.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                await app.dismiss_web_token(token)
        """
        return await self.invoke(
            raw.functions.auth.CancelWebTokenAuthorization(web_auth_token=token)
        )
