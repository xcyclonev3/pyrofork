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


class CheckOauthRequestMatchCode:
    async def check_oauth_request_match_code(
        self: "pyrogram.Client",
        url: str,
        match_code: str
    ) -> bool:
        """Check the code chosen by the user for an OAuth authorization request.

        Only usable when :obj:`~pyrogram.types.OauthLinkInfo` has *match_code_first* set. If the check fails, the
        authorization request has failed. Otherwise, show the authorization confirmation dialog and process the
        request with :meth:`~pyrogram.Client.accept_oauth_request` (passing the same code) or
        :meth:`~pyrogram.Client.decline_oauth_request`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            url (``str``):
                The OAuth deep link.

            match_code (``str``):
                The code chosen by the user among :obj:`~pyrogram.types.OauthLinkInfo` *match_codes*.

        Returns:
            ``bool``: True, if the code matches the one shown on the login page.

        Example:
            .. code-block:: python

                if await app.check_oauth_request_match_code(link, info.match_codes[0]):
                    print("The code is correct")
        """
        return await self.invoke(
            raw.functions.messages.CheckUrlAuthMatchCode(
                url=url,
                match_code=match_code
            )
        )
