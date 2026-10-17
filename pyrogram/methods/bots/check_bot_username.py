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
from pyrogram import errors, raw


class CheckBotUsername:
    async def check_bot_username(
        self: "pyrogram.Client",
        username: str,
        is_secondary: bool = False
    ) -> bool:
        """Check whether a username can be used for a new bot or as a secondary username of an existing bot.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            username (``str``):
                The username to check, without the "@".

            is_secondary (``bool``, *optional*):
                Pass True to check the username as a secondary username of an existing bot
                (see :meth:`~pyrogram.Client.add_bot_secondary_username`);
                pass False to check it for a new bot (see :meth:`~pyrogram.Client.create_bot`).
                Defaults to False.

        Returns:
            ``bool``: True if the username can be used, False if it is occupied or can only be bought on
            fragment.com.

        Raises:
            RPCError: In case the username is invalid, e.g. ``USERNAME_INVALID`` or ``USERNAME_SUFFIX_MISSING``
                (a bot username must end with "bot").

        Example:
            .. code-block:: python

                if await app.check_bot_username("my_managed_bot"):
                    bot = await app.create_bot("ManagerBot", "My Bot", "my_managed_bot")
        """
        try:
            return await self.invoke(
                raw.functions.bots.CheckUsername(
                    username=username,
                    additional=is_secondary or None
                )
            )
        except (errors.UsernameOccupied, errors.UsernamePurchaseAvailable):
            # Like TDLib, which returns them as results of the check
            return False
