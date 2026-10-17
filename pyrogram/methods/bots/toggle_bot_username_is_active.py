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

from typing import Union

import pyrogram
from pyrogram import raw


class ToggleBotUsernameIsActive:
    async def toggle_bot_username_is_active(
        self: "pyrogram.Client",
        bot_id: Union[int, str],
        username: str,
        is_active: bool
    ) -> bool:
        """Activate or disable a username of a bot owned by the current user.

        The editable username can be disabled only if the bot has other active usernames.
        The secondary username can't be toggled, see :meth:`~pyrogram.Client.delete_bot_secondary_username`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            bot_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the bot.

            username (``str``):
                The username to change.

            is_active (``bool``):
                Pass True to activate the username, False to disable it.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.toggle_bot_username_is_active(bot_id, "my_collectible_name", True)
        """
        return await self.invoke(
            raw.functions.bots.ToggleUsername(
                bot=await self.resolve_peer(bot_id),
                username=username,
                active=is_active
            )
        )
