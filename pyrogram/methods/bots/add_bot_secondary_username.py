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


class AddBotSecondaryUsername:
    async def add_bot_secondary_username(
        self: "pyrogram.Client",
        bot_id: Union[int, str],
        username: str
    ) -> bool:
        """Add a secondary username to a bot owned by the current user.

        The bot must not have a secondary username yet, and the current user must have a Telegram Premium
        subscription. A secondary username doesn't need to end with "bot".

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            bot_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the bot.

            username (``str``):
                The new secondary username. Use :meth:`~pyrogram.Client.check_bot_username` with
                ``is_secondary=True`` to check whether it can be used.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.add_bot_secondary_username(bot_id, "my_shop")
        """
        return await self.invoke(
            raw.functions.bots.AddUsername(
                bot=await self.resolve_peer(bot_id),
                username=username
            )
        )
