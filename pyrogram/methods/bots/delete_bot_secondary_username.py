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


class DeleteBotSecondaryUsername:
    async def delete_bot_secondary_username(
        self: "pyrogram.Client",
        bot_id: Union[int, str]
    ) -> bool:
        """Remove the secondary username from a bot owned by the current user.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            bot_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the bot.

        Returns:
            ``bool``: True on success, also if the bot has no secondary username.

        Example:
            .. code-block:: python

                await app.delete_bot_secondary_username(bot_id)
        """
        peer = await self.resolve_peer(bot_id)

        # As in TDLib, the secondary username is the one Telegram marks as deletable
        users = await self.invoke(raw.functions.users.GetUsers(id=[peer]))
        bot = users[0] if users else None
        deletable = [u.username for u in getattr(bot, "usernames", None) or [] if u.deletable]

        if not deletable:
            return True

        return await self.invoke(
            raw.functions.bots.RemoveUsername(
                bot=peer,
                username=deletable[0]
            )
        )
