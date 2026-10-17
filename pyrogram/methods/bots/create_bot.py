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
from pyrogram import raw, types


class CreateBot:
    async def create_bot(
        self: "pyrogram.Client",
        manager_bot_id: Union[int, str],
        name: str,
        username: str,
        via_link: bool = None
    ) -> "types.User":
        """Create a bot managed by another bot.

        The new bot is owned by the current user and controlled by the manager bot, which can then get its token with
        :meth:`~pyrogram.Client.get_managed_bot_token`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            manager_bot_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the bot that will manage the created bot.
                The bot must be allowed to manage other bots.

            name (``str``):
                Name of the bot, 1-64 characters.

            username (``str``):
                Username of the bot. Use :meth:`~pyrogram.Client.check_bot_username` to check whether it can be used.

            via_link (``bool``, *optional*):
                Pass True if the bot is created from a link that requests the creation of a managed bot.

        Returns:
            :obj:`~pyrogram.types.User`: On success, the created bot is returned.

        Example:
            .. code-block:: python

                bot = await app.create_bot("ManagerBot", "My Bot", "my_managed_bot")
        """
        r = await self.invoke(
            raw.functions.bots.CreateBot(
                name=name,
                username=username,
                manager_id=await self.resolve_peer(manager_bot_id),
                via_deeplink=via_link
            )
        )

        # The result is a bare User: cache it, so that the new bot can be used right away
        await self.fetch_peers([r])

        return types.User._parse(self, r)
