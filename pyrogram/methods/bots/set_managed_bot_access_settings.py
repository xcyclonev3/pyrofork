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

from typing import List, Union

import pyrogram
from pyrogram import raw


class SetManagedBotAccessSettings:
    async def set_managed_bot_access_settings(
        self: "pyrogram.Client",
        bot_id: Union[int, str],
        is_restricted: bool,
        added_user_ids: List[Union[int, str]] = None
    ) -> bool:
        """Change the access settings of a bot managed by the current bot.

        .. include:: /_includes/usable-by/bots.rst

        Parameters:
            bot_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the managed bot.

            is_restricted (``bool``):
                Pass True to restrict access to the bot to its owner and the users in *added_user_ids*.

            added_user_ids (List of ``int`` | ``str``, *optional*):
                Unique identifiers (int) or usernames (str) of the users who can use the bot in addition to its owner,
                up to 10.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                # Only the owner and two other users can use the bot
                await app.set_managed_bot_access_settings(bot_id, True, [user_id1, user_id2])

                # Everyone can use the bot
                await app.set_managed_bot_access_settings(bot_id, False)
        """
        return await self.invoke(
            raw.functions.bots.EditAccessSettings(
                bot=await self.resolve_peer(bot_id),
                restricted=is_restricted or None,
                add_users=[await self.resolve_peer(user_id) for user_id in added_user_ids or []] or None
            )
        )
