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


class ConfirmBusinessConnectedBot:
    async def confirm_business_connected_bot(
        self: "pyrogram.Client",
        bot_id: Union[int, str]
    ) -> bool:
        """Confirm an unconfirmed connection of a business bot to the current user's account.

        A business bot connected from another device must be confirmed, see :obj:`~pyrogram.types.BusinessConnectedBot`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            bot_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the connected bot.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.confirm_business_connected_bot(bot_id)
        """
        return await self.invoke(
            raw.functions.account.ConfirmBotConnection(
                bot_id=await self.resolve_peer(bot_id)
            )
        )
