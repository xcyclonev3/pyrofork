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
from pyrogram import raw, types, utils


class GetPersonalChatHistory:
    async def get_personal_chat_history(
        self: "pyrogram.Client",
        user_id: Union[int, str],
        limit: int = 20
    ) -> List["types.Message"]:
        """Get the last messages of the personal chat of a user.

        The personal chat is the channel a user shows on their profile page
        (see :meth:`~pyrogram.Client.update_personal_chat`).

        .. include:: /_includes/usable-by/bots.rst

        Parameters:
            user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target user.

            limit (``int``, *optional*):
                The maximum number of messages to be returned; 1-20.
                Defaults to 20.

        Returns:
            List of :obj:`~pyrogram.types.Message`: On success, the list of the latest messages of the personal chat
            is returned.

        Raises:
            ValueError: In case the limit is not positive.

        Example:
            .. code-block:: python

                for message in await app.get_personal_chat_history(user_id):
                    print(message.text)
        """
        if limit <= 0:
            raise ValueError("The limit must be positive.")

        r = await self.invoke(
            raw.functions.messages.GetPersonalChannelHistory(
                user_id=await self.resolve_peer(user_id),
                limit=limit,
                # Like TDLib: return the latest messages
                max_id=(1 << 31) - 1,
                min_id=0,
                hash=0
            )
        )

        return await utils.parse_messages(self, r, replies=0)
