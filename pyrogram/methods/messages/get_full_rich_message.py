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

from typing import Optional, Union

import pyrogram
from pyrogram import raw, types


class GetFullRichMessage:
    async def get_full_rich_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int
    ) -> Optional["types.RichMessage"]:
        """Get the full version of a rich message received partially (see :obj:`~pyrogram.types.RichMessage`
        ``is_full``).

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat the message belongs to.

            message_id (``int``):
                Identifier of the message.

        Returns:
            :obj:`~pyrogram.types.RichMessage`: On success, the full rich message is returned; None if the message
            has no rich message.

        Example:
            .. code-block:: python

                rich_message = await app.get_full_rich_message(chat_id, message_id)
        """
        r = await self.invoke(
            raw.functions.messages.GetRichMessage(
                peer=await self.resolve_peer(chat_id),
                id=message_id
            )
        )

        for message in r.messages:
            if getattr(message, "rich_message", None) is not None:
                return types.RichMessage._parse(self, message.rich_message)

        return None
