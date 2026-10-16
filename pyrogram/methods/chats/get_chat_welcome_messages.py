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
from pyrogram import raw, types


class GetChatWelcomeMessages:
    async def get_chat_welcome_messages(
        self: "pyrogram.Client",
        chat_id: Union[int, str]
    ) -> List["types.WelcomeMessage"]:
        """Get the welcome messages of a chat.

        .. include:: /_includes/usable-by/users.rst

        You must be an administrator of the chat with the *can_send_welcome_messages* privilege.

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

        Returns:
            List of :obj:`~pyrogram.types.WelcomeMessage`: The welcome messages, from the first to the last sent.

        Example:
            .. code-block:: python

                for message in await app.get_chat_welcome_messages(chat_id):
                    print(message.text)
        """
        r = await self.invoke(
            raw.functions.ephemeral.GetWelcomeMessages(peer=await self.resolve_peer(chat_id), hash=0)
        )

        return types.List([
            await types.WelcomeMessage._parse(self, message) for message in getattr(r, "messages", [])
        ])
