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


class DeleteChatWelcomeMessage:
    async def delete_chat_welcome_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        welcome_message_id: int
    ) -> bool:
        """Delete a welcome message of a chat.

        .. include:: /_includes/usable-by/users.rst

        You must be an administrator of the chat with the *can_send_welcome_messages* privilege.

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            welcome_message_id (``int``):
                Identifier of the welcome message.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.delete_chat_welcome_message(chat_id, message.id)
        """
        return await self.invoke(
            raw.functions.ephemeral.DeleteWelcomeMessage(peer=await self.resolve_peer(chat_id), id=welcome_message_id)
        )
