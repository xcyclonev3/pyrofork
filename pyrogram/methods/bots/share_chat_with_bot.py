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

from .share_users_with_bot import send_bot_requested_peers


class ShareChatWithBot:
    async def share_chat_with_bot(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        button_id: int,
        shared_chat_id: Union[int, str],
        message_id: int = None,
        prepared_button_id: str = None
    ) -> bool:
        """Share a chat with a bot after pressing a button that requests a chat.

        The button is either in a message of the bot (pass *message_id*) or prepared by the bot for a Mini App
        (pass *prepared_button_id*, see :meth:`~pyrogram.Client.get_prepared_keyboard_button`).

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the bot.

            button_id (``int``):
                Identifier of the button, as found in its *request_chat* field.

            shared_chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat to share.

            message_id (``int``, *optional*):
                Identifier of the message with the button.

            prepared_button_id (``str``, *optional*):
                Identifier of the button prepared for a Mini App.

        Returns:
            ``bool``: True on success.

        Raises:
            ValueError: In case neither or both of *message_id* and *prepared_button_id* are passed.

        Example:
            .. code-block:: python

                await app.share_chat_with_bot(bot_id, 2, chat_id, message_id=123)
        """
        return await send_bot_requested_peers(
            self, chat_id, button_id, [shared_chat_id], message_id, prepared_button_id
        )
