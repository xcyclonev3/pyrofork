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


class RequestEphemeralCallbackAnswer:
    async def request_ephemeral_callback_answer(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        ephemeral_message_id: int,
        callback_data: Union[str, bytes],
        timeout: int = 10
    ):
        """Press a callback button of an ephemeral message sent by a bot, and get the answer of the bot.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            ephemeral_message_id (``int``):
                Identifier of the ephemeral message with the button.

            callback_data (``str`` | ``bytes``):
                Callback data of the button.

            timeout (``int``, *optional*):
                Timeout in seconds.

        Returns:
            The answer containing info useful for clients to display a notification at the top of the chat screen
            or as an alert.

        Raises:
            TimeoutError: In case the bot fails to answer within 10 seconds.

        Example:
            .. code-block:: python

                await app.request_ephemeral_callback_answer(chat_id, message_id, "callback_data")
        """
        data = bytes(callback_data, "utf-8") if isinstance(callback_data, str) else callback_data

        return await self.invoke(
            raw.functions.ephemeral.GetCallbackAnswer(
                peer=await self.resolve_peer(chat_id),
                id=ephemeral_message_id,
                data=data
            ),
            retries=0,
            timeout=timeout
        )
