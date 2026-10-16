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


async def get_login_button_url(
    client: "pyrogram.Client",
    chat_id: Union[int, str],
    message_id: int,
    button_id: int
) -> str:
    """URL of the login button of a message, needed to build the result like TDLib does."""
    message = await client.get_messages(chat_id, message_id, replies=0)
    markup = getattr(message, "reply_markup", None)

    if not isinstance(markup, types.InlineKeyboardMarkup):
        raise ValueError("The message has no inline keyboard.")

    for row in markup.inline_keyboard:
        for button in row:
            login_url = getattr(button, "login_url", None)

            if login_url is not None and login_url.button_id == button_id:
                return login_url.url

    raise ValueError("The message has no login button with the given button_id.")


class GetLoginUrlInfo:
    async def get_login_url_info(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        button_id: int
    ) -> "types.LoginUrlInfo":
        """Get information about an inline button of type :obj:`~pyrogram.types.LoginUrl`.

        Call it when the user presses the button: if an authorization confirmation is needed, ask the user and then
        get the URL to open with :meth:`~pyrogram.Client.get_login_url`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat with the message.

            message_id (``int``):
                Identifier of the message with the button. The message must not be scheduled.

            button_id (``int``):
                Identifier of the button, see :obj:`~pyrogram.types.LoginUrl`.

        Returns:
            :obj:`~pyrogram.types.LoginUrlInfoOpen` | :obj:`~pyrogram.types.LoginUrlInfoRequestConfirmation`: On
            success, what to do with the button is returned.

        Raises:
            ValueError: In case the message has no login button with the given identifier.

        Example:
            .. code-block:: python

                from pyrogram import types

                button = message.reply_markup.inline_keyboard[0][0]
                info = await app.get_login_url_info(message.chat.id, message.id, button.login_url.button_id)

                if isinstance(info, types.LoginUrlInfoRequestConfirmation):
                    print(f"Log in to {info.domain} as {info.bot.username}?")
        """
        url = await get_login_button_url(self, chat_id, message_id, button_id)

        r = await self.invoke(
            raw.functions.messages.RequestUrlAuth(
                peer=await self.resolve_peer(chat_id),
                msg_id=message_id,
                button_id=button_id
            )
        )

        return types.LoginUrlInfo._parse(self, r, url)
