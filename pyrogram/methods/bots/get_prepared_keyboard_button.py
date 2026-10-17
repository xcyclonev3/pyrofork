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


class GetPreparedKeyboardButton:
    async def get_prepared_keyboard_button(
        self: "pyrogram.Client",
        bot_id: Union[int, str],
        prepared_button_id: str
    ) -> "types.KeyboardButton":
        """Get a keyboard button prepared by a bot for the current user in a Mini App.

        The button requests users, a chat or the creation of a managed bot. Share the choice of the user with
        :meth:`~pyrogram.Client.share_users_with_bot` or :meth:`~pyrogram.Client.share_chat_with_bot`, passing the
        same *prepared_button_id*.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            bot_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the bot that prepared the button.

            prepared_button_id (``str``):
                Identifier of the prepared button, as received from the Mini App.

        Returns:
            :obj:`~pyrogram.types.KeyboardButton`: On success, the prepared button is returned.

        Example:
            .. code-block:: python

                button = await app.get_prepared_keyboard_button(bot_id, prepared_button_id)
                print(button.text, button.request_user)
        """
        r = await self.invoke(
            raw.functions.bots.GetRequestedWebViewButton(
                bot=await self.resolve_peer(bot_id),
                webapp_req_id=prepared_button_id
            )
        )

        button = types.KeyboardButton.read(r)

        # Plain text buttons are read as their text
        return types.KeyboardButton(text=button) if isinstance(button, str) else button
