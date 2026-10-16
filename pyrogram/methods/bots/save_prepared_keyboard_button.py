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


class SavePreparedKeyboardButton:
    async def save_prepared_keyboard_button(
        self: "pyrogram.Client",
        user_id: Union[int, str],
        button: "types.KeyboardButton"
    ) -> str:
        """Prepare a keyboard button to be used by a user in a Mini App of the bot.

        The Mini App passes the returned identifier to the user's Telegram app, which gets the button with
        :meth:`~pyrogram.Client.get_prepared_keyboard_button`, lets the user choose and shares the choice with the bot,
        as if the button was pressed in a chat with the bot.

        .. include:: /_includes/usable-by/bots.rst

        Parameters:
            user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the user who will use the button.

            button (:obj:`~pyrogram.types.KeyboardButton`):
                The button. It must request users (*request_user*), a chat (*request_chat*)
                or the creation of a managed bot (*request_managed_bot*).

        Returns:
            ``str``: On success, the identifier of the prepared button is returned.

        Example:
            .. code-block:: python

                from pyrogram.types import KeyboardButton, RequestPeerTypeUser

                prepared_button_id = await app.save_prepared_keyboard_button(
                    user_id,
                    KeyboardButton("Choose a friend", request_user=RequestPeerTypeUser(button_id=1, is_bot=False))
                )
        """
        r = await self.invoke(
            raw.functions.bots.RequestWebViewButton(
                user_id=await self.resolve_peer(user_id),
                button=button.write()
            )
        )

        return r.webapp_req_id
