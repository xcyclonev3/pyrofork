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
from .get_login_url_info import get_login_button_url


class GetLoginUrl:
    async def get_login_url(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        button_id: int,
        allow_write_access: Optional[bool] = None,
        allow_phone_number_access: Optional[bool] = None
    ) -> Optional[str]:
        """Get an HTTP URL which automatically authorizes the user on a website after pressing an inline button of
        type :obj:`~pyrogram.types.LoginUrl`.

        Use :meth:`~pyrogram.Client.get_login_url_info` first to find out whether a confirmation of the user is
        needed. If an error is raised, the button must be handled as an ordinary URL button.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat with the message.

            message_id (``int``):
                Identifier of the message with the button.

            button_id (``int``):
                Identifier of the button, see :obj:`~pyrogram.types.LoginUrl`.

            allow_write_access (``bool``, *optional*):
                Pass True to allow the bot to send messages to the current user.

            allow_phone_number_access (``bool``, *optional*):
                Pass True to share the phone number of the current user with the bot, if it was requested (see
                *request_phone_number_access* of :obj:`~pyrogram.types.LoginUrlInfoRequestConfirmation`).
                Login buttons can't request the phone number at the moment.

        Returns:
            ``str`` | ``None``: On success, the URL to open is returned, or None if just a notification about the
            successful login has to be shown.

        Raises:
            ValueError: In case the message has no login button with the given identifier.

        Example:
            .. code-block:: python

                url = await app.get_login_url(chat_id, message_id, button_id, allow_write_access=True)
        """
        url = await get_login_button_url(self, chat_id, message_id, button_id)

        r = await self.invoke(
            raw.functions.messages.AcceptUrlAuth(
                peer=await self.resolve_peer(chat_id),
                msg_id=message_id,
                button_id=button_id,
                write_allowed=allow_write_access,
                share_phone_number=allow_phone_number_access
            )
        )

        return types.LoginUrlInfo._parse_url(r, url)
