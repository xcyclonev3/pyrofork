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


class ProcessChatProtectedContentDisableRequest:
    async def process_chat_protected_content_disable_request(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        request_message_id: int,
        approve: bool
    ) -> bool:
        """Approve or reject a request to disable the content protection of a chat.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            request_message_id (``int``):
                Identifier of the incoming service message with the request
                (see :obj:`~pyrogram.types.ChatHasProtectedContentDisableRequested`).

            approve (``bool``):
                Pass True to approve the request and disable the content protection, False to reject it.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.process_chat_protected_content_disable_request(chat_id, message.id, approve=True)
        """
        await self.invoke(
            raw.functions.messages.ToggleNoForwards(
                peer=await self.resolve_peer(chat_id),
                enabled=not approve,
                request_msg_id=request_message_id
            )
        )

        return True
