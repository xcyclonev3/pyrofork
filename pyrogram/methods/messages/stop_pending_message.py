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
from pyrogram import raw


class StopPendingMessage:
    async def stop_pending_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        draft_id: int,
        message_thread_id: Optional[int] = None
    ) -> bool:
        """Stop the generation of a pending message (a live draft) by a bot.

        Use it for a :obj:`~pyrogram.types.PendingMessage` with *can_stop* set: the bot receives a
        :obj:`~pyrogram.types.MessageDraftStopped` update.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat with the bot.

            draft_id (``int``):
                Identifier of the message draft within the message thread.

            message_thread_id (``int``, *optional*):
                Identifier of the forum topic of the message draft.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                await app.stop_pending_message(pending_message.chat.id, pending_message.draft_id)
        """
        return await self.invoke(
            raw.functions.messages.SetTyping(
                peer=await self.resolve_peer(chat_id),
                top_msg_id=message_thread_id,
                action=raw.types.SendMessageStopDraftAction(random_id=draft_id)
            )
        )
