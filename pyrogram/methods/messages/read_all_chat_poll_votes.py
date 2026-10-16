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


class ReadAllChatPollVotes:
    async def read_all_chat_poll_votes(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_thread_id: int = None
    ) -> bool:
        """Mark all the unseen poll votes of a chat as read.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            message_thread_id (``int``, *optional*):
                Unique identifier of a forum topic, to mark only the votes of that topic as read.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.read_all_chat_poll_votes(chat_id)
        """
        peer = await self.resolve_peer(chat_id)

        while True:
            r = await self.invoke(
                raw.functions.messages.ReadPollVotes(
                    peer=peer,
                    top_msg_id=message_thread_id
                )
            )

            # Like messages.readMentions, repeat while the server reports more history to process
            if not r.offset:
                return True
