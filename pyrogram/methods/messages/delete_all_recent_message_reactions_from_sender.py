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


class DeleteAllRecentMessageReactionsFromSender:
    async def delete_all_recent_message_reactions_from_sender(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        sender_id: Union[int, str]
    ) -> bool:
        """Delete all recent reactions added by a sender in a chat.

        .. note::

            Requires the *can_delete_messages* administrator right in the chat.

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target group or channel.

            sender_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the user or chat whose reactions will be deleted.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                await app.delete_all_recent_message_reactions_from_sender(chat_id, user_id)
        """
        return await self.invoke(
            raw.functions.messages.DeleteParticipantReactions(
                peer=await self.resolve_peer(chat_id),
                participant=await self.resolve_peer(sender_id)
            )
        )
