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
from pyrogram import errors, raw


class DeleteMessageReactionsFromSender:
    async def delete_message_reactions_from_sender(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        sender_id: Union[int, str]
    ) -> bool:
        """Delete all reactions added by a sender to a message.

        .. note::

            Requires the *can_delete_messages* administrator right in the chat.

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target group or channel.

            message_id (``int``):
                Identifier of the message containing the reactions.

            sender_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the user or chat whose reactions will be deleted.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                await app.delete_message_reactions_from_sender(chat_id, message_id, user_id)
        """
        try:
            await self.invoke(
                raw.functions.messages.DeleteParticipantReaction(
                    peer=await self.resolve_peer(chat_id),
                    msg_id=message_id,
                    participant=await self.resolve_peer(sender_id)
                )
            )
        except errors.MessageNotModified:
            # The sender has no reactions on the message: nothing to delete (as TDLib does)
            pass

        return True
