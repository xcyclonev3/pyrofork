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


class DeletePollOption:
    async def delete_poll_option(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        option: Union[bytes, "types.PollOption"]
    ) -> bool:
        """Delete an answer option that was added to a poll after its creation.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            message_id (``int``):
                Identifier of the message with the poll.

            option (``bytes`` | :obj:`~pyrogram.types.PollOption`):
                The option to delete, or its *data*.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                message = await app.get_messages(chat_id, message_id)
                await app.delete_poll_option(chat_id, message_id, message.poll.options[-1])
        """
        await self.invoke(
            raw.functions.messages.DeletePollAnswer(
                peer=await self.resolve_peer(chat_id),
                msg_id=message_id,
                option=option.data if isinstance(option, types.PollOption) else option
            )
        )

        return True
