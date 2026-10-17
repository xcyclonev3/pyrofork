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
from pyrogram import raw, types, utils


class AnswerGuestQuery:
    async def answer_guest_query(
        self: "pyrogram.Client",
        guest_query_id: Union[int, str],
        result: "types.InlineQueryResult"
    ) -> str:
        """Answer a guest query: post a message as a guest into the chat in which the bot was mentioned.

        Bots that support guest mode can be mentioned by their username in private chats, groups and supergroups,
        even if they aren't members, and receive a guest query, see :obj:`~pyrogram.types.GuestQuery`.

        .. include:: /_includes/usable-by/bots.rst

        Parameters:
            guest_query_id (``int`` | ``str``):
                Unique identifier of the guest query.

            result (:obj:`~pyrogram.types.InlineQueryResult`):
                The message to post, described as an inline query result.

        Returns:
            ``str``: On success, the identifier of the posted inline message is returned.

        Example:
            .. code-block:: python

                from pyrogram.types import InlineQueryResultArticle, InputTextMessageContent

                inline_message_id = await app.answer_guest_query(
                    guest_query.id,
                    InlineQueryResultArticle("Answer", InputTextMessageContent("Hello from a guest bot!"))
                )
        """
        r = await self.invoke(
            raw.functions.messages.SetBotGuestChatResult(
                query_id=int(guest_query_id),
                result=await result.write(self)
            )
        )

        return utils.pack_inline_message_id(r)
