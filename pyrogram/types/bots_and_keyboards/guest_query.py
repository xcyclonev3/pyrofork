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

from typing import Dict, List

import pyrogram
from pyrogram import raw, types

from ..object import Object
from ..update import Update


class GuestQuery(Object, Update):
    """An incoming guest query.

    Bots that support guest mode can be mentioned by their username in private chats, groups and supergroups, even if
    they aren't members: the bot then answers with a message that is posted into the chat as a guest message.

    Parameters:
        id (``str``):
            Unique identifier of the query.

        message (:obj:`~pyrogram.types.Message`):
            The message that mentioned the bot.

        reference_messages (List of :obj:`~pyrogram.types.Message`, *optional*):
            Messages referenced by the message that mentioned the bot, e.g. the replied message.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        id: str,
        message: "types.Message",
        reference_messages: List["types.Message"] = None
    ):
        super().__init__(client)

        self.id = id
        self.message = message
        self.reference_messages = reference_messages

    @staticmethod
    async def _parse(
        client: "pyrogram.Client",
        update: "raw.types.UpdateBotGuestChatQuery",
        users: Dict[int, "raw.types.User"],
        chats: Dict[int, "raw.types.Chat"]
    ) -> "GuestQuery":
        # The bot isn't necessarily a member of the chat: replied messages come as reference messages
        # and are not fetched.
        reference_messages = types.List([
            await types.Message._parse(client, message, users, chats, replies=0)
            for message in update.reference_messages or []
        ])

        return GuestQuery(
            id=str(update.query_id),
            message=await types.Message._parse(client, update.message, users, chats, replies=0),
            reference_messages=reference_messages or None,
            client=client
        )

    async def answer(self, result: "types.InlineQueryResult") -> str:
        """Bound method *answer* of :obj:`~pyrogram.types.GuestQuery`.

        Use as a shortcut for:

        .. code-block:: python

            await client.answer_guest_query(guest_query.id, result)

        Example:
            .. code-block:: python

                from pyrogram.types import InlineQueryResultArticle, InputTextMessageContent

                await guest_query.answer(
                    InlineQueryResultArticle("Answer", InputTextMessageContent("Hello from a guest bot!"))
                )

        Parameters:
            result (:obj:`~pyrogram.types.InlineQueryResult`):
                The message to post, described as an inline query result.

        Returns:
            ``str``: On success, the identifier of the posted inline message is returned.

        Raises:
            RPCError: In case of a Telegram RPC error.
        """
        return await self._client.answer_guest_query(
            guest_query_id=self.id,
            result=result
        )
