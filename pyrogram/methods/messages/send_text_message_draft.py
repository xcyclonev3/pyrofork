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

from typing import List, Optional, Union

import pyrogram
from pyrogram import enums, raw, types, utils


class SendTextMessageDraft:
    async def send_text_message_draft(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        draft_id: int,
        text: Optional[str] = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: Optional[List["types.MessageEntity"]] = None,
        message_thread_id: Optional[int] = None,
        can_stop: Optional[bool] = None,
        keep_on_stop: Optional[bool] = None
    ) -> bool:
        """Send a live draft of a message being generated (live response streaming).

        The draft is shown to the user as the last message of the chat until it expires, the final message is sent
        or a draft with another *draft_id* is sent. Send a new draft with the same *draft_id* to update it.

        .. note::

            Drafts can be sent only to private chats. Like chat actions, they are subject to rate limits: at most 20
            calls in 5 seconds and 40 calls in 30 seconds per chat.

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target private chat.

            draft_id (``int``):
                Unique non-zero identifier of the draft within the message thread.

            text (``str``, *optional*):
                The current text of the message.
                Omit it to show a "Thinking..." placeholder.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.

            entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
                List of special entities that appear in the text, which can be specified instead of *parse_mode*.

            message_thread_id (``int``, *optional*):
                Identifier of the forum topic of the chat with the bot in which the message will be sent.

            can_stop (``bool``, *optional*):
                Pass True to show the user a button to stop further drafts.
                When the user presses it, a :obj:`~pyrogram.types.MessageDraftStopped` update is received.

            keep_on_stop (``bool``, *optional*):
                Pass True to keep the current draft when the user stops further generation.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                await app.send_text_message_draft(chat_id, 1, can_stop=True)
                await app.send_text_message_draft(chat_id, 1, "Hello", can_stop=True)
                await app.send_text_message_draft(chat_id, 1, "Hello, **world**!", can_stop=True)
                await app.send_message(chat_id, "Hello, **world**!")
        """
        message, entities = (await utils.parse_text_entities(self, text or "", parse_mode, entities)).values()

        return await self.invoke(
            raw.functions.messages.SetTyping(
                peer=await self.resolve_peer(chat_id),
                top_msg_id=message_thread_id,
                action=raw.types.SendMessageTextDraftAction(
                    random_id=draft_id,
                    text=raw.types.TextWithEntities(text=message, entities=entities or []),
                    can_stop=can_stop,
                    keep_on_stop=keep_on_stop
                )
            )
        )
