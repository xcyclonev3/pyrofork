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


class SetChatDraftMessage:
    async def set_chat_draft_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        text: str = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: List["types.MessageEntity"] = None,
        rich_message: "types.InputRichMessage" = None,
        message_thread_id: int = None,
        reply_to_message_id: int = None,
        disable_web_page_preview: bool = None,
        invert_media: bool = None,
        message_effect_id: int = None
    ) -> bool:
        """Change the message draft of a chat or of a forum topic: a text or a rich message.

        Call it without *text* and *rich_message* to delete the draft.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            text (``str``, *optional*):
                Text of the draft.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.

            entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
                List of special entities that appear in the text, which can be specified instead of *parse_mode*.

            rich_message (:obj:`~pyrogram.types.InputRichMessage`, *optional*):
                Rich message of the draft, instead of a text.

            message_thread_id (``int``, *optional*):
                Unique identifier of the forum topic whose draft is changed.

            reply_to_message_id (``int``, *optional*):
                Identifier of the message to be replied.

            disable_web_page_preview (``bool``, *optional*):
                Disables link previews for links in the text.

            invert_media (``bool``, *optional*):
                Shows the link preview above the text.

            message_effect_id (``int`` ``64-bit``, *optional*):
                Unique identifier of the message effect to be added to the message when it is sent.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                from pyrogram import types

                # Save a text draft
                await app.set_chat_draft_message(chat_id, "To be continued...")

                # Save a rich message draft
                await app.set_chat_draft_message(
                    chat_id,
                    rich_message=types.InputRichMessage(types.RichMessageSourceBlocks([
                        types.InputPageBlockSectionHeading("Plan", size=2)
                    ]))
                )

                # Delete the draft
                await app.set_chat_draft_message(chat_id)
        """
        if text and rich_message is not None:
            raise ValueError("Pass either text or rich_message")

        message, entities = (await utils.parse_text_entities(self, text, parse_mode, entities)).values()

        reply_to = await utils.get_reply_to(
            client=self,
            chat_id=chat_id,
            reply_to_message_id=reply_to_message_id,
            message_thread_id=message_thread_id
        )

        return await self.invoke(
            raw.functions.messages.SaveDraft(
                peer=await self.resolve_peer(chat_id),
                message=message or "",
                entities=entities,
                no_webpage=disable_web_page_preview or None,
                invert_media=invert_media or None,
                reply_to=reply_to,
                effect=message_effect_id,
                rich_message=await rich_message.write(self, chat_id) if rich_message is not None else None
            )
        )
