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


class EditChatWelcomeMessage:
    async def edit_chat_welcome_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        welcome_message_id: int,
        text: str = None,
        media: "types.InputMedia" = None,
        rich_message: "types.InputRichMessage" = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: List["types.MessageEntity"] = None,
        invert_media: bool = None
    ) -> bool:
        """Edit a welcome message of a chat.

        .. include:: /_includes/usable-by/users.rst

        You must be an administrator of the chat with the *can_send_welcome_messages* privilege.
        Pass exactly one of *text*, *media* and *rich_message*.

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            welcome_message_id (``int``):
                Identifier of the welcome message.

            text (``str``, *optional*):
                Text of the message.

            media (:obj:`~pyrogram.types.InputMedia`, *optional*):
                Media of the message, with its caption: :obj:`~pyrogram.types.InputMediaPhoto`,
                :obj:`~pyrogram.types.InputMediaVideo`, :obj:`~pyrogram.types.InputMediaAnimation`,
                :obj:`~pyrogram.types.InputMediaAudio` or :obj:`~pyrogram.types.InputMediaDocument`.

            rich_message (:obj:`~pyrogram.types.InputRichMessage`, *optional*):
                Rich message to send.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.

            entities (List of :obj:`~pyrogram.types.MessageEntity`):
                List of special entities that appear in the text, which can be specified instead of *parse_mode*.

            invert_media (``bool``, *optional*):
                Pass True to show the caption above the media.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.edit_chat_welcome_message(chat_id, message.id, "Welcome! Read the pinned rules.")
        """
        content = await utils.get_input_message_content(
            self, chat_id, text, parse_mode, entities, media, rich_message
        )

        if content["message"] is None and content["media"] is None and content["rich_message"] is None:
            raise ValueError("Pass one of text, media and rich_message")

        await self.invoke(
            raw.functions.ephemeral.EditMessage(
                peer=await self.resolve_peer(chat_id),
                receiver_id=raw.types.InputUserEmpty(),
                id=welcome_message_id,
                welcome=True,
                invert_media=invert_media,
                message=content["message"] or "",
                media=content["media"],
                entities=content["entities"],
                rich_message=content["rich_message"]
            )
        )

        return True
