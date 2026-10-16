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
from pyrogram.types.rich_messages.input_rich_message import get_input_user


class EditEphemeralMessage:
    async def edit_ephemeral_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        receiver_user_id: Union[int, str],
        ephemeral_message_id: int,
        text: str = None,
        media: "types.InputMedia" = None,
        rich_message: "types.InputRichMessage" = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: List["types.MessageEntity"] = None,
        reply_markup: "types.InlineKeyboardMarkup" = None,
        invert_media: bool = None
    ) -> Optional["types.EphemeralMessage"]:
        """Edit the text, the media or the reply markup of an ephemeral message sent by the bot.

        .. include:: /_includes/usable-by/bots.rst

        Pass one of *text*, *media* and *rich_message* to change the content, or none of them to change only the
        reply markup.

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            receiver_user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the user who received the message.

            ephemeral_message_id (``int``):
                Identifier of the ephemeral message.

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

            reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup`, *optional*):
                The new inline keyboard of the message.

            invert_media (``bool``, *optional*):
                Pass True to show the caption above the media.

        Returns:
            :obj:`~pyrogram.types.EphemeralMessage`: On success, the edited message is returned.

        Example:
            .. code-block:: python

                await app.edit_ephemeral_message(chat_id, user_id, message_id, "New text")
        """
        content = await utils.get_input_message_content(
            self, chat_id, text, parse_mode, entities, media, rich_message
        )

        r = await self.invoke(
            raw.functions.ephemeral.EditMessage(
                peer=await self.resolve_peer(chat_id),
                receiver_id=get_input_user(await self.resolve_peer(receiver_user_id)),
                id=ephemeral_message_id,
                invert_media=invert_media,
                message=content["message"],
                media=content["media"],
                entities=content["entities"],
                reply_markup=await reply_markup.write(self) if reply_markup else None,
                rich_message=content["rich_message"]
            )
        )

        return await utils.parse_ephemeral_message_updates(self, r, raw.types.UpdateEditEphemeralMessage)
