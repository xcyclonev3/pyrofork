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


class EditEphemeralMessageCaption:
    async def edit_ephemeral_message_caption(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        receiver_user_id: Union[int, str],
        ephemeral_message_id: int,
        caption: str,
        parse_mode: Optional["enums.ParseMode"] = None,
        caption_entities: List["types.MessageEntity"] = None,
        reply_markup: "types.InlineKeyboardMarkup" = None,
        invert_media: bool = None
    ) -> Optional["types.EphemeralMessage"]:
        """Edit the caption of a media ephemeral message sent by the bot.

        .. include:: /_includes/usable-by/bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            receiver_user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the user who received the message.

            ephemeral_message_id (``int``):
                Identifier of the ephemeral message.

            caption (``str``):
                New caption of the media; pass an empty string to remove it.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.

            caption_entities (List of :obj:`~pyrogram.types.MessageEntity`):
                List of special entities that appear in the caption, which can be specified instead of *parse_mode*.

            reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup`, *optional*):
                The new inline keyboard of the message.

            invert_media (``bool``, *optional*):
                Pass True to show the caption above the media.

        Returns:
            :obj:`~pyrogram.types.EphemeralMessage`: On success, the edited message is returned.

        Example:
            .. code-block:: python

                await app.edit_ephemeral_message_caption(chat_id, user_id, message_id, "New caption")
        """
        parsed = await utils.parse_text_entities(self, caption, parse_mode, caption_entities)

        r = await self.invoke(
            raw.functions.ephemeral.EditMessage(
                peer=await self.resolve_peer(chat_id),
                receiver_id=get_input_user(await self.resolve_peer(receiver_user_id)),
                id=ephemeral_message_id,
                invert_media=invert_media,
                message=parsed["message"],
                entities=parsed["entities"],
                reply_markup=await reply_markup.write(self) if reply_markup else None
            )
        )

        return await utils.parse_ephemeral_message_updates(self, r, raw.types.UpdateEditEphemeralMessage)
