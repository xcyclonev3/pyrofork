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


class SendEphemeralMessage:
    async def send_ephemeral_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        receiver_user_id: Union[int, str],
        text: str = None,
        media: "types.InputMedia" = None,
        rich_message: "types.InputRichMessage" = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: List["types.MessageEntity"] = None,
        reply_markup: Union[
            "types.InlineKeyboardMarkup",
            "types.ReplyKeyboardMarkup",
            "types.ReplyKeyboardRemove",
            "types.ForceReply"
        ] = None,
        reply_to_message_id: int = None,
        reply_to_ephemeral_message_id: int = None,
        message_thread_id: int = None,
        callback_query_id: Union[int, str] = None,
        replace_callback_query_message: bool = None,
        protect_content: bool = None,
        invert_media: bool = None
    ) -> Optional["types.EphemeralMessage"]:
        """Send an ephemeral message: a message in a chat that only its receiver can see.

        .. include:: /_includes/usable-by/users-bots.rst

        Bots can send ephemeral messages of any kind to the users of a chat. Users can only send ephemeral bot commands
        and replies to ephemeral messages of bots, which only the bot receives.
        Pass exactly one of *text*, *media* and *rich_message*.

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            receiver_user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the only user who will see the message.

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

            reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup` | :obj:`~pyrogram.types.ReplyKeyboardMarkup` | :obj:`~pyrogram.types.ReplyKeyboardRemove` | :obj:`~pyrogram.types.ForceReply`, *optional*):
                Additional interface options. For bots only.

            reply_to_message_id (``int``, *optional*):
                If the message is a reply, ID of the original message.

            reply_to_ephemeral_message_id (``int``, *optional*):
                If the message is a reply to an ephemeral message, ID of that message.

            message_thread_id (``int``, *optional*):
                Unique identifier of the forum topic of the message.

            callback_query_id (``int`` | ``str``, *optional*):
                Identifier of the callback query that triggered the message. For bots only.

            replace_callback_query_message (``bool``, *optional*):
                Pass True to show the message instead of the message of the callback query. For bots only.

            protect_content (``bool``, *optional*):
                Protects the contents of the sent message from forwarding and saving. For bots only.

            invert_media (``bool``, *optional*):
                Pass True to show the caption above the media.

        Returns:
            :obj:`~pyrogram.types.EphemeralMessage`: On success, the sent message is returned.

        Example:
            .. code-block:: python

                await app.send_ephemeral_message(chat_id, user_id, "Only you can see this")
        """
        content = await utils.get_input_message_content(
            self, chat_id, text, parse_mode, entities, media, rich_message
        )

        if content["message"] is None and content["media"] is None and content["rich_message"] is None:
            raise ValueError("Pass one of text, media and rich_message")

        if reply_to_ephemeral_message_id:
            reply_to = raw.types.InputReplyToEphemeralMessage(id=reply_to_ephemeral_message_id)
        else:
            reply_to = await utils.get_reply_to(
                self, chat_id, reply_to_message_id=reply_to_message_id, message_thread_id=message_thread_id
            )

        r = await self.invoke(
            raw.functions.ephemeral.SendMessage(
                peer=await self.resolve_peer(chat_id),
                receiver_id=get_input_user(await self.resolve_peer(receiver_user_id)),
                message=content["message"] or "",
                random_id=self.rnd_id(),
                invert_media=invert_media,
                anchor=replace_callback_query_message,
                noforwards=protect_content,
                query_id=int(callback_query_id) if callback_query_id is not None else None,
                entities=content["entities"],
                media=content["media"],
                reply_markup=await reply_markup.write(self) if reply_markup else None,
                rich_message=content["rich_message"],
                reply_to=reply_to
            )
        )

        return await utils.parse_ephemeral_message_updates(self, r, raw.types.UpdateNewEphemeralMessage)
