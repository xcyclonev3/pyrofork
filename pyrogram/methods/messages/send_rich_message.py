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

from datetime import datetime
from typing import List, Optional, Union

import pyrogram
from pyrogram import enums, raw, types, utils
from pyrogram.errors import RPCError


class SendRichMessage:
    async def send_rich_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        rich_message: "types.InputRichMessage",
        disable_notification: bool = None,
        message_thread_id: int = None,
        business_connection_id: str = None,
        reply_to_message_id: int = None,
        reply_to_story_id: int = None,
        reply_to_chat_id: Union[int, str] = None,
        reply_to_monoforum_id: Union[int, str] = None,
        quote_text: str = None,
        quote_entities: List["types.MessageEntity"] = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        schedule_date: datetime = None,
        protect_content: bool = None,
        allow_paid_broadcast: bool = None,
        message_effect_id: int = None,
        clear_draft: bool = None,
        reply_markup: Union[
            "types.InlineKeyboardMarkup",
            "types.ReplyKeyboardMarkup",
            "types.ReplyKeyboardRemove",
            "types.ForceReply"
        ] = None
    ) -> "types.Message":
        """Send a rich message: a message with headings, lists, tables, quotes, media, formulas, buttons and more.

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).
                You can also use chat public link in form of *t.me/<username>* (str).

            rich_message (:obj:`~pyrogram.types.InputRichMessage`):
                The rich message to send. Its media are uploaded first.

            disable_notification (``bool``, *optional*):
                Sends the message silently.
                Users will receive a notification with no sound.

            message_thread_id (``int``, *optional*):
                Unique identifier for the target message thread (topic) of the forum.
                for forum supergroups only.

            business_connection_id (``str``, *optional*):
                Business connection identifier.
                for business bots only.

            reply_to_message_id (``int``, *optional*):
                If the message is a reply, ID of the original message.

            reply_to_story_id (``int``, *optional*):
                Unique identifier for the target story.

            reply_to_chat_id (``int`` | ``str``, *optional*):
                Unique identifier for the origin chat.
                for reply to message from another chat.
                You can also use chat public link in form of *t.me/<username>* (str).

            reply_to_monoforum_id (``int`` | ``str``, *optional*):
                Unique identifier for the target user of the monoforum.
                for reply to message from a monoforum.
                for channel administrators only.

            quote_text (``str``, *optional*):
                Text to quote.
                for reply_to_message only.

            quote_entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
                List of special entities that appear in quote_text, which can be specified instead of *parse_mode*.
                for reply_to_message only.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, quote_text is parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.

            schedule_date (:py:obj:`~datetime.datetime`, *optional*):
                Date when the message will be automatically sent.

            protect_content (``bool``, *optional*):
                Protects the contents of the sent message from forwarding and saving.

            allow_paid_broadcast (``bool``, *optional*):
                Pass True to allow the message to ignore regular broadcast limits for a small fee; for bots only.

            message_effect_id (``int`` ``64-bit``, *optional*):
                Unique identifier of the message effect to be added to the message; for private chats only.

            clear_draft (``bool``, *optional*):
                Pass True to delete the message draft in the chat.

            reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup` | :obj:`~pyrogram.types.ReplyKeyboardMarkup` | :obj:`~pyrogram.types.ReplyKeyboardRemove` | :obj:`~pyrogram.types.ForceReply`, *optional*):
                Additional interface options. An object for an inline keyboard, custom reply keyboard,
                instructions to remove reply keyboard or to force a reply from the user.

        Returns:
            :obj:`~pyrogram.types.Message`: On success, the sent message is returned.

        Example:
            .. code-block:: python

                from pyrogram import types

                await app.send_rich_message(
                    chat_id,
                    types.InputRichMessage(types.RichMessageSourceBlocks([
                        types.InputPageBlockSectionHeading("Shopping list", size=2),
                        types.InputPageBlockList([
                            types.InputPageBlockListItem([types.InputPageBlockParagraph("Milk")], has_checkbox=True),
                            types.InputPageBlockListItem([types.InputPageBlockParagraph("Eggs")], has_checkbox=True,
                                                         is_checked=True),
                        ]),
                        types.InputPageBlockMathematicalExpression("e^{i\\\\pi} + 1 = 0"),
                    ]))
                )

                # Bots can let Telegram parse HTML or Markdown
                await app.send_rich_message(
                    chat_id,
                    types.InputRichMessage(types.RichMessageSourceHtml(
                        "<h1>Report</h1><p>See the chart</p>",
                        media=[types.InputRichMessageMedia("chart", types.InputMediaPhoto("chart.png"))]
                    ))
                )
        """
        reply_to = await utils.get_reply_to(
            client=self,
            chat_id=chat_id,
            reply_to_message_id=reply_to_message_id,
            reply_to_story_id=reply_to_story_id,
            message_thread_id=message_thread_id,
            reply_to_chat_id=reply_to_chat_id,
            reply_to_monoforum_id=reply_to_monoforum_id,
            quote_text=quote_text,
            quote_entities=quote_entities,
            parse_mode=parse_mode
        )

        rpc = raw.functions.messages.SendMessage(
            peer=await self.resolve_peer(chat_id),
            message="",
            random_id=self.rnd_id(),
            silent=disable_notification or None,
            clear_draft=clear_draft or None,
            reply_to=reply_to,
            schedule_date=utils.datetime_to_timestamp(schedule_date),
            reply_markup=await reply_markup.write(self) if reply_markup else None,
            noforwards=protect_content,
            allow_paid_floodskip=allow_paid_broadcast,
            effect=message_effect_id,
            rich_message=await rich_message.write(self, chat_id, business_connection_id)
        )

        if business_connection_id is not None:
            r = await self.invoke(
                raw.functions.InvokeWithBusinessConnection(
                    connection_id=business_connection_id,
                    query=rpc
                )
            )
        else:
            r = await self.invoke(rpc)

        if isinstance(r, raw.types.UpdateShortSentMessage):
            # Only the id comes back: get the message for its rich message, unless it can't be fetched
            if business_connection_id is None and schedule_date is None:
                try:
                    return await self.get_messages(chat_id, r.id)
                except RPCError:
                    pass

            peer = await self.resolve_peer(chat_id)

            if isinstance(peer, raw.types.InputPeerSelf):
                peer_id = self.me.id if self.me else None
            else:
                peer_id = utils.get_peer_id(peer)

            return types.Message(
                id=r.id,
                chat=types.Chat(
                    id=peer_id,
                    type=enums.ChatType.GROUP if peer_id and peer_id < 0 else enums.ChatType.PRIVATE,
                    client=self
                ),
                date=utils.timestamp_to_datetime(r.date),
                outgoing=r.out,
                reply_markup=reply_markup,
                client=self
            )

        for i in r.updates:
            if isinstance(i, (raw.types.UpdateNewMessage,
                              raw.types.UpdateNewChannelMessage,
                              raw.types.UpdateNewScheduledMessage,
                              raw.types.UpdateBotNewBusinessMessage)):
                return await types.Message._parse(
                    self, i.message,
                    {i.id: i for i in r.users},
                    {i.id: i for i in r.chats},
                    is_scheduled=isinstance(i, raw.types.UpdateNewScheduledMessage),
                    business_connection_id=business_connection_id
                )
