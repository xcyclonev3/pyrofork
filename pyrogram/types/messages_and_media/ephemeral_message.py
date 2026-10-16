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
from typing import Dict, List, Optional, Union

import pyrogram
from pyrogram import enums, raw, types, utils
from ..object import Object
from ..update import Update


class EphemeralMessage(Object, Update):
    """An ephemeral message: a message in a chat that only one user, its receiver, can see.

    Bots send ephemeral messages to the users of a chat with :meth:`~pyrogram.Client.send_ephemeral_message`; users
    send ephemeral bot commands and replies to ephemeral messages to bots. Ephemeral messages are kept for 7 days.

    Parameters:
        id (``int``):
            Identifier of the ephemeral message, unique in the chat.

        chat (:obj:`~pyrogram.types.Chat`):
            The chat of the message.

        receiver (:obj:`~pyrogram.types.User`, *optional*):
            The only user who can see the message.

        date (:py:obj:`~datetime.datetime`, *optional*):
            Date the message was sent.

        from_user (:obj:`~pyrogram.types.User`, *optional*):
            Sender of the message.

        sender_chat (:obj:`~pyrogram.types.Chat`, *optional*):
            Sender of the message, if it was sent on behalf of a chat.

        message_thread_id (``int``, *optional*):
            Identifier of the forum topic of the message.

        reply_to_message_id (``int``, *optional*):
            Identifier of the replied message.

        reply_to_ephemeral_message_id (``int``, *optional*):
            Identifier of the replied ephemeral message.

        anchor_message_id (``int``, *optional*):
            For users, identifier of the message of the chat whose content must be replaced by the content of the
            ephemeral message, e.g. when a bot answered a callback query with an ephemeral message.

        chat_instance (``str``, *optional*):
            For bots, global identifier that uniquely corresponds to the chat of the message.

        outgoing (``bool``, *optional*):
            True, if the message was sent by the current user or bot.

        has_protected_content (``bool``, *optional*):
            True, if the message can't be forwarded or saved.

        text (``str``, *optional*):
            For text messages, the text of the message.

        entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
            For text messages, special entities like usernames, URLs, bot commands, etc. that appear in the text.

        caption (``str``, *optional*):
            Caption of the media of the message.

        caption_entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
            Special entities like usernames, URLs, bot commands, etc. that appear in the caption.

        media (:obj:`~pyrogram.enums.MessageMediaType`, *optional*):
            The type of the media of the message, if any.

        has_media_spoiler (``bool``, *optional*):
            True, if the media is covered by a spoiler animation.

        invert_media (``bool``, *optional*):
            True, if the caption is shown above the media.

        photo (:obj:`~pyrogram.types.Photo`, *optional*):
            The message is a photo.

        video (:obj:`~pyrogram.types.Video`, *optional*):
            The message is a video.

        audio (:obj:`~pyrogram.types.Audio`, *optional*):
            The message is an audio file.

        document (:obj:`~pyrogram.types.Document`, *optional*):
            The message is a general file.

        animation (:obj:`~pyrogram.types.Animation`, *optional*):
            The message is an animation.

        sticker (:obj:`~pyrogram.types.Sticker`, *optional*):
            The message is a sticker.

        voice (:obj:`~pyrogram.types.Voice`, *optional*):
            The message is a voice message.

        video_note (:obj:`~pyrogram.types.VideoNote`, *optional*):
            The message is a video note.

        location (:obj:`~pyrogram.types.Location`, *optional*):
            The message is a location.

        venue (:obj:`~pyrogram.types.Venue`, *optional*):
            The message is a venue.

        contact (:obj:`~pyrogram.types.Contact`, *optional*):
            The message is a contact.

        web_page_preview (:obj:`~pyrogram.types.WebPagePreview`, *optional*):
            Link preview of the text.

        rich_message (:obj:`~pyrogram.types.RichMessage`, *optional*):
            The message is a rich message.

        reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup` | :obj:`~pyrogram.types.ReplyKeyboardMarkup` | :obj:`~pyrogram.types.ReplyKeyboardRemove` | :obj:`~pyrogram.types.ForceReply`, *optional*):
            Additional interface options of the message.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        id: int,
        chat: "types.Chat",
        receiver: "types.User" = None,
        date: datetime = None,
        from_user: "types.User" = None,
        sender_chat: "types.Chat" = None,
        message_thread_id: int = None,
        reply_to_message_id: int = None,
        reply_to_ephemeral_message_id: int = None,
        anchor_message_id: int = None,
        chat_instance: str = None,
        outgoing: bool = None,
        has_protected_content: bool = None,
        text: str = None,
        entities: List["types.MessageEntity"] = None,
        caption: str = None,
        caption_entities: List["types.MessageEntity"] = None,
        media: "enums.MessageMediaType" = None,
        has_media_spoiler: bool = None,
        invert_media: bool = None,
        photo: "types.Photo" = None,
        video: "types.Video" = None,
        audio: "types.Audio" = None,
        document: "types.Document" = None,
        animation: "types.Animation" = None,
        sticker: "types.Sticker" = None,
        voice: "types.Voice" = None,
        video_note: "types.VideoNote" = None,
        location: "types.Location" = None,
        venue: "types.Venue" = None,
        contact: "types.Contact" = None,
        web_page_preview: "types.WebPagePreview" = None,
        rich_message: "types.RichMessage" = None,
        reply_markup: Union[
            "types.InlineKeyboardMarkup",
            "types.ReplyKeyboardMarkup",
            "types.ReplyKeyboardRemove",
            "types.ForceReply"
        ] = None
    ):
        super().__init__(client)

        self.id = id
        self.chat = chat
        self.receiver = receiver
        self.date = date
        self.from_user = from_user
        self.sender_chat = sender_chat
        self.message_thread_id = message_thread_id
        self.reply_to_message_id = reply_to_message_id
        self.reply_to_ephemeral_message_id = reply_to_ephemeral_message_id
        self.anchor_message_id = anchor_message_id
        self.chat_instance = chat_instance
        self.outgoing = outgoing
        self.has_protected_content = has_protected_content
        self.text = text
        self.entities = entities
        self.caption = caption
        self.caption_entities = caption_entities
        self.media = media
        self.has_media_spoiler = has_media_spoiler
        self.invert_media = invert_media
        self.photo = photo
        self.video = video
        self.audio = audio
        self.document = document
        self.animation = animation
        self.sticker = sticker
        self.voice = voice
        self.video_note = video_note
        self.location = location
        self.venue = venue
        self.contact = contact
        self.web_page_preview = web_page_preview
        self.rich_message = rich_message
        self.reply_markup = reply_markup

    @staticmethod
    def _parse_chat(
        client,
        peer: "raw.base.Peer",
        users: Dict[int, "raw.base.User"],
        chats: Dict[int, "raw.base.Chat"]
    ) -> "types.Chat":
        chat = types.Chat._parse_peer(client, peer, users, chats)

        if chat is not None:
            return chat

        # The chat should always come along with the message; keep at least its identifier
        if isinstance(peer, raw.types.PeerUser):
            chat_type = enums.ChatType.PRIVATE
        elif isinstance(peer, raw.types.PeerChat):
            chat_type = enums.ChatType.GROUP
        else:
            chat_type = enums.ChatType.SUPERGROUP

        return types.Chat(id=utils.get_peer_id(peer), type=chat_type, client=client)

    @staticmethod
    async def _parse(
        client,
        message: "raw.types.EphemeralMessage",
        users: Dict[int, "raw.base.User"],
        chats: Dict[int, "raw.base.Chat"]
    ) -> Optional["EphemeralMessage"]:
        # Like TDLib, ignore the templates of welcome messages and messages without a chat
        if message.welcome_template or message.peer_id is None:
            return None

        content = await utils.parse_message_content(
            client, message.message, message.entities, message.media, message.rich_message, message.reply_markup,
            message.invert_media, users
        )

        from_user = None
        sender_chat = None

        if isinstance(message.from_id, raw.types.PeerUser):
            from_user = types.User._parse(client, users.get(message.from_id.user_id))
        elif message.from_id is not None:
            sender_chat = EphemeralMessage._parse_chat(client, message.from_id, users, chats)

        reply_to_message_id = None
        reply_to_ephemeral_message_id = None
        reply_to = message.reply_to

        if isinstance(reply_to, raw.types.MessageReplyHeader) and reply_to.reply_to_msg_id:
            if reply_to.reply_to_ephemeral:
                reply_to_ephemeral_message_id = reply_to.reply_to_msg_id
            else:
                reply_to_message_id = reply_to.reply_to_msg_id

        return EphemeralMessage(
            id=message.id,
            chat=EphemeralMessage._parse_chat(client, message.peer_id, users, chats),
            receiver=(
                types.User._parse(client, users.get(message.receiver_id))
                or types.User(id=message.receiver_id, client=client)
            ),
            date=utils.timestamp_to_datetime(message.date),
            from_user=from_user,
            sender_chat=sender_chat,
            message_thread_id=message.top_msg_id,
            reply_to_message_id=reply_to_message_id,
            reply_to_ephemeral_message_id=reply_to_ephemeral_message_id,
            anchor_message_id=message.anchor_msg_id,
            chat_instance=str(message.chat_instance) if message.chat_instance is not None else None,
            outgoing=message.out,
            has_protected_content=message.noforwards,
            client=client,
            **content
        )

    async def reply(
        self,
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
        protect_content: bool = None
    ) -> Optional["EphemeralMessage"]:
        """Bound method *reply* of :obj:`~pyrogram.types.EphemeralMessage`.

        Reply with an ephemeral message that only the sender of this message can see.

        Use as a shortcut for:

        .. code-block:: python

            await client.send_ephemeral_message(
                chat_id=message.chat.id,
                receiver_user_id=message.from_user.id,
                text="hello",
                reply_to_ephemeral_message_id=message.id
            )

        Example:
            .. code-block:: python

                await message.reply("Only you can see this")

        Parameters:
            text (``str``, *optional*):
                Text of the message.

            media (:obj:`~pyrogram.types.InputMedia`, *optional*):
                Media of the message, with its caption.

            rich_message (:obj:`~pyrogram.types.InputRichMessage`, *optional*):
                Rich message to send.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.

            entities (List of :obj:`~pyrogram.types.MessageEntity`):
                List of special entities that appear in the text, which can be specified instead of *parse_mode*.

            reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup` | :obj:`~pyrogram.types.ReplyKeyboardMarkup` | :obj:`~pyrogram.types.ReplyKeyboardRemove` | :obj:`~pyrogram.types.ForceReply`, *optional*):
                Additional interface options.

            protect_content (``bool``, *optional*):
                Protects the contents of the sent message from forwarding and saving.

        Returns:
            :obj:`~pyrogram.types.EphemeralMessage`: On success, the sent message is returned.
        """
        return await self._client.send_ephemeral_message(
            chat_id=self.chat.id,
            receiver_user_id=self.from_user.id if self.from_user else self.sender_chat.id,
            text=text,
            media=media,
            rich_message=rich_message,
            parse_mode=parse_mode,
            entities=entities,
            reply_markup=reply_markup,
            reply_to_ephemeral_message_id=self.id,
            message_thread_id=self.message_thread_id,
            protect_content=protect_content
        )

    async def edit(
        self,
        text: str = None,
        media: "types.InputMedia" = None,
        rich_message: "types.InputRichMessage" = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: List["types.MessageEntity"] = None,
        reply_markup: "types.InlineKeyboardMarkup" = None
    ) -> Optional["EphemeralMessage"]:
        """Bound method *edit* of :obj:`~pyrogram.types.EphemeralMessage`.

        Use as a shortcut for:

        .. code-block:: python

            await client.edit_ephemeral_message(
                chat_id=message.chat.id,
                receiver_user_id=message.receiver.id,
                ephemeral_message_id=message.id,
                text="new text"
            )

        Example:
            .. code-block:: python

                await message.edit("new text")

        Parameters:
            text (``str``, *optional*):
                New text of the message.

            media (:obj:`~pyrogram.types.InputMedia`, *optional*):
                New media of the message, with its caption.

            rich_message (:obj:`~pyrogram.types.InputRichMessage`, *optional*):
                New rich message.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.

            entities (List of :obj:`~pyrogram.types.MessageEntity`):
                List of special entities that appear in the text, which can be specified instead of *parse_mode*.

            reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup`, *optional*):
                New inline keyboard of the message.

        Returns:
            :obj:`~pyrogram.types.EphemeralMessage`: On success, the edited message is returned.
        """
        return await self._client.edit_ephemeral_message(
            chat_id=self.chat.id,
            receiver_user_id=self.receiver.id,
            ephemeral_message_id=self.id,
            text=text,
            media=media,
            rich_message=rich_message,
            parse_mode=parse_mode,
            entities=entities,
            reply_markup=reply_markup
        )

    async def delete(self) -> bool:
        """Bound method *delete* of :obj:`~pyrogram.types.EphemeralMessage`.

        Use as a shortcut for:

        .. code-block:: python

            await client.delete_ephemeral_message(
                chat_id=message.chat.id,
                receiver_user_id=message.receiver.id,
                ephemeral_message_id=message.id
            )

        Example:
            .. code-block:: python

                await message.delete()

        Returns:
            ``bool``: True on success.
        """
        return await self._client.delete_ephemeral_message(
            chat_id=self.chat.id,
            receiver_user_id=self.receiver.id,
            ephemeral_message_id=self.id
        )
