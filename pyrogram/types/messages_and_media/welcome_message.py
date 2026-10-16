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
from pyrogram import enums, raw, types, utils
from ..object import Object


class WelcomeMessage(Object):
    """A welcome message of a chat.

    Welcome messages are shown to the new members of the chat, only to them.
    They are managed with :meth:`~pyrogram.Client.add_chat_welcome_message` and the related methods.

    Parameters:
        id (``int``):
            Identifier of the welcome message, unique in the chat.

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
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        id: int,
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
        rich_message: "types.RichMessage" = None
    ):
        super().__init__(client)

        self.id = id
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

    @staticmethod
    async def _parse(
        client,
        message: "raw.types.EphemeralMessage",
        users: Dict[int, "raw.base.User"] = None
    ) -> "WelcomeMessage":
        content = await utils.parse_message_content(
            client, message.message, message.entities, message.media, message.rich_message, None,
            message.invert_media, users or {}
        )
        content.pop("reply_markup")

        return WelcomeMessage(id=message.id, client=client, **content)
