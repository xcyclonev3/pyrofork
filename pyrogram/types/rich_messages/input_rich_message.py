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

import os
import re
from typing import BinaryIO, Union

import pyrogram
from pyrogram import raw, types, utils
from ..object import Object

DEFAULT_FILE_NAMES = {
    "animation": "animation.mp4",
    "audio": "audio.mp3",
    "document": "file",
    "video": "video.mp4",
    "voice": "voice.ogg",
}

DEFAULT_MIME_TYPES = {
    "animation": "video/mp4",
    "audio": "audio/mpeg",
    "document": "application/zip",
    "video": "video/mp4",
    "voice": "audio/ogg",
}


def get_input_user(peer: "raw.base.InputPeer") -> "raw.base.InputUser":
    if isinstance(peer, raw.types.InputPeerSelf):
        return raw.types.InputUserSelf()

    if isinstance(peer, raw.types.InputPeerUser):
        return raw.types.InputUser(user_id=peer.user_id, access_hash=peer.access_hash)

    if isinstance(peer, raw.types.InputPeerUserFromMessage):
        return raw.types.InputUserFromMessage(peer=peer.peer, msg_id=peer.msg_id, user_id=peer.user_id)

    raise ValueError("The peer is not a user")


class RichMessageWriter:
    """Builds the raw form of a rich message: uploads its media and collects the photos, documents and users the
    blocks refer to. Not a public type: :meth:`~pyrogram.types.InputRichMessage.write` uses it."""

    def __init__(
        self,
        client: "pyrogram.Client",
        chat_id: Union[int, str] = None,
        business_connection_id: str = None
    ):
        self.client = client
        self.chat_id = chat_id
        self.business_connection_id = business_connection_id
        self.photos = []
        self.documents = []
        self.users = []

    async def get_input_user(self, user_id: Union[int, str]) -> "raw.base.InputUser":
        return get_input_user(await self.client.resolve_peer(user_id))

    async def add_user(self, user_id: Union[int, str]) -> "raw.base.InputUser":
        """Get a mentioned user, also sent along with the message blocks."""
        input_user = await self.get_input_user(user_id)

        if all(user.write() != input_user.write() for user in self.users):
            self.users.append(input_user)

        return input_user

    async def add_photo(self, media) -> "raw.types.InputPhoto":
        photo = await self.upload("photo", media)
        self.photos.append(photo)
        return photo

    async def add_document(self, kind: str, media, **kwargs) -> "raw.types.InputDocument":
        document = await self.upload(kind, media, **kwargs)
        self.documents.append(document)
        return document

    async def upload(
        self,
        kind: str,
        media: Union[str, BinaryIO, "types.InputMedia"],
        duration: int = 0
    ) -> Union["raw.types.InputPhoto", "raw.types.InputDocument"]:
        """Upload a media of the given kind (photo, animation, audio, document, video or voice) to Telegram
        servers, unless it is a file id already."""
        client = self.client

        if isinstance(media, types.InputMedia):
            file = media.media
        else:
            file, media = media, None

        is_path = isinstance(file, str) and os.path.isfile(file)
        is_url = isinstance(file, str) and not is_path and re.match("^https?://", file) is not None

        if isinstance(file, str) and not is_path and not is_url:
            input_media = utils.get_input_media_from_file_id(file)

            if (kind == "photo") != isinstance(input_media, raw.types.InputMediaPhoto):
                raise ValueError(f"The file id {file!r} can't be used as a {kind}")

            return input_media.id

        peer = await client.resolve_peer(self.chat_id) if self.chat_id is not None else raw.types.InputPeerSelf()

        if kind == "photo":
            if is_url:
                input_media = raw.types.InputMediaPhotoExternal(url=file)
            else:
                input_media = raw.types.InputMediaUploadedPhoto(file=await client.save_file(file))

            r = await client.invoke(raw.functions.messages.UploadMedia(
                peer=peer, media=input_media, business_connection_id=self.business_connection_id
            ))

            return raw.types.InputPhoto(
                id=r.photo.id,
                access_hash=r.photo.access_hash,
                file_reference=r.photo.file_reference
            )

        if is_url:
            input_media = raw.types.InputMediaDocumentExternal(url=file)
        else:
            file_name = (
                getattr(media, "file_name", None)
                or (os.path.basename(file) if is_path else getattr(file, "name", None))
                or DEFAULT_FILE_NAMES[kind]
            )
            file_name = os.path.basename(file_name)
            attributes = [raw.types.DocumentAttributeFilename(file_name=file_name)]

            if kind in ("animation", "video"):
                attributes.insert(0, raw.types.DocumentAttributeVideo(
                    supports_streaming=True if kind == "animation" else getattr(media, "supports_streaming", None),
                    duration=getattr(media, "duration", 0) or 0,
                    w=getattr(media, "width", 0) or 0,
                    h=getattr(media, "height", 0) or 0
                ))

                if kind == "animation":
                    attributes.append(raw.types.DocumentAttributeAnimated())
            elif kind == "audio":
                attributes.insert(0, raw.types.DocumentAttributeAudio(
                    duration=getattr(media, "duration", 0) or 0,
                    performer=getattr(media, "performer", None),
                    title=getattr(media, "title", None)
                ))
            elif kind == "voice":
                attributes = [raw.types.DocumentAttributeAudio(voice=True, duration=duration or 0)]

            input_media = raw.types.InputMediaUploadedDocument(
                file=await client.save_file(file),
                thumb=await client.save_file(getattr(media, "thumb", None)),
                mime_type=client.guess_mime_type(file_name) or DEFAULT_MIME_TYPES[kind],
                attributes=attributes,
                nosound_video=True if kind == "animation" else None
            )

        r = await client.invoke(raw.functions.messages.UploadMedia(
            peer=peer, media=input_media, business_connection_id=self.business_connection_id
        ))

        return raw.types.InputDocument(
            id=r.document.id,
            access_hash=r.document.access_hash,
            file_reference=r.document.file_reference
        )


class InputRichMessage(Object):
    """A rich message to send.

    The total length of all texts, including custom emoji alternative texts and formula sources, the total number of
    blocks, list items and table rows, the depth of nested blocks and rich texts, the number of media and the number
    of table columns are limited by Telegram.

    Parameters:
        source (:obj:`~pyrogram.types.RichMessageSource`):
            Source of the rich message: :obj:`~pyrogram.types.RichMessageSourceBlocks`,
            :obj:`~pyrogram.types.RichMessageSourceMarkdown` or :obj:`~pyrogram.types.RichMessageSourceHtml`.

        is_rtl (``bool``, *optional*):
            Pass True if the message must be shown from right to left.

        detect_automatic_blocks (``bool``, *optional*):
            Pass True to enable detection of URLs, email addresses and other automatic blocks.
            Defaults to False, as in TDLib.

    Example:
        .. code-block:: python

            from pyrogram import types

            # Blocks
            types.InputRichMessage(types.RichMessageSourceBlocks([
                types.InputPageBlockSectionHeading("Weekly report", size=1),
                types.InputPageBlockParagraph(types.RichTexts(["Sales grew by ", types.RichTextBold("12%")])),
                types.InputPageBlockPhoto("chart.png", caption="Sales"),
            ]))

            # HTML or Markdown, parsed by Telegram (bots only)
            types.InputRichMessage(types.RichMessageSourceHtml("<h1>Weekly report</h1><p>Sales grew</p>"))
    """

    def __init__(
        self,
        source: "types.RichMessageSource",
        is_rtl: bool = None,
        detect_automatic_blocks: bool = None
    ):
        super().__init__()

        self.source = source
        self.is_rtl = is_rtl
        self.detect_automatic_blocks = detect_automatic_blocks

    async def write(
        self,
        client: "pyrogram.Client",
        chat_id: Union[int, str] = None,
        business_connection_id: str = None
    ) -> "raw.base.InputRichMessage":
        """Upload the media of the message, if any, to the chat with *chat_id* (or to Saved Messages), through the
        business connection if any, and return the raw rich message."""
        if self.source is None:
            raise ValueError("The rich message must have a source")

        return await self.source.write(
            RichMessageWriter(client, chat_id, business_connection_id),
            rtl=self.is_rtl or None,
            noautolink=None if self.detect_automatic_blocks else True
        )
