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

import re
from typing import Union

from pyrogram import raw, types
from ..object import Object

MEDIA_KINDS = (
    ("InputMediaPhoto", "photo"),
    ("InputMediaAnimation", "animation"),
    ("InputMediaVideo", "video"),
    ("InputMediaAudio", "audio"),
    ("InputMediaDocument", "document"),
)


def get_media_kind(media) -> str:
    for name, kind in MEDIA_KINDS:
        if isinstance(media, getattr(types, name)):
            return kind

    raise ValueError(f"{type(media).__name__} can't be used as rich message media")


class InputRichMessageMedia(Object):
    """A media to be used in a rich message sent from Markdown or HTML, see
    :obj:`~pyrogram.types.RichMessageSourceMarkdown` and :obj:`~pyrogram.types.RichMessageSourceHtml`.

    Parameters:
        id (``str``):
            Unique identifier of the media, by which the message text refers to it; 1-64 base64url characters
            (A-Z, a-z, 0-9, "-" and "_").

        media (:obj:`~pyrogram.types.InputMediaPhoto` | :obj:`~pyrogram.types.InputMediaVideo` | :obj:`~pyrogram.types.InputMediaAnimation` | :obj:`~pyrogram.types.InputMediaAudio` | :obj:`~pyrogram.types.InputMediaDocument`):
            The media to send: a local file, an HTTP URL or a file id. The caption of the input media is ignored.
    """

    def __init__(
        self,
        id: str,
        media: Union[
            "types.InputMediaPhoto",
            "types.InputMediaVideo",
            "types.InputMediaAnimation",
            "types.InputMediaAudio",
            "types.InputMediaDocument"
        ]
    ):
        super().__init__()

        self.id = id
        self.media = media

    async def write(self, writer) -> "raw.base.InputRichFile":
        if not isinstance(self.id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.id):
            raise ValueError(f"Invalid rich message media identifier {self.id!r}: use 1-64 base64url characters")

        kind = get_media_kind(self.media)
        uploaded = await writer.upload(kind, self.media)

        if kind == "photo":
            return raw.types.InputRichFilePhoto(id=self.id, photo=uploaded)

        return raw.types.InputRichFileDocument(id=self.id, document=uploaded)
