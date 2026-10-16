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

from typing import Optional

import pyrogram
from pyrogram import raw
from ..object import Object


class WebDocument(Object):
    """A remote file which is not stored on Telegram servers.

    It is used, for example, for the media of a :obj:`~pyrogram.types.TonNft` and for the icon of a
    :obj:`~pyrogram.types.TonConnectManifest`.

    Parameters:
        url (``str``):
            URL of the file.

        mime_type (``str``):
            MIME type of the file.

        size (``int``):
            Size of the file in bytes.

        access_hash (``int``, *optional*):
            Access hash of the file. It is present if the file can be downloaded through Telegram with
            :obj:`~pyrogram.raw.functions.upload.GetWebFile`; otherwise the file must be downloaded directly
            from its URL.

        width (``int``, *optional*):
            Width of the image or video, if known.

        height (``int``, *optional*):
            Height of the image or video, if known.

        file_name (``str``, *optional*):
            Name of the file, if known.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        url: str,
        mime_type: str,
        size: int,
        access_hash: Optional[int] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        file_name: Optional[str] = None
    ):
        super().__init__(client)

        self.url = url
        self.mime_type = mime_type
        self.size = size
        self.access_hash = access_hash
        self.width = width
        self.height = height
        self.file_name = file_name

    @staticmethod
    def _parse(client, web_document: "raw.base.WebDocument") -> Optional["WebDocument"]:
        if web_document is None:
            return None

        width = height = file_name = None

        for attribute in web_document.attributes:
            if isinstance(attribute, (raw.types.DocumentAttributeImageSize, raw.types.DocumentAttributeVideo)):
                width, height = attribute.w, attribute.h
            elif isinstance(attribute, raw.types.DocumentAttributeFilename):
                file_name = attribute.file_name

        return WebDocument(
            url=web_document.url,
            mime_type=web_document.mime_type,
            size=web_document.size,
            access_hash=getattr(web_document, "access_hash", None),
            width=width,
            height=height,
            file_name=file_name,
            client=client
        )
