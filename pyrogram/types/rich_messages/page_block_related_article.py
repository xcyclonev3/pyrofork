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

import pyrogram
from pyrogram import raw, types, utils
from ..object import Object


class PageBlockRelatedArticle(Object):
    """A related article of a :obj:`~pyrogram.types.PageBlockRelatedArticles` block.

    Parameters:
        url (``str``):
            Related article URL.

        title (``str``, *optional*):
            Article title.

        description (``str``, *optional*):
            Article description.

        photo (:obj:`~pyrogram.types.Photo`, *optional*):
            Article photo.

        author (``str``, *optional*):
            Article author.

        publish_date (:py:obj:`~datetime.datetime`, *optional*):
            Date the article was published.
    """

    def __init__(
        self,
        *,
        url: str,
        title: str = None,
        description: str = None,
        photo: "types.Photo" = None,
        author: str = None,
        publish_date: datetime = None
    ):
        super().__init__()

        self.url = url
        self.title = title
        self.description = description
        self.photo = photo
        self.author = author
        self.publish_date = publish_date

    @staticmethod
    def _parse(client: "pyrogram.Client", article: "raw.types.PageRelatedArticle", context=None) -> "PageBlockRelatedArticle":
        photo = context.photos.get(article.photo_id) if context and article.photo_id else None

        return PageBlockRelatedArticle(
            url=article.url,
            title=article.title,
            description=article.description,
            photo=types.Photo._parse(client, photo) if photo is not None else None,
            author=article.author,
            publish_date=utils.timestamp_to_datetime(article.published_date) if article.published_date else None
        )
