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

from typing import AsyncGenerator, Union

import pyrogram
from pyrogram import raw, types


class GetStoryAlbumStories:
    async def get_story_album_stories(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        story_album_id: int,
        limit: int = 0,
        offset: int = 0
    ) -> AsyncGenerator["types.Story", None]:
        """Get the stories added to a story album.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the user or channel that owns the album.
                For your personal stories you can simply use "me" or "self".

            story_album_id (``int``):
                Identifier of the story album.

            limit (``int``, *optional*):
                Limits the number of stories to be retrieved.
                By default, no limit is applied and all stories are returned.

            offset (``int``, *optional*):
                Number of album stories to skip; use 0 to get results from the first album story.

        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.Story` objects.

        Example:
            .. code-block:: python

                async for story in app.get_story_album_stories(chat_id, story_album_id):
                    print(story)
        """
        peer = await self.resolve_peer(chat_id)

        current = 0
        total = abs(limit) or (1 << 31) - 1

        while True:
            r = await self.invoke(
                raw.functions.stories.GetAlbumStories(
                    peer=peer,
                    album_id=story_album_id,
                    offset=offset,
                    limit=min(100, total - current)
                ),
                sleep_threshold=60
            )

            if not r.stories:
                return

            for story in r.stories:
                yield await types.Story._parse(self, story, peer)

                current += 1

                if current >= total:
                    return

            offset += len(r.stories)

            if offset >= r.count:
                return
