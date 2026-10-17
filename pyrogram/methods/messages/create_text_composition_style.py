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
from pyrogram import raw, types


class CreateTextCompositionStyle:
    async def create_text_composition_style(
        self: "pyrogram.Client",
        title: str,
        custom_emoji_id: int,
        prompt: str,
        show_creator: Optional[bool] = None
    ) -> "types.TextCompositionStyle":
        """Create a custom text composition style.

        The style is added to the styles of the current user.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            title (``str``):
                Title of the style.

            custom_emoji_id (``int``):
                Identifier of the custom emoji corresponding to the style.

            prompt (``str``):
                Prompt that describes how the style must change a text.

            show_creator (``bool``, *optional*):
                Pass True to publicly show the current user as the creator of the style.

        Returns:
            :obj:`~pyrogram.types.TextCompositionStyle`: On success, the created style is returned.

        Example:
            .. code-block:: python

                style = await app.create_text_composition_style(
                    "Pirate", custom_emoji_id, "Rewrite the text the way a pirate would say it"
                )
                print(style.link)
        """
        r = await self.invoke(
            raw.functions.aicompose.CreateTone(
                emoji_id=custom_emoji_id,
                title=title,
                prompt=prompt,
                display_author=show_creator
            )
        )

        return types.TextCompositionStyle._parse(self, r)
