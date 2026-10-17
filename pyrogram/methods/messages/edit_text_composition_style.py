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

from typing import Optional, Union

import pyrogram
from pyrogram import raw, types


class EditTextCompositionStyle:
    async def edit_text_composition_style(
        self: "pyrogram.Client",
        style: Union[str, "types.TextCompositionStyle"],
        title: Optional[str] = None,
        custom_emoji_id: Optional[int] = None,
        prompt: Optional[str] = None,
        show_creator: Optional[bool] = None
    ) -> "types.TextCompositionStyle":
        """Edit a custom text composition style created by the current user.

        Only the passed fields are changed.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            style (``str`` | :obj:`~pyrogram.types.TextCompositionStyle`):
                The style or its name.

            title (``str``, *optional*):
                New title of the style.

            custom_emoji_id (``int``, *optional*):
                Identifier of the new custom emoji corresponding to the style.

            prompt (``str``, *optional*):
                New prompt that describes how the style must change a text.

            show_creator (``bool``, *optional*):
                Pass True to publicly show the current user as the creator of the style, False to hide them.

        Returns:
            :obj:`~pyrogram.types.TextCompositionStyle`: On success, the edited style is returned.

        Example:
            .. code-block:: python

                await app.edit_text_composition_style(style, title="Captain", show_creator=False)
        """
        r = await self.invoke(
            raw.functions.aicompose.UpdateTone(
                tone=await types.TextCompositionStyle._get_input(self, style),
                display_author=show_creator,
                emoji_id=custom_emoji_id,
                title=title,
                prompt=prompt
            )
        )

        return types.TextCompositionStyle._parse(self, r)
