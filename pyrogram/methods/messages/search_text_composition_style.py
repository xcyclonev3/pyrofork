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


class SearchTextCompositionStyle:
    async def search_text_composition_style(
        self: "pyrogram.Client",
        style: Union[str, "types.TextCompositionStyle"]
    ) -> Optional["types.TextCompositionStyle"]:
        """Find a text composition style by its name, for example the name in a https://t.me/addstyle/<name> link.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            style (``str`` | :obj:`~pyrogram.types.TextCompositionStyle`):
                Name of the style, or the style itself to get its up to date information.

        Returns:
            :obj:`~pyrogram.types.TextCompositionStyle` | ``None``: On success, the style is returned.

        Example:
            .. code-block:: python

                style = await app.search_text_composition_style("AbCdEf123")
                await app.add_text_composition_style(style)
        """
        r = await self.invoke(
            raw.functions.aicompose.GetTone(
                tone=await types.TextCompositionStyle._get_input(self, style)
            )
        )

        if not isinstance(r, raw.types.aicompose.Tones) or len(r.tones) != 1:
            return None

        return types.TextCompositionStyle._parse(self, r.tones[0], {u.id: u for u in r.users})
