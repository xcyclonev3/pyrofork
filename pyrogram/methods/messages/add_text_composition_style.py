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

from typing import Union

import pyrogram
from pyrogram import raw, types


class AddTextCompositionStyle:
    async def add_text_composition_style(
        self: "pyrogram.Client",
        style: Union[str, "types.TextCompositionStyle"]
    ) -> bool:
        """Add a custom text composition style to the styles of the current user.

        The number of added styles is limited, and the limit is higher for Telegram Premium users.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            style (``str`` | :obj:`~pyrogram.types.TextCompositionStyle`):
                The style or its name.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.add_text_composition_style("AbCdEf123")
        """
        return await self.invoke(
            raw.functions.aicompose.SaveTone(
                tone=await types.TextCompositionStyle._get_input(self, style),
                unsave=False
            )
        )
