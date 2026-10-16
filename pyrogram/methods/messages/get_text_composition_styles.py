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

from typing import List

import pyrogram
from pyrogram import raw, types


class GetTextCompositionStyles:
    async def get_text_composition_styles(
        self: "pyrogram.Client"
    ) -> List["types.TextCompositionStyle"]:
        """Get the text composition styles of the current user: the default styles provided by Telegram and
        the custom styles added by the user.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            List of :obj:`~pyrogram.types.TextCompositionStyle`: On success, the list of styles is returned.

        Example:
            .. code-block:: python

                styles = await app.get_text_composition_styles()

                for style in styles:
                    print(style.name, style.title)
        """
        r = await self.invoke(raw.functions.aicompose.GetTones(hash=0))

        if not isinstance(r, raw.types.aicompose.Tones):
            return types.List()

        users = {u.id: u for u in r.users}

        return types.List(
            filter(
                lambda x: x is not None,
                [types.TextCompositionStyle._parse(self, tone, users) for tone in r.tones]
            )
        )
