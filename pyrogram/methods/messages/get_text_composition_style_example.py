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


class GetTextCompositionStyleExample:
    async def get_text_composition_style_example(
        self: "pyrogram.Client",
        style: Union[str, "types.TextCompositionStyle"],
        example_number: int = 0
    ) -> "types.TextCompositionStyleExample":
        """Get an example of how a text composition style changes a text.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            style (``str`` | :obj:`~pyrogram.types.TextCompositionStyle`):
                The style or its name.

            example_number (``int``, *optional*):
                0-based number of the example, to cycle through the available examples
                (their count is the *aicompose_tone_examples_num* value of the client configuration).
                Defaults to 0.

        Returns:
            :obj:`~pyrogram.types.TextCompositionStyleExample`: On success, the example is returned.

        Example:
            .. code-block:: python

                example = await app.get_text_composition_style_example(style, 1)
                print(example.source_text.text, "->", example.result_text.text)
        """
        r = await self.invoke(
            raw.functions.aicompose.GetToneExample(
                tone=await types.TextCompositionStyle._get_input(self, style),
                num=example_number
            )
        )

        return types.TextCompositionStyleExample._parse(self, r)
