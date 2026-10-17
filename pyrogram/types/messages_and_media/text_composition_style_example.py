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

from ..object import Object


class TextCompositionStyleExample(Object):
    """An example of how a :obj:`~pyrogram.types.TextCompositionStyle` changes a text.

    Parameters:
        source_text (:obj:`~pyrogram.types.FormattedText`):
            The source text.

        result_text (:obj:`~pyrogram.types.FormattedText`):
            The text after the style was applied to the source text.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        source_text: "types.FormattedText",
        result_text: "types.FormattedText"
    ):
        super().__init__(client)

        self.source_text = source_text
        self.result_text = result_text

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        example: "raw.base.AiComposeToneExample"
    ) -> Optional["TextCompositionStyleExample"]:
        if not isinstance(example, raw.types.AiComposeToneExample):
            return None

        return TextCompositionStyleExample(
            source_text=types.FormattedText._parse(client, example.from_peer),
            result_text=types.FormattedText._parse(client, example.to),
            client=client
        )
