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

from typing import List, Optional

import pyrogram
from pyrogram import enums, raw, types, utils


class FixTextWithAi:
    async def fix_text_with_ai(
        self: "pyrogram.Client",
        text: str,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: Optional[List["types.MessageEntity"]] = None
    ) -> "types.FixedText":
        """Fix mistakes in a text using an AI model.

        The number of AI text changes per day is limited, and the limit is much higher for Telegram Premium users:
        an AICOMPOSE_FLOOD_PREMIUM error is raised once it is reached.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            text (``str``):
                The original text.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.

            entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
                List of special entities that appear in the text, which can be specified instead of *parse_mode*.

        Returns:
            :obj:`~pyrogram.types.FixedText`: On success, the fixed text and the changes made to the original text
            are returned.

        Example:
            .. code-block:: python

                fixed = await app.fix_text_with_ai("This sentense has a fiew mistakes")
                print(fixed.text.text)

                if fixed.diff_text and fixed.diff_text.entities:
                    for entity in fixed.diff_text.entities:
                        print(entity.type, entity.offset, entity.length, entity.old_text)
        """
        message, entities = (await utils.parse_text_entities(self, text, parse_mode, entities)).values()

        r = await self.invoke(
            raw.functions.messages.ComposeMessageWithAI(
                text=raw.types.TextWithEntities(text=message, entities=entities or []),
                proofread=True
            )
        )

        return types.FixedText._parse(self, r)
