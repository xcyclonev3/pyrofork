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

from typing import List, Optional, Union

import pyrogram
from pyrogram import enums, raw, types, utils


class ComposeTextWithAi:
    async def compose_text_with_ai(
        self: "pyrogram.Client",
        text: str,
        translate_to_language_code: Optional[str] = None,
        style: Optional[Union[str, "types.TextCompositionStyle"]] = None,
        add_emojis: Optional[bool] = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: Optional[List["types.MessageEntity"]] = None
    ) -> "types.FormattedText":
        """Change a text using an AI model: translate it, rewrite it in a style and/or add emojis to it.

        Pass at least one of *translate_to_language_code*, *style* and *add_emojis*.
        The number of AI text changes per day is limited, and the limit is much higher for Telegram Premium users:
        an AICOMPOSE_FLOOD_PREMIUM error is raised once it is reached.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            text (``str``):
                The original text.

            translate_to_language_code (``str``, *optional*):
                Language code of the language to which the text will be translated.
                See :meth:`~pyrogram.Client.translate_message_text` for the supported language codes.

            style (``str`` | :obj:`~pyrogram.types.TextCompositionStyle`, *optional*):
                Style of the resulting text, or its name, see :meth:`~pyrogram.Client.get_text_composition_styles`.
                A name costs one more request, to find the style.
                By default, the style of the text is kept.

            add_emojis (``bool``, *optional*):
                Pass True to add emojis to the text.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.

            entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
                List of special entities that appear in the text, which can be specified instead of *parse_mode*.

        Returns:
            :obj:`~pyrogram.types.FormattedText`: On success, the changed text is returned.

        Example:
            .. code-block:: python

                # Rewrite a text in the formal style and translate it to Spanish
                result = await app.compose_text_with_ai(
                    "hey, are we still on for tonight?",
                    translate_to_language_code="es",
                    style="formal"
                )
                print(result.text)

                # Add emojis to a text
                await app.compose_text_with_ai("I love pizza", add_emojis=True)
        """
        message, entities = (await utils.parse_text_entities(self, text, parse_mode, entities)).values()

        r = await self.invoke(
            raw.functions.messages.ComposeMessageWithAI(
                text=raw.types.TextWithEntities(text=message, entities=entities or []),
                translate_to_lang=translate_to_language_code or None,
                tone=await types.TextCompositionStyle._get_input(self, style, optional=True),
                emojify=add_emojis
            )
        )

        return types.FormattedText._parse(self, r.result_text)
