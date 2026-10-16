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

import re
from typing import Optional

import pyrogram
from pyrogram import raw, types


async def get_input_ai_compose_tone(
    client: "pyrogram.Client",
    style_name: Optional[str]
) -> Optional["raw.base.InputAiComposeTone"]:
    """The text composition style (AI compose tone) with the given name, looked up as TDLib does: a default or
    installed style by its name, any other style by its slug."""
    if not style_name:
        return None

    r = await client.invoke(raw.functions.aicompose.GetTones(hash=0))

    for tone in getattr(r, "tones", []):
        if isinstance(tone, raw.types.AiComposeToneDefault) and tone.tone == style_name:
            return raw.types.InputAiComposeToneDefault(tone=style_name)

        if isinstance(tone, raw.types.AiComposeTone) and tone.slug == style_name:
            return raw.types.InputAiComposeToneID(id=tone.id, access_hash=tone.access_hash)

    if re.fullmatch(r"[A-Za-z0-9_-]+", style_name):
        return raw.types.InputAiComposeToneSlug(slug=style_name)

    raise ValueError(f"Text composition style {style_name!r} not found")


async def compose_rich_message(client: "pyrogram.Client", **kwargs) -> "types.RichMessage":
    r = await client.invoke(raw.functions.messages.ComposeRichMessageWithAI(**kwargs))

    return types.RichMessage._parse(client, r.result)


class ComposeRichMessageWithAi:
    async def compose_rich_message_with_ai(
        self: "pyrogram.Client",
        rich_message: "types.InputRichMessage",
        translate_to_language_code: str = None,
        style_name: str = None,
        custom_prompt: str = None,
        add_emojis: bool = None
    ) -> "types.RichMessage":
        """Change a rich message using an AI model.

        May raise an error with the message "AICOMPOSE_FLOOD_PREMIUM" if Telegram Premium is required to send
        further requests.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            rich_message (:obj:`~pyrogram.types.InputRichMessage`):
                The original message.

            translate_to_language_code (``str``, *optional*):
                Language code of the language to which the message will be translated.

            style_name (``str``, *optional*):
                Name of the style of the resulting message: the name of a default or installed text composition
                style, or the slug of another one. Pass None to keep the current style of the message.

            custom_prompt (``str``, *optional*):
                Custom prompt to be used instead of *style_name*.

            add_emojis (``bool``, *optional*):
                Pass True to add emoji to the message.

        Returns:
            :obj:`~pyrogram.types.RichMessage`: On success, the changed rich message is returned.

        Example:
            .. code-block:: python

                from pyrogram import types

                composed = await app.compose_rich_message_with_ai(
                    types.InputRichMessage(types.RichMessageSourceBlocks([
                        types.InputPageBlockParagraph("we shipped the release")
                    ])),
                    custom_prompt="Make it sound exciting",
                    add_emojis=True
                )
        """
        if custom_prompt:
            tone = raw.types.InputAiComposeToneSingleUse(custom_prompt=custom_prompt)
        else:
            tone = await get_input_ai_compose_tone(self, style_name)

        return await compose_rich_message(
            self,
            emojify=add_emojis or None,
            text=await rich_message.write(self),
            translate_to_lang=translate_to_language_code or None,
            tone=tone
        )
