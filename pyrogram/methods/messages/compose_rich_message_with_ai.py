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


async def compose_rich_message(client: "pyrogram.Client", **kwargs) -> "types.RichMessage":
    r = await client.invoke(raw.functions.messages.ComposeRichMessageWithAI(**kwargs))

    return types.RichMessage._parse(client, r.result)


class ComposeRichMessageWithAi:
    async def compose_rich_message_with_ai(
        self: "pyrogram.Client",
        rich_message: "types.InputRichMessage",
        translate_to_language_code: str = None,
        style: Union[str, "types.TextCompositionStyle"] = None,
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

            style (``str`` | :obj:`~pyrogram.types.TextCompositionStyle`, *optional*):
                Style of the resulting message: a style, or the name of a default or installed text composition
                style, or the slug of another one. Pass None to keep the current style of the message.

            custom_prompt (``str``, *optional*):
                Custom prompt to be used instead of *style*.

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
        tone = await types.TextCompositionStyle._get_input(self, style, custom_prompt, optional=True)

        return await compose_rich_message(
            self,
            emojify=add_emojis or None,
            text=await rich_message.write(self),
            translate_to_lang=translate_to_language_code or None,
            tone=tone
        )
