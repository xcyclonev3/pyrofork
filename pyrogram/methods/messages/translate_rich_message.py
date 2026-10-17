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
from pyrogram import errors, raw, types


def get_translation_tone(tone: Optional[str]) -> Optional[str]:
    """The tone to send, as TDLib checks it: "neutral" is the default one."""
    if tone in (None, "", "neutral"):
        return None

    if tone not in ("formal", "casual"):
        raise ValueError(f"Invalid tone {tone!r}: must be formal, neutral or casual")

    return tone


async def translate_rich_messages(client: "pyrogram.Client", **kwargs) -> Optional["types.RichMessage"]:
    try:
        r = await client.invoke(raw.functions.messages.TranslateRichMessage(**kwargs))
    except errors.InputTextEmpty:
        return types.RichMessage(client=client, blocks=[])

    return types.RichMessage._parse(client, r.result[0]) if r.result else None


class TranslateRichMessage:
    async def translate_rich_message(
        self: "pyrogram.Client",
        rich_message: "types.InputRichMessage",
        to_language_code: str,
        tone: str = None
    ) -> "types.RichMessage":
        """Translate a rich message to the given language.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            rich_message (:obj:`~pyrogram.types.InputRichMessage`):
                The rich message to translate.

            to_language_code (``str``):
                Language code of the language to which the message is translated.

            tone (``str``, *optional*):
                Tone of the translation: "formal", "neutral" or "casual"; defaults to "neutral".

        Returns:
            :obj:`~pyrogram.types.RichMessage`: On success, the translated rich message is returned.

        Example:
            .. code-block:: python

                from pyrogram import types

                translated = await app.translate_rich_message(
                    types.InputRichMessage(types.RichMessageSourceBlocks([
                        types.InputPageBlockParagraph("Ciao a tutti")
                    ])),
                    "en"
                )
        """
        return await translate_rich_messages(
            self,
            text=[await rich_message.write(self)],
            to_lang=to_language_code,
            tone=get_translation_tone(tone)
        )
