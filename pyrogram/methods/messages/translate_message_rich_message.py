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
from pyrogram import types
from .translate_rich_message import get_translation_tone, translate_rich_messages


class TranslateMessageRichMessage:
    async def translate_message_rich_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        to_language_code: str,
        tone: str = None
    ) -> "types.RichMessage":
        """Translate the rich message of a message to the given language.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the chat the message belongs to.

            message_id (``int``):
                Identifier of the message.

            to_language_code (``str``):
                Language code of the language to which the message is translated.

            tone (``str``, *optional*):
                Tone of the translation: "formal", "neutral" or "casual"; defaults to "neutral".

        Returns:
            :obj:`~pyrogram.types.RichMessage`: On success, the translated rich message is returned.

        Example:
            .. code-block:: python

                translated = await app.translate_message_rich_message(chat_id, message_id, "en")
        """
        return await translate_rich_messages(
            self,
            peer=await self.resolve_peer(chat_id),
            id=[message_id],
            to_lang=to_language_code,
            tone=get_translation_tone(tone)
        )
