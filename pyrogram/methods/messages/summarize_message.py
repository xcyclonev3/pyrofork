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

from typing import Optional, Union

import pyrogram
from pyrogram import raw, types


class SummarizeMessage:
    async def summarize_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        translate_to_language_code: Optional[str] = None,
        tone: Optional[str] = None
    ) -> "types.FormattedText":
        """Summarize the text of a message using an AI model.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).

            message_id (``int``):
                Identifier of the message to summarize.

            translate_to_language_code (``str``, *optional*):
                Language code of the language in which the summary will be written.
                See :meth:`~pyrogram.Client.translate_message_text` for the supported language codes.
                By default, the summary is written in the language of the message.

            tone (``str``, *optional*):
                Tone of the summary, one of "formal", "neutral" and "casual". Defaults to "neutral".

        Returns:
            :obj:`~pyrogram.types.FormattedText`: On success, the summary is returned.

        Example:
            .. code-block:: python

                summary = await app.summarize_message(chat_id, message_id, translate_to_language_code="en")
                print(summary.text)
        """
        r = await self.invoke(
            raw.functions.messages.SummarizeText(
                peer=await self.resolve_peer(chat_id),
                id=message_id,
                to_lang=translate_to_language_code or None,
                # Like TDLib, the neutral tone is the default one and isn't sent
                tone=None if tone in (None, "", "neutral") else tone
            )
        )

        return types.FormattedText._parse(self, r)
