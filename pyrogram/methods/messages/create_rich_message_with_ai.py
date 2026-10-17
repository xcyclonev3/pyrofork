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

import pyrogram
from pyrogram import raw, types
from .compose_rich_message_with_ai import compose_rich_message


class CreateRichMessageWithAi:
    async def create_rich_message_with_ai(
        self: "pyrogram.Client",
        prompt: str,
        language_code: str = None,
        add_emojis: bool = None
    ) -> "types.RichMessage":
        """Create a new rich message using an AI model.

        May raise an error with the message "AICOMPOSE_FLOOD_PREMIUM" if Telegram Premium is required to send
        further requests.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            prompt (``str``):
                Prompt that will be used to create the message.

            language_code (``str``, *optional*):
                Language code of the language in which the message will be created.

            add_emojis (``bool``, *optional*):
                Pass True to add emoji to the message.

        Returns:
            :obj:`~pyrogram.types.RichMessage`: On success, the created rich message is returned.

        Example:
            .. code-block:: python

                rich_message = await app.create_rich_message_with_ai("A short packing list for a weekend hike", "en")
        """
        return await compose_rich_message(
            self,
            emojify=add_emojis or None,
            translate_to_lang=language_code or None,
            tone=raw.types.InputAiComposeToneSingleUse(custom_prompt=prompt)
        )
