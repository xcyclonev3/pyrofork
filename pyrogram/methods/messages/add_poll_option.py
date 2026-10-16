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


class AddPollOption:
    async def add_poll_option(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        text: str,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: List["types.MessageEntity"] = None,
        media: Union[
            "types.InputMediaPhoto",
            "types.InputMediaVideo",
            "types.InputMediaAnimation",
            "types.InputMediaAudio",
            "types.InputMediaDocument"
        ] = None
    ) -> bool:
        """Add an answer option to a poll that allows it.

        Polls allow it when sent with *allow_adding_options* (see :meth:`~pyrogram.Client.send_poll`).

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            message_id (``int``):
                Identifier of the message with the poll.

            text (``str``):
                Text of the option, 1-100 characters.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.

            entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
                List of special entities that appear in the option text, which can be specified instead of
                *parse_mode*.

            media (:obj:`~pyrogram.types.InputMediaPhoto` | :obj:`~pyrogram.types.InputMediaVideo` | :obj:`~pyrogram.types.InputMediaAnimation` | :obj:`~pyrogram.types.InputMediaAudio` | :obj:`~pyrogram.types.InputMediaDocument`, *optional*):
                Media of the option.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.add_poll_option(chat_id, message_id, "Another answer")
        """
        peer = await self.resolve_peer(chat_id)
        text, entities = (await utils.parse_text_entities(self, text, parse_mode, entities)).values()

        await self.invoke(
            raw.functions.messages.AddPollAnswer(
                peer=peer,
                msg_id=message_id,
                answer=raw.types.InputPollAnswer(
                    text=raw.types.TextWithEntities(text=text, entities=entities or []),
                    media=await utils.get_input_poll_media(self, peer, media)
                )
            )
        )

        return True
