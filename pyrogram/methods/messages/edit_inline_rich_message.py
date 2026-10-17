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
from pyrogram import raw, types, utils
from .inline_session import get_session


class EditInlineRichMessage:
    async def edit_inline_rich_message(
        self: "pyrogram.Client",
        inline_message_id: str,
        rich_message: "types.InputRichMessage",
        reply_markup: "types.InlineKeyboardMarkup" = None
    ) -> bool:
        """Edit the text of an inline text or rich message sent via the bot, replacing it with a rich message.

        .. include:: /_includes/usable-by/bots.rst

        Parameters:
            inline_message_id (``str``):
                Identifier of the inline message.

            rich_message (:obj:`~pyrogram.types.InputRichMessage`):
                New content of the message. Media should be passed as file ids: other media are uploaded to the
                bot's own chat first.

            reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup`, *optional*):
                An InlineKeyboardMarkup object.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                from pyrogram import types

                await app.edit_inline_rich_message(
                    inline_message_id,
                    types.InputRichMessage(types.RichMessageSourceHtml("<h2>Done</h2>"))
                )
        """
        unpacked = utils.unpack_inline_message_id(inline_message_id)
        rpc = raw.functions.messages.EditInlineBotMessage(
            id=unpacked,
            reply_markup=await reply_markup.write(self) if reply_markup else None,
            rich_message=await rich_message.write(self)
        )

        session = await get_session(self, unpacked.dc_id)

        return await session.invoke(rpc, sleep_threshold=self.sleep_threshold)
