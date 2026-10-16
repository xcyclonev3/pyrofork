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
from pyrogram import types
from .compose_rich_message_with_ai import compose_rich_message


class FixRichMessageWithAi:
    async def fix_rich_message_with_ai(
        self: "pyrogram.Client",
        rich_message: "types.InputRichMessage"
    ) -> "types.RichMessage":
        """Fix the spelling and grammar of a rich message using an AI model.

        May raise an error with the message "AICOMPOSE_FLOOD_PREMIUM" if Telegram Premium is required to send
        further requests.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            rich_message (:obj:`~pyrogram.types.InputRichMessage`):
                The original message.

        Returns:
            :obj:`~pyrogram.types.RichMessage`: On success, the fixed rich message is returned.

        Example:
            .. code-block:: python

                from pyrogram import types

                fixed = await app.fix_rich_message_with_ai(
                    types.InputRichMessage(types.RichMessageSourceBlocks([
                        types.InputPageBlockParagraph("teh quick brown fox")
                    ]))
                )
        """
        return await compose_rich_message(self, proofread=True, text=await rich_message.write(self))
