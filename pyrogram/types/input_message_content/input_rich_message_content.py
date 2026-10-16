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
from .input_message_content import InputMessageContent


class InputRichMessageContent(InputMessageContent):
    """Content of a rich message to be sent as the result of an inline query.

    Parameters:
        rich_message (:obj:`~pyrogram.types.InputRichMessage`):
            The rich message to send. Media should be passed as file ids: other media are uploaded to the bot's own
            chat first.

    Example:
        .. code-block:: python

            from pyrogram import types

            types.InlineQueryResultArticle(
                "Report",
                types.InputRichMessageContent(types.InputRichMessage(
                    types.RichMessageSourceHtml("<h1>Report</h1><p>All good</p>")
                ))
            )
    """

    def __init__(self, rich_message: "types.InputRichMessage"):
        super().__init__()

        self.rich_message = rich_message

    async def write(self, client: "pyrogram.Client", reply_markup):
        return raw.types.InputBotInlineMessageRichMessage(
            rich_message=await self.rich_message.write(client),
            reply_markup=await reply_markup.write(client) if reply_markup else None
        )
