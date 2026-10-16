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


class ReportEphemeralMessage:
    async def report_ephemeral_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        ephemeral_message_id: int,
        option_id: bytes = b"",
        text: str = ""
    ) -> "types.ReportResult":
        """Report an ephemeral message to Telegram.

        .. include:: /_includes/usable-by/users.rst

        Start with an empty *option_id*: the result tells which option to choose or whether a text is needed, then
        repeat the call with the chosen option and the text until the message is reported.

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            ephemeral_message_id (``int``):
                Identifier of the ephemeral message to report.

            option_id (``bytes``, *optional*):
                Identifier of the chosen option, from a previous result.

            text (``str``, *optional*):
                Additional report details.

        Returns:
            :obj:`~pyrogram.types.ReportResult`: The result of the report.

        Example:
            .. code-block:: python

                result = await app.report_ephemeral_message(chat_id, message_id)
                result = await app.report_ephemeral_message(chat_id, message_id, result.options[0].id)
        """
        r = await self.invoke(
            raw.functions.ephemeral.ReportMessage(
                peer=await self.resolve_peer(chat_id),
                id=ephemeral_message_id,
                option=option_id,
                message=text
            )
        )

        return types.ReportResult._parse(self, r)
