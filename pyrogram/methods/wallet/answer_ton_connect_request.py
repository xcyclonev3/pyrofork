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
from pyrogram import raw


class AnswerTonConnectRequest:
    async def answer_ton_connect_request(
        self: "pyrogram.Client",
        session_id: int,
        message_id: int,
        body: bytes,
        trace_id: Optional[str] = None
    ) -> bool:
        """Send the answer to a request received through TON Connect.

        .. warning::

            The answer is delivered to the dApp. Answering a request, for example a request to sign or to send a
            transaction, can authorize operations on the funds of the wallet. The body must be created by your own
            TON Connect code; Pyrofork doesn't implement its cryptography.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            session_id (``int``):
                Unique identifier of the TON Connect session.

            message_id (``int``):
                Identifier of the service message with the request.

            body (``bytes``):
                Body of the response.

            trace_id (``str``, *optional*):
                Identifier to trace the request.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.answer_ton_connect_request(request.session_id, request.message_id, body, request.trace_id)
        """
        r = await self.invoke(
            raw.functions.wallet.TonConnectSubmitResponse(
                session_id=session_id,
                msg_id=message_id,
                body=body,
                trace_id=trace_id or None
            )
        )

        return bool(r)
