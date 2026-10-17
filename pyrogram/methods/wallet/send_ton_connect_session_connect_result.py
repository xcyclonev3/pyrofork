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


class SendTonConnectSessionConnectResult:
    async def send_ton_connect_session_connect_result(
        self: "pyrogram.Client",
        session_id: int,
        challenge_answer: bytes,
        body: bytes,
        is_error: Optional[bool] = None,
        trace_id: Optional[str] = None
    ) -> bool:
        """Accept or reject a pending TON Connect session.

        .. warning::

            Accepting the session connects the TON wallet of the current user to the dApp, which can then send
            requests to the wallet. The challenge answer and the body must be created by your own TON Connect code;
            Pyrofork doesn't implement its cryptography.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            session_id (``int``):
                Unique identifier of the session.

            challenge_answer (``bytes``):
                Answer to the challenge received from :meth:`~pyrogram.Client.set_ton_connect_session_wallet_client_id`.

            body (``bytes``):
                Body of the result.

            is_error (``bool``, *optional*):
                Pass True to reject the session. By default, the session is accepted.

            trace_id (``str``, *optional*):
                Identifier to trace the connection request.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.send_ton_connect_session_connect_result(session.id, challenge_answer, body)
        """
        r = await self.invoke(
            raw.functions.wallet.TonConnectSubmitConnectResult(
                session_id=session_id,
                challenge_answer=challenge_answer,
                body=body,
                error=is_error,
                trace_id=trace_id or None
            )
        )

        return bool(r)
