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


class ClaimTonConnectRequest:
    async def claim_ton_connect_request(
        self: "pyrogram.Client",
        session_id: int,
        message_id: int,
        dapp_request_id: str,
        is_rejected: Optional[bool] = None,
        challenge_answer: Optional[bytes] = None
    ) -> bool:
        """Claim a request received through TON Connect for processing on this device, or reject it.

        After claiming a request, answer it with :meth:`~pyrogram.Client.answer_ton_connect_request`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            session_id (``int``):
                Unique identifier of the TON Connect session.

            message_id (``int``):
                Identifier of the service message with the request.

            dapp_request_id (``str``):
                Identifier of the request assigned by the dApp.

            is_rejected (``bool``, *optional*):
                Pass True to reject the request.

            challenge_answer (``bytes``, *optional*):
                Answer to a challenge. Undocumented: TDLib never sends it.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.claim_ton_connect_request(request.session_id, request.message_id, dapp_request_id)
        """
        r = await self.invoke(
            raw.functions.wallet.TonConnectClaimRequest(
                session_id=session_id,
                msg_id=message_id,
                app_request_id=dapp_request_id,
                declined=is_rejected,
                challenge_answer=challenge_answer or None
            )
        )

        return bool(r)
