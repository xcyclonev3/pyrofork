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


class GetTonConnectSessionPendingRequests:
    async def get_ton_connect_session_pending_requests(
        self: "pyrogram.Client",
        session_id: int
    ) -> "types.TonConnectRequests":
        """Get the pending requests of a TON Connect session.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            session_id (``int``):
                Unique identifier of the session.

        Returns:
            :obj:`~pyrogram.types.TonConnectRequests`: On success, the session and its pending requests are returned.

        Example:
            .. code-block:: python

                pending = await app.get_ton_connect_session_pending_requests(session.id)

                for request in pending.requests:
                    print(request.message_id, request.topic)
        """
        r = await self.invoke(raw.functions.wallet.TonConnectGetPending(session_id=session_id))

        return types.TonConnectRequests._parse(self, r)
