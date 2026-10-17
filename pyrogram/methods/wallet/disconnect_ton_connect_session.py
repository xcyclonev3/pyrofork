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


class DisconnectTonConnectSession:
    async def disconnect_ton_connect_session(
        self: "pyrogram.Client",
        session_id: int,
        body: Optional[bytes] = None
    ) -> bool:
        """Close a TON Connect session.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            session_id (``int``):
                Unique identifier of the TON Connect session to close.

            body (``bytes``, *optional*):
                Encrypted body of the disconnect event, created by your own TON Connect code; omit it if the session
                has already been closed. Use :meth:`~pyrogram.Client.get_ton_connect_session_next_event_id` to get
                the identifier of the event.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.disconnect_ton_connect_session(session.id, body)
        """
        r = await self.invoke(
            raw.functions.wallet.TonConnectCloseSession(
                session_id=session_id,
                body=body or None
            )
        )

        return bool(r)
