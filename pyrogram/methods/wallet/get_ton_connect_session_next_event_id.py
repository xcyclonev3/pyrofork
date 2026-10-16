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
from pyrogram import raw


class GetTonConnectSessionNextEventId:
    async def get_ton_connect_session_next_event_id(
        self: "pyrogram.Client",
        session_id: int
    ) -> int:
        """Get the identifier of the next event of a TON Connect session.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            session_id (``int``):
                Unique identifier of the TON Connect session.

        Returns:
            ``int``: On success, the identifier of the next event is returned.

        Example:
            .. code-block:: python

                event_id = await app.get_ton_connect_session_next_event_id(session.id)
        """
        r = await self.invoke(raw.functions.wallet.TonConnectNextEventId(session_id=session_id))

        return r.event_id
