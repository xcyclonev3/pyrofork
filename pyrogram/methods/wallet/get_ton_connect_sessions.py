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

from typing import List

import pyrogram
from pyrogram import raw, types


class GetTonConnectSessions:
    async def get_ton_connect_sessions(
        self: "pyrogram.Client"
    ) -> List["types.TonConnectSession"]:
        """Get the active TON Connect sessions of the TON wallet of the current user.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            List of :obj:`~pyrogram.types.TonConnectSession`: On success, the list of sessions is returned.

        Example:
            .. code-block:: python

                for session in await app.get_ton_connect_sessions():
                    print(session.id, session.state)
        """
        r = await self.invoke(raw.functions.wallet.TonConnectGetSessions())

        return types.List(types.TonConnectSession._parse(self, session) for session in r.sessions)
