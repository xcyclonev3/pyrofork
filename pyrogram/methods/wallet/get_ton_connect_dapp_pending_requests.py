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


class GetTonConnectDappPendingRequests:
    async def get_ton_connect_dapp_pending_requests(
        self: "pyrogram.Client",
        dapp_client_id: str
    ) -> "types.TonConnectRequests":
        """Get the pending requests received from a dApp through TON Connect.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            dapp_client_id (``str``):
                Hex-encoded public X25519 key of the dApp.

        Returns:
            :obj:`~pyrogram.types.TonConnectRequests`: On success, the session with the dApp and its pending requests
            are returned.

        Example:
            .. code-block:: python

                pending = await app.get_ton_connect_dapp_pending_requests(dapp_client_id)
        """
        r = await self.invoke(raw.functions.wallet.TonConnectGetPending(dapp_client_id=dapp_client_id))

        return types.TonConnectRequests._parse(self, r)
