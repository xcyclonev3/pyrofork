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


class CreateTonConnectSession:
    async def create_ton_connect_session(
        self: "pyrogram.Client",
        dapp_client_id: str,
        manifest_url: str
    ) -> "types.TonConnectSession":
        """Create a TON Connect session between the TON wallet of the current user and a dApp.

        The new session is pending: set the key of the application with
        :meth:`~pyrogram.Client.set_ton_connect_session_wallet_client_id`, then accept or reject the connection with
        :meth:`~pyrogram.Client.send_ton_connect_session_connect_result`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            dapp_client_id (``str``):
                Hex-encoded public X25519 key of the dApp.

            manifest_url (``str``):
                URL of the manifest of the dApp.

        Returns:
            :obj:`~pyrogram.types.TonConnectSession`: On success, the created session is returned.

        Example:
            .. code-block:: python

                session = await app.create_ton_connect_session(dapp_client_id, "https://example.org/manifest.json")
        """
        r = await self.invoke(
            raw.functions.wallet.TonConnectCreateSession(
                dapp_client_id=dapp_client_id,
                manifest_url=manifest_url
            )
        )

        return types.TonConnectSession._parse(self, r)
