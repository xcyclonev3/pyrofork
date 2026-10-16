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


class SetTonConnectSessionWalletClientId:
    async def set_ton_connect_session_wallet_client_id(
        self: "pyrogram.Client",
        session_id: int,
        wallet_client_id: str
    ) -> "types.TonConnectChallenge":
        """Set the key of the application of the current user for a pending TON Connect session.

        Then, use :meth:`~pyrogram.Client.send_ton_connect_session_connect_result` to accept or reject the session.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            session_id (``int``):
                Unique identifier of the session.

            wallet_client_id (``str``):
                Hex-encoded public X25519 key of the application of the current user, calculated from the private
                key of the TON wallet and the session parameters by your own code.

        Returns:
            :obj:`~pyrogram.types.TonConnectChallenge`: On success, the challenge to answer is returned.

        Example:
            .. code-block:: python

                challenge = await app.set_ton_connect_session_wallet_client_id(session.id, wallet_client_id)
        """
        r = await self.invoke(
            raw.functions.wallet.TonConnectRegisterKey(
                session_id=session_id,
                client_id=wallet_client_id
            )
        )

        return types.TonConnectChallenge._parse(self, r)
