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

from datetime import datetime
from typing import Optional

import pyrogram
from pyrogram import enums, raw, types, utils
from ..object import Object


class TonConnectSession(Object):
    """A TON Connect session between the TON wallet of the current user and a dApp.

    See https://github.com/ton-blockchain/ton-connect/tree/main/spec for the TON Connect protocol.

    Parameters:
        id (``int``):
            Unique identifier of the session.

        dapp_client_id (``str``):
            Hex-encoded public X25519 key of the dApp.

        state (:obj:`~pyrogram.enums.TonConnectSessionState`):
            State of the session.

        creation_date (:py:obj:`~datetime.datetime`):
            Date the session was created.

        nonce (``bytes``):
            Nonce used for the session.

        wallet_client_id (``str``, *optional*):
            Hex-encoded public X25519 key of the application of the current user, if already set.

        manifest (:obj:`~pyrogram.types.TonConnectManifest`, *optional*):
            Manifest of the dApp, once downloaded.

        manifest_error (``int``, *optional*):
            Error code of the download of the manifest, if it failed. TDLib reports 3 as an invalid manifest and the
            other codes as a failed download. While neither *manifest* nor *manifest_error* is set, the manifest is
            being downloaded.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        id: int,
        dapp_client_id: str,
        state: "enums.TonConnectSessionState",
        creation_date: datetime,
        nonce: bytes,
        wallet_client_id: Optional[str] = None,
        manifest: Optional["types.TonConnectManifest"] = None,
        manifest_error: Optional[int] = None
    ):
        super().__init__(client)

        self.id = id
        self.dapp_client_id = dapp_client_id
        self.state = state
        self.creation_date = creation_date
        self.nonce = nonce
        self.wallet_client_id = wallet_client_id
        self.manifest = manifest
        self.manifest_error = manifest_error

    @staticmethod
    def _parse(client, session: "raw.types.TonConnectSession") -> "TonConnectSession":
        # Same precedence as TDLib's TonConnectSession::get_ton_connect_session_object()
        if session.pending:
            state = enums.TonConnectSessionState.PENDING
        elif session.closing:
            state = enums.TonConnectSessionState.CLOSING
        elif session.closed:
            state = enums.TonConnectSessionState.CLOSED
        else:
            state = enums.TonConnectSessionState.READY

        return TonConnectSession(
            id=session.id,
            dapp_client_id=session.dapp_client_id,
            state=state,
            creation_date=utils.timestamp_to_datetime(session.date),
            nonce=session.nonce,
            wallet_client_id=session.client_id,
            manifest=types.TonConnectManifest._parse(client, session.manifest),
            manifest_error=session.manifest_error or None,
            client=client
        )
