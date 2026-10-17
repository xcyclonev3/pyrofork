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
from pyrogram import raw, utils
from ..object import Object


class TonConnectRequest(Object):
    """A request received from a dApp through TON Connect.

    Each request is delivered as a service message in the chat with Telegram's service notifications (777000).
    Pending requests are returned by :meth:`~pyrogram.Client.get_ton_connect_session_pending_requests` and
    :meth:`~pyrogram.Client.get_ton_connect_dapp_pending_requests`; *dapp_name*, *is_accepted* and *is_rejected* are
    only available in service messages, *body* only in pending requests.

    Parameters:
        session_id (``int``):
            Unique identifier of the TON Connect session.

        message_id (``int``, *optional*):
            Identifier of the service message with the request.

        body (``bytes``, *optional*):
            Body of the request.

        expiration_date (:py:obj:`~datetime.datetime`, *optional*):
            Date the request expires. None, if it never expires.

        topic (``str``, *optional*):
            Topic of the request.

        trace_id (``str``, *optional*):
            Identifier to trace the request.

        dapp_name (``str``, *optional*):
            Name of the dApp, if known.

        is_accepted (``bool``, *optional*):
            True, if the request was accepted.

        is_rejected (``bool``, *optional*):
            True, if the request was rejected.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        session_id: int,
        message_id: Optional[int] = None,
        body: Optional[bytes] = None,
        expiration_date: Optional[datetime] = None,
        topic: Optional[str] = None,
        trace_id: Optional[str] = None,
        dapp_name: Optional[str] = None,
        is_accepted: Optional[bool] = None,
        is_rejected: Optional[bool] = None
    ):
        super().__init__(client)

        self.session_id = session_id
        self.message_id = message_id
        self.body = body
        self.expiration_date = expiration_date
        self.topic = topic
        self.trace_id = trace_id
        self.dapp_name = dapp_name
        self.is_accepted = is_accepted
        self.is_rejected = is_rejected

    @staticmethod
    def _parse(client, request: "raw.types.TonConnectRequest") -> "TonConnectRequest":
        return TonConnectRequest(
            session_id=request.session_id,
            message_id=request.msg_id,
            body=request.body,
            expiration_date=utils.timestamp_to_datetime(request.expires),
            topic=request.topic,
            trace_id=request.trace_id,
            client=client
        )

    @staticmethod
    def _parse_action(
        client,
        action: "raw.types.MessageActionWalletTonConnectRequest",
        message_id: Optional[int] = None
    ) -> "TonConnectRequest":
        return TonConnectRequest(
            session_id=action.session_id,
            message_id=message_id,
            expiration_date=utils.timestamp_to_datetime(action.expires),
            topic=action.topic,
            trace_id=action.trace_id,
            dapp_name=action.dapp_name,
            is_accepted=action.accepted,
            is_rejected=action.declined,
            client=client
        )
