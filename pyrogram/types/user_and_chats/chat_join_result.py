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

from typing import Dict

import pyrogram
from pyrogram import enums, raw, types
from ..object import Object
from ..update import Update


class ChatJoinResult(Object, Update):
    """The decision of a guard bot about a join request of the current user.

    It follows a :obj:`~pyrogram.errors.GuardBotApprovalRequired` raised by :meth:`~pyrogram.Client.join_chat`.

    Parameters:
        chat (:obj:`~pyrogram.types.Chat`):
            The chat the user asked to join.

        query_id (``int``):
            Identifier of the join request, as in :obj:`~pyrogram.errors.GuardBotApprovalRequired`.

        type (:obj:`~pyrogram.enums.ChatJoinResultType`):
            The decision.

        url (``str``, *optional*):
            URL of the Web App to open, for :obj:`~pyrogram.enums.ChatJoinResultType.WEB_APP` only.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        chat: "types.Chat",
        query_id: int,
        type: "enums.ChatJoinResultType",
        url: str = None
    ):
        super().__init__(client)

        self.chat = chat
        self.query_id = query_id
        self.type = type
        self.url = url

    @staticmethod
    def _parse(
        client,
        update: "raw.types.UpdateJoinChatWebViewDecision",
        users: Dict[int, "raw.base.User"],
        chats: Dict[int, "raw.base.Chat"]
    ) -> "ChatJoinResult":
        result = update.result

        if isinstance(result, raw.types.JoinChatBotResultApproved):
            result_type = enums.ChatJoinResultType.APPROVED
        elif isinstance(result, raw.types.JoinChatBotResultDeclined):
            result_type = enums.ChatJoinResultType.DECLINED
        elif isinstance(result, raw.types.JoinChatBotResultQueued):
            result_type = enums.ChatJoinResultType.QUEUED
        else:
            result_type = enums.ChatJoinResultType.WEB_APP

        return ChatJoinResult(
            chat=types.Chat._parse_chat(client, chats.get(pyrogram.utils.get_raw_peer_id(update.peer))),
            query_id=update.query_id,
            type=result_type,
            url=getattr(result, "url", None),
            client=client
        )
