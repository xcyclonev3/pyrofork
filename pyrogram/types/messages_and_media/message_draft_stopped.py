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

from typing import Dict, Optional

import pyrogram
from pyrogram import raw, types
from ..object import Object


class MessageDraftStopped(Object):
    """The generation of a message draft was stopped by the user (TDLib's *updateStopMessageDraft*).

    Received by the sender of the drafts (usually a bot) when the user presses the stop button of a draft sent with
    *can_stop* (see :meth:`~pyrogram.Client.send_text_message_draft` and
    :meth:`~pyrogram.Client.stop_pending_message`): no more drafts with the same *draft_id* should be sent.

    Parameters:
        chat (:obj:`~pyrogram.types.Chat`):
            The private chat with the user who stopped the generation.

        draft_id (``int``):
            Identifier of the message draft within the message thread.

        message_thread_id (``int``, *optional*):
            Identifier of the forum topic of the message draft.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        chat: "types.Chat",
        draft_id: int,
        message_thread_id: Optional[int] = None
    ):
        super().__init__(client)

        self.chat = chat
        self.draft_id = draft_id
        self.message_thread_id = message_thread_id

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        update: "raw.types.UpdateUserTyping",
        users: Dict[int, "raw.base.User"]
    ) -> Optional["MessageDraftStopped"]:
        action = update.action

        # A zero draft identifier is invalid: TDLib handles such an action as a cancelled chat action
        if not isinstance(action, raw.types.SendMessageStopDraftAction) or not action.random_id:
            return None

        return MessageDraftStopped(
            chat=types.PendingMessage._parse_chat(client, update.user_id, users),
            draft_id=action.random_id,
            message_thread_id=update.top_msg_id,
            client=client
        )
