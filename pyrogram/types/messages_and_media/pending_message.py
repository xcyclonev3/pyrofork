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

from typing import Dict, List, Optional

import pyrogram
from pyrogram import enums, raw, types, utils
from ..object import Object


class PendingMessage(Object):
    """A live draft of a message being generated, received in a private chat (usually from an AI bot).

    The draft must be shown in the chat for at most *message_typing_draft_ttl* seconds (see the client
    configuration), must replace any other pending message with the same *draft_id* in the same message thread, and
    must be deleted when any incoming message or a pending message with another *draft_id* is received in the message
    thread. Drafts are sent with :meth:`~pyrogram.Client.send_text_message_draft`.

    Parameters:
        chat (:obj:`~pyrogram.types.Chat`):
            The private chat the draft was received in.

        draft_id (``int``):
            Unique identifier of the draft within the message thread.

        text (``str``):
            The text of the draft; empty while a "Thinking..." placeholder must be shown.

        entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
            Special entities like usernames, URLs, bot commands, etc. that appear in the text.

        message_thread_id (``int``, *optional*):
            Identifier of the forum topic of the chat with the bot in which the message will be sent.

        can_stop (``bool``, *optional*):
            True, if a button that calls :meth:`~pyrogram.Client.stop_pending_message` to stop further generation of
            the message must be shown.

        keep_on_stop (``bool``, *optional*):
            True, if the draft must not be automatically deleted when the user stops the generation.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        chat: "types.Chat",
        draft_id: int,
        text: str,
        entities: Optional[List["types.MessageEntity"]] = None,
        message_thread_id: Optional[int] = None,
        can_stop: Optional[bool] = None,
        keep_on_stop: Optional[bool] = None
    ):
        super().__init__(client)

        self.chat = chat
        self.draft_id = draft_id
        self.text = text
        self.entities = entities
        self.message_thread_id = message_thread_id
        self.can_stop = can_stop
        self.keep_on_stop = keep_on_stop

    @staticmethod
    def _parse_chat(client: "pyrogram.Client", user_id: int, users: Dict[int, "raw.base.User"]) -> "types.Chat":
        return (
            types.Chat._parse_user_chat(client, users.get(user_id))
            or types.Chat(id=user_id, type=enums.ChatType.PRIVATE, client=client)
        )

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        update: "raw.types.UpdateUserTyping",
        users: Dict[int, "raw.base.User"]
    ) -> Optional["PendingMessage"]:
        action = update.action

        if not isinstance(action, raw.types.SendMessageTextDraftAction):
            return None

        text = utils.parse_text_with_entities(client, action.text, users)

        return PendingMessage(
            chat=PendingMessage._parse_chat(client, update.user_id, users),
            draft_id=action.random_id,
            text=text["text"] or "",
            entities=text["entities"],
            message_thread_id=update.top_msg_id,
            can_stop=action.can_stop,
            keep_on_stop=action.keep_on_stop,
            client=client
        )
