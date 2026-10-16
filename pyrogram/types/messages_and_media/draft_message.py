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
from typing import Dict, List, Optional

import pyrogram
from pyrogram import raw, types, utils
from ..object import Object
from .message import Str


class DraftMessage(Object):
    """A message draft.

    Parameters:
        date (:py:obj:`~datetime.datetime`):
            Date the draft was created.

        text (``str``, *optional*):
            Text of the draft; empty for rich message drafts.

        entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
            Special entities like URLs, usernames, bold text, ... that appear in the text.

        rich_message (:obj:`~pyrogram.types.RichMessage`, *optional*):
            The rich message of a rich message draft.

        reply_to_message_id (``int``, *optional*):
            Identifier of the message to be replied.

        message_thread_id (``int``, *optional*):
            Identifier of the forum topic of the draft.

        disable_web_page_preview (``bool``, *optional*):
            True, if link previews are disabled.

        invert_media (``bool``, *optional*):
            True, if the link preview must be shown above the text.

        effect_id (``int``, *optional*):
            Identifier of the effect to apply to the message when it is sent.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        date: datetime,
        text: str = None,
        entities: List["types.MessageEntity"] = None,
        rich_message: "types.RichMessage" = None,
        reply_to_message_id: int = None,
        message_thread_id: int = None,
        disable_web_page_preview: bool = None,
        invert_media: bool = None,
        effect_id: int = None
    ):
        super().__init__(client)

        self.date = date
        self.text = text
        self.entities = entities
        self.rich_message = rich_message
        self.reply_to_message_id = reply_to_message_id
        self.message_thread_id = message_thread_id
        self.disable_web_page_preview = disable_web_page_preview
        self.invert_media = invert_media
        self.effect_id = effect_id

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        draft: "raw.base.DraftMessage",
        users: Dict[int, "raw.base.User"] = None
    ) -> Optional["DraftMessage"]:
        if not isinstance(draft, raw.types.DraftMessage):
            return None

        entities = types.List(filter(None, [
            types.MessageEntity._parse(client, entity, users or {}) for entity in draft.entities or []
        ]))
        reply_to = draft.reply_to if isinstance(draft.reply_to, raw.types.InputReplyToMessage) else None

        return DraftMessage(
            client=client,
            date=utils.timestamp_to_datetime(draft.date),
            text=Str(draft.message).init(entities) or None,
            entities=entities or None,
            rich_message=types.RichMessage._parse(client, draft.rich_message),
            reply_to_message_id=reply_to.reply_to_msg_id if reply_to else None,
            message_thread_id=reply_to.top_msg_id if reply_to else None,
            disable_web_page_preview=draft.no_webpage,
            invert_media=draft.invert_media,
            effect_id=draft.effect
        )
