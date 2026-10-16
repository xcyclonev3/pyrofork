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
from pyrogram import enums, raw, types

from ..object import Object
from .message import Str


class FormattedText(Object):
    """A text with some entities.

    Parameters:
        text (``str``):
            The text. Use its *html* or *markdown* property to get the text together with its entities.

        entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
            Special entities like bold, italic or custom emoji that appear in the text.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        text: str,
        entities: List["types.MessageEntity"] = None
    ):
        super().__init__(client)

        self.text = text
        self.entities = entities

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        text: "raw.base.TextWithEntities",
        users: Dict[int, "raw.base.User"] = None
    ) -> Optional["FormattedText"]:
        if not isinstance(text, raw.types.TextWithEntities):
            return None

        entities = types.List()

        for raw_entity in text.entities:
            entity = types.MessageEntity._parse(client, raw_entity, users or {})

            if entity is None:
                continue

            # Texts returned by the AI methods come without users: keep the identifier of a mentioned user
            if entity.type == enums.MessageEntityType.TEXT_MENTION and entity.user is None:
                entity.user = types.User(id=raw_entity.user_id, client=client)

            entities.append(entity)

        return FormattedText(
            text=Str(text.text).init(entities),
            entities=entities or None,
            client=client
        )
