#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
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
from pyrogram import raw, types, utils
from ..object import Object
from typing import Dict, List, Optional


class PollOption(Object):
    """Contains information about one answer option in a poll.

    Parameters:
        text (``str``):
            Option text, 1-100 characters.

        voter_count (``int``, *optional*):
            Number of users that voted for this option.
            Equals to 0 until you vote.

        data (``bytes``, *optional*):
            The data this poll option is holding.

        entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
            Special entities like usernames, URLs, bot commands, etc. that appear in the option text.

        media (:obj:`~pyrogram.types.InputMediaPhoto` | :obj:`~pyrogram.types.InputMediaVideo` | :obj:`~pyrogram.types.InputMediaAnimation` | :obj:`~pyrogram.types.InputMediaAudio` | :obj:`~pyrogram.types.InputMediaDocument`, *optional*):
            Media of the option. When sending, an input media; in received polls, the media object
            (:obj:`~pyrogram.types.Photo`, :obj:`~pyrogram.types.Video`, ...).

        author (:obj:`~pyrogram.types.Chat`, *optional*):
            The user or chat that added the option, for options added after the poll was created.

        addition_date (:py:obj:`~datetime.datetime`, *optional*):
            Date the option was added, for options added after the poll was created.
    """

    def __init__(
        self: "pyrogram.Client",
        text: str,
        voter_count: int = 0,
        data: bytes = None,
        entities: Optional[List["pyrogram.types.MessageEntity"]] = None,
        media=None,
        author: "pyrogram.types.Chat" = None,
        addition_date: "datetime" = None
    ):
        super().__init__(self)

        self.text = text
        self.voter_count = voter_count
        self.data = data
        self.entities = entities
        self.media = media
        self.author = author
        self.addition_date = addition_date

    @staticmethod
    async def _parse(
        client,
        answer: "raw.types.PollAnswer",
        users: Dict[int, "raw.base.User"] = None,
        chats: Dict[int, "raw.base.Chat"] = None,
        voter_count: int = 0
    ) -> "PollOption":
        entities = types.List(
            filter(lambda x: x is not None, [types.MessageEntity._parse(client, e, {}) for e in answer.text.entities])
        )

        added_by_id = utils.get_raw_peer_id(getattr(answer, "added_by", None))
        added_by = (users or {}).get(added_by_id) or (chats or {}).get(added_by_id)

        return PollOption(
            text=answer.text.text,
            voter_count=voter_count,
            data=getattr(answer, "option", None),
            entities=entities,
            media=await types.Poll._parse_media(client, getattr(answer, "media", None)),
            author=types.Chat._parse_chat(client, added_by) if added_by else None,
            addition_date=utils.timestamp_to_datetime(getattr(answer, "date", None))
        )

    async def write(self, client, i):
        # Since Layer 230 the server assigns the option bytes: official clients send inputPollAnswer
        # (tdesktop PollDataToMTP, TDLib PollOption::get_input_poll_answer). "i" is kept for compatibility.
        option, entities = (await pyrogram.utils.parse_text_entities(client, self.text, None, self.entities)).values()
        return pyrogram.raw.types.InputPollAnswer(
            text=pyrogram.raw.types.TextWithEntities(
                text=option,
                entities=entities or []
            )
        )
