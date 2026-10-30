#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

import logging
from typing import Union, List

import pyrogram
from pyrogram import raw, types, utils

log = logging.getLogger(__name__)

# A media group has up to 10 messages. In channels and supergroups their ids are close together, in other chats the
# ids are shared with all the chats of the account, so messages of other chats can be in between. 99 ids are within
# the limit of 100 ids of a request.
CHANNEL_WINDOW = 9
SHARED_IDS_WINDOW = 49


class GetMediaGroup:
    async def get_media_group(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int
    ) -> List["types.Message"]:
        """Get the media group a message belongs to.

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).
                You can also use chat public link in form of *t.me/<username>* (str).

            message_id (``int``):
                The id of one of the messages that belong to the media group.
                
        Returns:
            List of :obj:`~pyrogram.types.Message`: On success, a list of messages of the media group is returned,
            sorted by id.
            
        Raises:
            ValueError: 
                In case the passed message_id is negative or equal 0. 
                In case target message doesn't belong to a media group.
        """

        if message_id <= 0:
            raise ValueError("Passed message_id is negative or equal to zero.")

        peer = await self.resolve_peer(chat_id)
        window = CHANNEL_WINDOW if isinstance(peer, raw.types.InputPeerChannel) else SHARED_IDS_WINDOW
        ids = [raw.types.InputMessageID(id=i) for i in range(max(message_id - window, 1), message_id + window + 1)]

        r = await utils.read_messages(self, peer, ids)

        media_group_id = next((getattr(msg, "grouped_id", None) for msg in r.messages if msg.id == message_id), None)

        if media_group_id is None:
            raise ValueError("The message doesn't belong to a media group")

        # Parsing can send requests, which must not depend on the messages around the media group
        r.messages = sorted(
            (msg for msg in r.messages if getattr(msg, "grouped_id", None) == media_group_id),
            key=lambda msg: msg.id
        )

        return await utils.parse_messages(self, r, replies=0, fetch_topic=False)
