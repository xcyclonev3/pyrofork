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

from typing import Union

import pyrogram
from pyrogram import raw


class SetChatMemberTag:
    async def set_chat_member_tag(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        user_id: Union[int, str],
        tag: str
    ) -> bool:
        """Change the tag of a chat member; for basic groups and supergroups only.

        Changing the tag of other members requires the *can_manage_tags* administrator privilege.
        For administrators, the tag is their custom title.

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            user_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the member whose tag is changed.

            tag (``str``):
                The new tag, 0-16 characters without emoji; pass an empty string to remove it.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                await app.set_chat_member_tag(chat_id, user_id, "Designer")
        """
        await self.invoke(
            raw.functions.messages.EditChatParticipantRank(
                peer=await self.resolve_peer(chat_id),
                participant=await self.resolve_peer(user_id),
                rank=tag
            )
        )

        return True
