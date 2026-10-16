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

from typing import Optional, Union

import pyrogram
from pyrogram import raw, types


class GetChatOwnerAfterLeaving:
    async def get_chat_owner_after_leaving(
        self: "pyrogram.Client",
        chat_id: Union[int, str]
    ) -> Optional["types.User"]:
        """Get the user who will become the owner of a chat if the current user leaves it.

        For supergroups and channels the ownership is transferred after 7 days if the current user doesn't return to
        the chat during that period; for basic groups it is transferred immediately.

        .. note::

            Requires owner privileges in the chat.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target basic group, supergroup or channel.

        Returns:
            :obj:`~pyrogram.types.User` | ``None``: On success, the future owner of the chat is returned, or None if
            there is no user who can become the owner.

        Raises:
            ValueError: In case the chat is not a basic group, supergroup or channel.

        Example:
            .. code-block:: python

                user = await app.get_chat_owner_after_leaving(chat_id)
                print(user.first_name)
        """
        peer = await self.resolve_peer(chat_id)

        if not isinstance(peer, (raw.types.InputPeerChannel, raw.types.InputPeerChat)):
            raise ValueError("The chat_id must belong to a basic group, supergroup or channel.")

        r = await self.invoke(
            raw.functions.messages.GetFutureChatCreatorAfterLeave(peer=peer)
        )

        if isinstance(r, raw.types.User):
            await self.fetch_peers([r])

        return types.User._parse(self, r)
