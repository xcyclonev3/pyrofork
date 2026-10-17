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
from pyrogram.types.rich_messages.input_rich_message import get_input_user


class SetChatJoinByRequest:
    async def set_chat_join_by_request(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        enabled: bool,
        guard_bot_id: Union[int, str] = None,
        apply_to_invite_links: bool = None
    ) -> bool:
        """Toggle whether the users who join a supergroup directly must be approved first.

        .. include:: /_includes/usable-by/users-bots.rst

        You must be an administrator of the supergroup with the *can_restrict_members* privilege.

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target supergroup.

            enabled (``bool``):
                Pass True to require the approval of join requests, False to let users join directly.

            guard_bot_id (``int`` | ``str``, *optional*):
                Unique identifier (int) or username (str) of the guard bot, which will approve the join requests
                (see :obj:`~pyrogram.types.ChatJoinRequest`). The bot must be a guard bot (see *is_guard_bot* of
                :obj:`~pyrogram.types.User`) and an administrator of the supergroup with the *can_invite_users*
                privilege. Ignored if *enabled* is False.

            apply_to_invite_links (``bool``, *optional*):
                Pass True to apply the change to the existing invite links too, including the primary link.

        Returns:
            ``bool``: True on success.

        Example:
            .. code-block:: python

                # Let a guard bot approve the new members
                await app.set_chat_join_by_request(chat_id, True, guard_bot_id="GuardBot")
        """
        guard_bot = None

        if enabled and guard_bot_id is not None:
            guard_bot = get_input_user(await self.resolve_peer(guard_bot_id))

        await self.invoke(
            raw.functions.channels.ToggleJoinRequest(
                channel=await self.resolve_peer(chat_id),
                enabled=enabled,
                guard_bot=guard_bot,
                apply_to_invites=apply_to_invite_links
            )
        )

        return True
