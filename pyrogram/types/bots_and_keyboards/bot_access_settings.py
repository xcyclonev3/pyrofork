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

from typing import List

import pyrogram
from pyrogram import raw, types

from ..object import Object


class BotAccessSettings(Object):
    """Describes the users that have access to a managed bot.

    Parameters:
        is_restricted (``bool``):
            True, if access to the bot is restricted to its owner and the users in *added_users*.

        added_users (List of :obj:`~pyrogram.types.User`, *optional*):
            Users who can use the bot in addition to its owner.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        is_restricted: bool,
        added_users: List["types.User"] = None
    ):
        super().__init__(client)

        self.is_restricted = is_restricted
        self.added_users = added_users

    @staticmethod
    def _parse(client: "pyrogram.Client", settings: "raw.types.bots.AccessSettings") -> "BotAccessSettings":
        added_users = types.List(types.User._parse(client, user) for user in settings.add_users or [])

        return BotAccessSettings(
            is_restricted=bool(settings.restricted),
            added_users=added_users or None,
            client=client
        )
