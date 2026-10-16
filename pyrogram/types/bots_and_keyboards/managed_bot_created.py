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

from typing import Dict

import pyrogram
from pyrogram import raw, types

from ..object import Object


class ManagedBotCreated(Object):
    """A service message about a bot created by the user and managed by the bot of the chat.

    Parameters:
        bot (:obj:`~pyrogram.types.User`):
            The created bot.

        manager_bot (:obj:`~pyrogram.types.User`, *optional*):
            The bot which manages the created bot.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        bot: "types.User",
        manager_bot: "types.User" = None
    ):
        super().__init__(client)

        self.bot = bot
        self.manager_bot = manager_bot

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        action: "raw.types.MessageActionManagedBotCreated",
        users: Dict[int, "raw.types.User"],
        manager_bot: "types.User" = None
    ) -> "ManagedBotCreated":
        # The action only names the created bot. As in TDLib, the manager bot is the bot of the private chat:
        # the chat partner for messages sent by the user, the current bot for messages it receives.
        return ManagedBotCreated(
            bot=types.User._parse(client, users.get(action.bot_id)),
            manager_bot=manager_bot,
            client=client
        )
