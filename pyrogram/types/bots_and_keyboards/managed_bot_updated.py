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
from ..update import Update


class ManagedBotUpdated(Object, Update):
    """A bot managed by the current bot was created or updated.

    The current bot can get the token of the managed bot with :meth:`~pyrogram.Client.get_managed_bot_token`.

    Parameters:
        user (:obj:`~pyrogram.types.User`):
            The user who created the bot and owns it.

        bot (:obj:`~pyrogram.types.User`):
            The managed bot.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        user: "types.User",
        bot: "types.User"
    ):
        super().__init__(client)

        self.user = user
        self.bot = bot

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        update: "raw.types.UpdateManagedBot",
        users: Dict[int, "raw.types.User"]
    ) -> "ManagedBotUpdated":
        return ManagedBotUpdated(
            user=types.User._parse(client, users.get(update.user_id)) or types.User(id=update.user_id, client=client),
            bot=types.User._parse(client, users.get(update.bot_id)) or types.User(id=update.bot_id, client=client),
            client=client
        )
