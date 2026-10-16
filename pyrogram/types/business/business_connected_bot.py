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
from typing import Dict

import pyrogram
from pyrogram import raw, types, utils

from ..object import Object
from ..update import Update


class BusinessConnectedBot(Object, Update):
    """A business bot connected to the current user's account.

    A bot connected from another device must be confirmed with
    :meth:`~pyrogram.Client.confirm_business_connected_bot`.

    Parameters:
        bot (:obj:`~pyrogram.types.User`):
            The connected bot.

        is_confirmed (``bool``, *optional*):
            True, if the connection was confirmed.

        connection_date (:py:obj:`~datetime.datetime`, *optional*):
            Date when the bot was connected.

        device_model (``str``, *optional*):
            Model of the device that was used to connect the bot, as provided by the application.

        location (``str``, *optional*):
            A human-readable description of the location from which the bot was connected, based on the IP address.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        bot: "types.User",
        is_confirmed: bool = None,
        connection_date: datetime = None,
        device_model: str = None,
        location: str = None
    ):
        super().__init__(client)

        self.bot = bot
        self.is_confirmed = is_confirmed
        self.connection_date = connection_date
        self.device_model = device_model
        self.location = location

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        update: "raw.types.UpdateNewBotConnection",
        users: Dict[int, "raw.types.User"]
    ) -> "BusinessConnectedBot":
        return BusinessConnectedBot(
            bot=types.User._parse(client, users.get(update.bot_id)),
            is_confirmed=update.confirmed,
            connection_date=utils.timestamp_to_datetime(update.date),
            device_model=update.device,
            location=update.location,
            client=client
        )
