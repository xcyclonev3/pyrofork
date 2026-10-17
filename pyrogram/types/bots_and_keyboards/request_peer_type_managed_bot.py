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

from ..object import Object


class RequestPeerTypeManagedBot(Object):
    """Object used to request clients to create a new bot managed by the current bot.

    Parameters:
        button_id (``int``, *optional*):
            Button identifier.

        suggested_name (``str``, *optional*):
            Suggested name for the bot.

        suggested_username (``str``, *optional*):
            Suggested username for the bot.
    """

    def __init__(
        self,
        button_id: int = 0,
        suggested_name: str = None,
        suggested_username: str = None
    ):
        super().__init__()

        self.button_id = button_id
        self.suggested_name = suggested_name
        self.suggested_username = suggested_username
