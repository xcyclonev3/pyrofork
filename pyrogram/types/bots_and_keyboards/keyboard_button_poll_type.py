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

from typing import Optional

from pyrogram import enums
from ..object import Object


class KeyboardButtonPollType(Object):
    """The type of a poll that can be created and sent when the corresponding button is pressed.

    Parameters:
        type (:obj:`~pyrogram.enums.PollType`, *optional*):
            If :obj:`~pyrogram.enums.PollType.QUIZ` is passed, the user will be allowed to create only polls in the
            quiz mode. If :obj:`~pyrogram.enums.PollType.REGULAR` is passed, only regular polls will be allowed.
            Otherwise, the user will be allowed to create a poll of any type.
    """

    def __init__(self, type: Optional["enums.PollType"] = None):
        super().__init__()

        self.type = type
