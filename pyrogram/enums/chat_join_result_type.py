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

from enum import auto

from .auto_name import AutoName


class ChatJoinResultType(AutoName):
    """Chat join result type enumeration used in :obj:`~pyrogram.types.ChatJoinResult`."""

    APPROVED = auto()
    "The guard bot approved the join request"

    DECLINED = auto()
    "The guard bot declined the join request"

    QUEUED = auto()
    "The join request was passed on to the chat administrators"

    WEB_APP = auto()
    "The user must continue in a Web App of the guard bot (see ``url``)"
