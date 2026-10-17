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

import pyrogram
from pyrogram import raw
from ..object import Object


class ChatHasProtectedContentDisableRequested(Object):
    """A service message with a request to disable the content protection of the chat.

    Use :meth:`~pyrogram.Client.process_chat_protected_content_disable_request` to approve or reject it.

    Parameters:
        is_expired (``bool``):
            True, if the request has expired.
    """

    def __init__(self, *, client: "pyrogram.Client" = None, is_expired: bool):
        super().__init__(client)

        self.is_expired = is_expired

    @staticmethod
    def _parse(
        client,
        action: "raw.types.MessageActionNoForwardsRequest"
    ) -> Optional["ChatHasProtectedContentDisableRequested"]:
        # Like TDLib, only requests to disable the protection are supported
        if not action.prev_value or action.new_value:
            return None

        return ChatHasProtectedContentDisableRequested(is_expired=action.expired, client=client)
