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


class ChatHasProtectedContentToggled(Object):
    """A service message about the change of the content protection of the chat, or about a rejected request to
    disable it.

    Parameters:
        request_message_id (``int``, *optional*):
            Identifier of the message with the request to change the setting, if any.
            The message can be already deleted.

        old_has_protected_content (``bool``):
            Previous value of the setting.

        new_has_protected_content (``bool``):
            New value of the setting.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        request_message_id: Optional[int] = None,
        old_has_protected_content: bool,
        new_has_protected_content: bool
    ):
        super().__init__(client)

        self.request_message_id = request_message_id
        self.old_has_protected_content = old_has_protected_content
        self.new_has_protected_content = new_has_protected_content

    @staticmethod
    def _parse(
        client,
        action: "raw.types.MessageActionNoForwardsToggle",
        request_message_id: Optional[int] = None
    ) -> "ChatHasProtectedContentToggled":
        return ChatHasProtectedContentToggled(
            request_message_id=request_message_id,
            old_has_protected_content=action.prev_value,
            new_has_protected_content=action.new_value,
            client=client
        )
