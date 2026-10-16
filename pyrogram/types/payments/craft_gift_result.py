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

import re
from typing import Optional

import pyrogram
from pyrogram import enums, raw, types
from ..object import Object


class CraftGiftResult(Object):
    """Contains the result of gift crafting.

    Parameters:
        type (:obj:`~pyrogram.enums.CraftGiftResultType`):
            Type of the result.

        gift (:obj:`~pyrogram.types.Gift`, *optional*):
            The crafted gift.
            For :obj:`~pyrogram.enums.CraftGiftResultType.SUCCESS` results only.

        owned_gift_id (``str``, *optional*):
            Unique identifier of the crafted gift for the current user, as accepted by the methods that manage owned
            gifts (e.g. :meth:`~pyrogram.Client.set_gift_resale_price`).
            For :obj:`~pyrogram.enums.CraftGiftResultType.SUCCESS` results only.

        retry_after (``int``, *optional*):
            Time left before the gift can be used for crafting, in seconds.
            For :obj:`~pyrogram.enums.CraftGiftResultType.TOO_EARLY` results only.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        type: "enums.CraftGiftResultType",
        gift: Optional["types.Gift"] = None,
        owned_gift_id: Optional[str] = None,
        retry_after: Optional[int] = None
    ):
        super().__init__(client)

        self.type = type
        self.gift = gift
        self.owned_gift_id = owned_gift_id
        self.retry_after = retry_after

    @staticmethod
    async def _parse(client, updates: "raw.base.Updates") -> Optional["CraftGiftResult"]:
        # Like TDLib: a failed craft is reported by updateStarGiftCraftFail, a successful one by a new
        # messageActionStarGiftUnique service message whose id identifies the crafted gift.
        if isinstance(updates, raw.types.UpdateShort):
            update_list = [updates.update]
        else:
            update_list = getattr(updates, "updates", None) or []

        if any(isinstance(update, raw.types.UpdateStarGiftCraftFail) for update in update_list):
            return CraftGiftResult(type=enums.CraftGiftResultType.FAIL, client=client)

        users = {i.id: i for i in getattr(updates, "users", [])}
        chats = {i.id: i for i in getattr(updates, "chats", [])}

        for update in update_list:
            if not isinstance(update, (raw.types.UpdateNewMessage, raw.types.UpdateNewChannelMessage)):
                continue

            message = update.message

            if (
                isinstance(message, raw.types.MessageService)
                and isinstance(message.action, raw.types.MessageActionStarGiftUnique)
            ):
                return CraftGiftResult(
                    type=enums.CraftGiftResultType.SUCCESS,
                    gift=await types.Gift._parse_action(client, message, users, chats),
                    owned_gift_id=str(message.id),
                    client=client
                )

        return None

    @staticmethod
    def _parse_error(client, error: "pyrogram.errors.RPCError") -> Optional["CraftGiftResult"]:
        # Like TDLib, errors about gifts that can't be used for crafting are results.
        if error.ID:
            error_id, value = error.ID, error.value
        else:  # An error unknown to Pyrofork, its value is "[CODE MESSAGE]"
            message = str(error.value).strip("[]").split(" ", 1)[-1]
            number = re.search(r"_(\d+)", message)
            error_id = re.sub(r"_\d+", "_X", message)
            value = int(number.group(1)) if number else None

        if error_id == "STARGIFT_CRAFT_TOO_EARLY_X":
            return CraftGiftResult(type=enums.CraftGiftResultType.TOO_EARLY, retry_after=value, client=client)

        if error_id in ("STARGIFT_CRAFT_UNAVAILABLE", "STARGIFT_ALREADY_CRAFTED"):
            return CraftGiftResult(type=enums.CraftGiftResultType.INVALID_GIFT, client=client)

        return None
