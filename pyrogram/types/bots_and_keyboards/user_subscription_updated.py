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


from typing import Dict, Union

import pyrogram
from pyrogram import raw, types
from ..object import Object
from ..update import Update


class UserSubscriptionUpdated(Object, Update):
    """The subscription of a user to the bot changed.

    Exactly one of *is_canceled*, *is_restored* and *is_payment_failed* is True.

    Parameters:
        user (:obj:`~pyrogram.types.User`):
            The subscribed user.

        payload (``str`` | ``bytes``):
            The payload of the invoice of the subscription, as specified by the bot.

        is_canceled (``bool``):
            True, if the subscription was canceled.

        is_restored (``bool``):
            True, if the subscription was restored.

        is_payment_failed (``bool``):
            True, if the payment for the subscription failed.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        user: "types.User",
        payload: Union[str, bytes],
        is_canceled: bool,
        is_restored: bool,
        is_payment_failed: bool
    ):
        super().__init__(client)

        self.user = user
        self.payload = payload
        self.is_canceled = is_canceled
        self.is_restored = is_restored
        self.is_payment_failed = is_payment_failed

    @staticmethod
    def _parse(
        client,
        update: "raw.types.UpdateBotStarsSubscription",
        users: Dict[int, "raw.base.User"]
    ) -> "UserSubscriptionUpdated":
        # Like the data of callback queries, keep the payload as bytes if it isn't text
        try:
            payload = update.payload.decode()
        except UnicodeDecodeError:
            payload = update.payload

        return UserSubscriptionUpdated(
            user=types.User._parse(client, users.get(update.user_id)) or types.User(id=update.user_id, client=client),
            payload=payload,
            is_canceled=update.canceled,
            is_restored=update.restored,
            is_payment_failed=update.payment_failed,
            client=client
        )
