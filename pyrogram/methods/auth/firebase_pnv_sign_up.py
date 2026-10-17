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
from pyrogram import raw, types
from .finish_firebase_pnv_login import parse_authorization


class FirebasePnvSignUp:
    async def firebase_pnv_sign_up(
        self: "pyrogram.Client",
        first_name: str,
        last_name: str = "",
        disable_notification: Optional[bool] = None
    ) -> "types.User":
        """Register a new Telegram account with the phone number verified with Firebase Phone Number Verification;
        for official Telegram apps only.

        Use it when :meth:`~pyrogram.Client.finish_firebase_pnv_login` reports that the phone number is not
        registered on Telegram.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            first_name (``str``):
                The first name of the user; 1-64 characters.

            last_name (``str``, *optional*):
                The last name of the user; 0-64 characters.

            disable_notification (``bool``, *optional*):
                Pass True to disable the notification about the user joining Telegram for other users that added them
                to their contact list.

        Returns:
            :obj:`~pyrogram.types.User`: On success, the new user is returned.

        Example:
            .. code-block:: python

                user = await app.firebase_pnv_sign_up("Pyrofork", "User")
        """
        r = await self.invoke(
            raw.functions.auth.FirebasePnvSignUp(
                first_name=first_name,
                last_name=last_name,
                no_joined_notifications=disable_notification
            )
        )

        return await parse_authorization(self, r)
