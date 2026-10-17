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

from typing import Union

import pyrogram
from pyrogram import raw, types


async def parse_authorization(
    client: "pyrogram.Client",
    authorization: "raw.base.auth.Authorization"
) -> Union["types.User", "types.TermsOfService", bool]:
    """Store the session owner of a completed authorization, like sign_in does."""
    if isinstance(authorization, raw.types.auth.AuthorizationSignUpRequired):
        if authorization.terms_of_service:
            return types.TermsOfService._parse(terms_of_service=authorization.terms_of_service)

        return False

    await client.storage.user_id(authorization.user.id)
    await client.storage.is_bot(False)

    return types.User._parse(client, authorization.user)


class FinishFirebasePnvLogin:
    async def finish_firebase_pnv_login(
        self: "pyrogram.Client",
        google_token: str
    ) -> Union["types.User", "types.TermsOfService", bool]:
        """Finish a login with Firebase Phone Number Verification; for official Telegram apps only.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            google_token (``str``):
                The phone number verification token received from Google for the parameters returned by
                :meth:`~pyrogram.Client.init_firebase_pnv_login`.

        Returns:
            :obj:`~pyrogram.types.User` | :obj:`~pyrogram.types.TermsOfService` | ``bool``: In case the
            authorization completed, the user is returned. In case the phone number is not registered on Telegram,
            the Terms of Service to accept (or False if there are none) are returned: register the account with
            :meth:`~pyrogram.Client.firebase_pnv_sign_up`, then accept them with
            ``raw.functions.help.AcceptTermsOfService(id=raw.types.DataJSON(data=terms.id))``.

        Raises:
            SessionPasswordNeeded: In case a password is needed to sign in.
            PhoneMigrate | UserMigrate: In case the account is on another data center. The verification is bound to
                the data center of :meth:`~pyrogram.Client.init_firebase_pnv_login`, so the login can't simply be
                finished there: log in with :meth:`~pyrogram.Client.send_code` instead.

        Example:
            .. code-block:: python

                user = await app.finish_firebase_pnv_login(google_token)
        """
        r = await self.invoke(
            raw.functions.auth.FinishFirebasePnvLogin(google_token=google_token)
        )

        return await parse_authorization(self, r)
