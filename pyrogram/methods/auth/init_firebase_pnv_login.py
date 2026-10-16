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

from typing import List, Optional

import pyrogram
from pyrogram import raw, types


class InitFirebasePnvLogin:
    async def init_firebase_pnv_login(
        self: "pyrogram.Client",
        except_ids: Optional[List[int]] = None
    ) -> "types.FirebasePnvIntent":
        """Start a login with Firebase Phone Number Verification; for official Telegram apps only.

        Use the returned parameters to verify the phone number with Firebase, then finish the login with
        :meth:`~pyrogram.Client.finish_firebase_pnv_login`.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            except_ids (List of ``int``, *optional*):
                Identifiers of the users already logged in on the device, to prevent logging in twice with the same
                user.

        Returns:
            :obj:`~pyrogram.types.FirebasePnvIntent`: On success, the parameters of the phone number verification
            are returned.

        Example:
            .. code-block:: python

                intent = await app.init_firebase_pnv_login()
        """
        r = await self.invoke(
            raw.functions.auth.InitFirebasePnvLogin(
                api_id=self.api_id,
                api_hash=self.api_hash,
                except_ids=except_ids
            )
        )

        return types.FirebasePnvIntent._parse(r)
