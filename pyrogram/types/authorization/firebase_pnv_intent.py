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

from pyrogram import raw
from ..object import Object


class FirebasePnvIntent(Object):
    """Parameters of a login with Firebase Phone Number Verification, returned by
    :meth:`~pyrogram.Client.init_firebase_pnv_login`.

    Parameters:
        nonce (``str``):
            The nonce to use for the phone number verification request.

        digital_credential_payload (``str``):
            The digital credential request payload to use for the phone number verification request.
    """

    def __init__(self, *, nonce: str, digital_credential_payload: str):
        super().__init__()

        self.nonce = nonce
        self.digital_credential_payload = digital_credential_payload

    @staticmethod
    def _parse(intent: "raw.types.auth.FirebasePnvIntent") -> "FirebasePnvIntent":
        return FirebasePnvIntent(
            nonce=intent.nonce,
            digital_credential_payload=intent.digital_credential_payload
        )
