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
from ..object import Object


class TonConnectManifest(Object):
    """Manifest of a dApp connected through TON Connect.

    Parameters:
        url (``str``):
            URL of the dApp.

        name (``str``):
            Name of the dApp.

        icon (:obj:`~pyrogram.types.WebDocument`, *optional*):
            Icon of the dApp.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        url: str,
        name: str,
        icon: Optional["types.WebDocument"] = None
    ):
        super().__init__(client)

        self.url = url
        self.name = name
        self.icon = icon

    @staticmethod
    def _parse(client, manifest: "raw.types.TonConnectManifest") -> Optional["TonConnectManifest"]:
        if manifest is None:
            return None

        return TonConnectManifest(
            url=manifest.url,
            name=manifest.name,
            icon=types.WebDocument._parse(client, manifest.icon),
            client=client
        )
