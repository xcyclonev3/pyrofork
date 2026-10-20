#  Pyrogram - Telegram MTProto API Client Library for Python
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.


import pyrogram
from pyrogram import raw
from ..object import Object


class ReportOption(Object):
    """An option to report an entity to Telegram.

    Parameters:
        id (``bytes``):
            Unique identifier of the option.

        text (``str``):
            Text of the option.
    """

    def __init__(self, *, client: "pyrogram.Client" = None, id: bytes, text: str):
        super().__init__(client)

        self.id = id
        self.text = text

    @staticmethod
    def _parse(client, option: "raw.types.MessageReportOption") -> "ReportOption":
        return ReportOption(id=option.option, text=option.text, client=client)
