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


from typing import List

import pyrogram
from pyrogram import enums, raw, types
from ..object import Object


class ReportResult(Object):
    """The result of a report.

    Parameters:
        type (:obj:`~pyrogram.enums.ReportResultType`):
            Type of the result.

        title (``str``, *optional*):
            Title for the options, if one of them must be chosen.

        options (List of :obj:`~pyrogram.types.ReportOption`, *optional*):
            The options, one of which must be chosen to continue the report.

        option_id (``bytes``, *optional*):
            Identifier of the option to pass along with the text, if a text must be added.

        is_text_optional (``bool``, *optional*):
            True, if the text can be skipped.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        type: "enums.ReportResultType",
        title: str = None,
        options: List["types.ReportOption"] = None,
        option_id: bytes = None,
        is_text_optional: bool = None
    ):
        super().__init__(client)

        self.type = type
        self.title = title
        self.options = options
        self.option_id = option_id
        self.is_text_optional = is_text_optional

    @staticmethod
    def _parse(client, result: "raw.base.ReportResult") -> "ReportResult":
        if isinstance(result, raw.types.ReportResultChooseOption):
            return ReportResult(
                type=enums.ReportResultType.OPTION_REQUIRED,
                title=result.title,
                options=types.List(types.ReportOption._parse(client, option) for option in result.options),
                client=client
            )

        if isinstance(result, raw.types.ReportResultAddComment):
            return ReportResult(
                type=enums.ReportResultType.TEXT_REQUIRED,
                option_id=result.option,
                is_text_optional=result.optional,
                client=client
            )

        return ReportResult(type=enums.ReportResultType.REPORTED, client=client)
