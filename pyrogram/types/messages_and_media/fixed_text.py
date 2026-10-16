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

import pyrogram
from pyrogram import raw, types

from ..object import Object


class FixedText(Object):
    """A text fixed with :meth:`~pyrogram.Client.fix_text_with_ai`.

    Parameters:
        text (:obj:`~pyrogram.types.FormattedText`):
            The fixed text.

        diff_text (:obj:`~pyrogram.types.FormattedText`, *optional*):
            The changes made to the original text: a text whose entities, of type
            :obj:`~pyrogram.enums.MessageEntityType.DIFF_INSERT`, :obj:`~pyrogram.enums.MessageEntityType.DIFF_REPLACE`
            (with the replaced text in *old_text*) and :obj:`~pyrogram.enums.MessageEntityType.DIFF_DELETE`,
            mark the inserted, replaced and deleted parts.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        text: "types.FormattedText",
        diff_text: "types.FormattedText" = None
    ):
        super().__init__(client)

        self.text = text
        self.diff_text = diff_text

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        composed: "raw.types.messages.ComposedMessageWithAI"
    ) -> "FixedText":
        return FixedText(
            text=types.FormattedText._parse(client, composed.result_text),
            diff_text=types.FormattedText._parse(client, composed.diff_text),
            client=client
        )
