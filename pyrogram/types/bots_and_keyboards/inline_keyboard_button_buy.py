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

from ..object import Object

from pyrogram import enums, raw, utils

class InlineKeyboardButtonBuy(Object):
    """One button of the inline keyboard.
    For simple invoice buttons.

    Parameters:
        text (``str``):
            Text of the button. If none of the optional fields are used, it will be sent as a message when
            the button is pressed.

        style (:obj:`~pyrogram.enums.ButtonStyle`, *optional*):
            Style (color) of the button.

        icon_custom_emoji_id (``int``, *optional*):
            Identifier of the custom emoji shown on the button.
    """

    def __init__(
        self,
        text: str,
        style: "enums.ButtonStyle" = None,
        icon_custom_emoji_id: int = None
    ):
        super().__init__()

        self.text = str(text)
        self.style = style
        self.icon_custom_emoji_id = icon_custom_emoji_id

    @staticmethod
    def read(b):
        style, icon_custom_emoji_id = utils.parse_button_style(getattr(b, "style", None))

        return InlineKeyboardButtonBuy(
            text=b.text,
            style=style,
            icon_custom_emoji_id=icon_custom_emoji_id
        )

    async def write(self, _: "pyrogram.Client"):
        return raw.types.KeyboardInlineButton(
            text=self.text,
            type=raw.types.InlineButtonTypeBuy(),
            style=utils.get_button_style(self.style, self.icon_custom_emoji_id)
        )
