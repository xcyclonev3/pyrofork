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

from types import SimpleNamespace
from typing import Optional, Tuple, Union

import pyrogram
from pyrogram import enums, raw, types
from ..object import Object
from .rich_text import RichText, write_rich_text

ACTION_FIELDS = (
    "callback_data",
    "url",
    "web_app",
    "login_url",
    "user_id",
    "switch_inline_query",
    "switch_inline_query_current_chat",
    "requires_password",
    "copy_text",
    "is_disabled",
)


class InlineButton(Object):
    """A button inside a rich message, shown by :obj:`~pyrogram.types.RichTextButton` and
    :obj:`~pyrogram.types.PageBlockButtonRow`.

    Pass exactly one of the optional action fields, as for :obj:`~pyrogram.types.InlineKeyboardButton`.
    Regular users may only send buttons with *url*, *user_id* or *copy_text*.

    Parameters:
        text (:obj:`~pyrogram.types.RichText` | ``str``):
            Text of the button; only :obj:`~pyrogram.types.RichTexts`, :obj:`~pyrogram.types.RichTextPlain` and
            :obj:`~pyrogram.types.RichTextCustomEmoji` are allowed.

        style (:obj:`~pyrogram.enums.ButtonStyle`, *optional*):
            Style of the button. :obj:`~pyrogram.enums.ButtonStyle.LINK` is allowed only for callback buttons.

        callback_data (``str`` | ``bytes``, *optional*):
            Data to be sent in a callback query to the bot when the button is pressed, 1-64 bytes.

        url (``str``, *optional*):
            HTTP URL to be opened when the button is pressed.

        web_app (:obj:`~pyrogram.types.WebAppInfo`, *optional*):
            Web App to be launched when the button is pressed.

        login_url (:obj:`~pyrogram.types.LoginUrl`, *optional*):
            An HTTP URL used to automatically authorize the user.

        user_id (``int``, *optional*):
            Identifier of a user whose profile is opened when the button is pressed.

        switch_inline_query (``str``, *optional*):
            Inline query to be inserted in the input field of a chat chosen by the user.

        switch_inline_query_current_chat (``str``, *optional*):
            Inline query to be inserted in the input field of the current chat.

        requires_password (``bool``, *optional*):
            True, if the callback button asks for the 2-step verification password of the current user first.

        copy_text (``str``, *optional*):
            Text to be copied to the clipboard when the button is pressed.

        is_disabled (``bool``, *optional*):
            True, if the button is disabled and does nothing when pressed; may be received only.
    """

    def __init__(
        self,
        text: Union["RichText", str],
        style: "enums.ButtonStyle" = None,
        callback_data: Union[str, bytes] = None,
        url: str = None,
        web_app: "types.WebAppInfo" = None,
        login_url: "types.LoginUrl" = None,
        user_id: int = None,
        switch_inline_query: str = None,
        switch_inline_query_current_chat: str = None,
        requires_password: bool = None,
        copy_text: str = None,
        is_disabled: bool = None
    ):
        super().__init__()

        self.text = text
        self.style = style
        self.callback_data = callback_data
        self.url = url
        self.web_app = web_app
        self.login_url = login_url
        self.user_id = user_id
        self.switch_inline_query = switch_inline_query
        self.switch_inline_query_current_chat = switch_inline_query_current_chat
        self.requires_password = requires_password
        self.copy_text = copy_text
        self.is_disabled = is_disabled

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        button: Union["raw.types.PageButton", "raw.types.TextButton"],
        context=None
    ) -> "InlineButton":
        action = types.InlineKeyboardButton._read_type(SimpleNamespace(text="", type=button.type))

        return InlineButton(
            text=RichText._parse(client, button.text, context),
            style=parse_rich_button_style(button.style),
            **{name: getattr(action, name, None) for name in ACTION_FIELDS}
        )

    async def write(self, writer) -> Tuple["raw.base.RichText", "raw.base.InlineButtonType", Optional["raw.types.RichButtonStyle"]]:
        """The text, type and style of the raw button (pageButton and textButton share them)."""
        client = writer.client

        if self.user_id is not None:
            button_type = raw.types.InputInlineButtonTypeUserProfile(user_id=await writer.add_user(self.user_id))
        elif self.login_url is not None:
            button_type = raw.types.InputInlineButtonTypeUrlAuth(
                url=self.login_url.url,
                bot=await writer.get_input_user(self.login_url.bot_username or "self"),
                fwd_text=self.login_url.forward_text,
                request_write_access=self.login_url.request_write_access
            )
        else:
            button = types.InlineKeyboardButton(
                text="",
                **{name: getattr(self, name) for name in ACTION_FIELDS if name not in ("user_id", "login_url")}
            )
            raw_button = await button._write_button(client)

            if raw_button is None:
                raise ValueError(f"The rich message button {self.text!r} has no action")

            button_type = raw_button.type

        return await write_rich_text(writer, self.text), button_type, get_rich_button_style(self.style)


def parse_rich_button_style(style: Optional["raw.types.RichButtonStyle"]) -> Optional["enums.ButtonStyle"]:
    """The style of a rich message button, as TDLib reads richButtonStyle."""
    if style is None:
        return None

    if style.bg_primary:
        return enums.ButtonStyle.PRIMARY
    if style.bg_danger:
        return enums.ButtonStyle.DANGER
    if style.bg_success:
        return enums.ButtonStyle.SUCCESS
    if style.link:
        return enums.ButtonStyle.LINK

    return enums.ButtonStyle.DEFAULT


def get_rich_button_style(style: Optional["enums.ButtonStyle"]) -> Optional["raw.types.RichButtonStyle"]:
    """The richButtonStyle to send, or None for the default style."""
    if style in (None, enums.ButtonStyle.DEFAULT):
        return None

    return raw.types.RichButtonStyle(
        bg_primary=style == enums.ButtonStyle.PRIMARY,
        bg_danger=style == enums.ButtonStyle.DANGER,
        bg_success=style == enums.ButtonStyle.SUCCESS,
        link=style == enums.ButtonStyle.LINK
    )
