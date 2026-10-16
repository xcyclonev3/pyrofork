#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
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

from pyrogram import enums, raw, types, utils
from ..object import Object
from typing import Union

class KeyboardButton(Object):
    """One button of the reply keyboard.
    For simple text buttons String can be used instead of this object to specify text of the button.
    Optional fields are mutually exclusive.

    Parameters:
        text (``str``):
            Text of the button. If none of the optional fields are used, it will be sent as a message when
            the button is pressed.

        request_contact (``bool``, *optional*):
            If True, the user's phone number will be sent as a contact when the button is pressed.
            Available in private chats only.

        request_location (``bool``, *optional*):
            If True, the user's current location will be sent when the button is pressed.
            Available in private chats only.

        request_chat (:obj:`~pyrogram.types.RequestPeerTypeChannel` | :obj:`~pyrogram.types.RequestPeerTypeChat`, *optional*):
            If specified, defines the criteria used to request a suitable chats/channels.
            The identifier of the selected chats will be shared with the bot when the corresponding button is pressed.

        request_user (:obj:`~pyrogram.types.RequestPeerTypeUser`, *optional*):
            If specified, defines the criteria used to request a suitable users.
            The identifier of the selected users will be shared with the bot when the corresponding button is pressed.

        web_app (:obj:`~pyrogram.types.WebAppInfo`, *optional*):
            If specified, the described `Web App <https://core.telegram.org/bots/webapps>`_ will be launched when the
            button is pressed. The Web App will be able to send a “web_app_data” service message. Available in private
            chats only.

        request_poll (:obj:`~pyrogram.types.KeyboardButtonPollType`, *optional*):
            If specified, the user will be asked to create a poll and send it to the bot when the button is pressed.
            Available in private chats only.

        request_managed_bot (:obj:`~pyrogram.types.RequestPeerTypeManagedBot`, *optional*):
            If specified, the user will be asked to create a bot managed by the current bot when the button is
            pressed. Available in private chats only.

        style (:obj:`~pyrogram.enums.ButtonStyle`, *optional*):
            Style (color) of the button.

        icon_custom_emoji_id (``int``, *optional*):
            Identifier of the custom emoji shown on the button.
    """

    def __init__(
        self,
        text: str,
        request_contact: bool = None,
        request_location: bool = None,
        request_chat: Union["types.RequestPeerTypeChat","types.RequestPeerTypeChannel"] = None,
        request_user: "types.RequestPeerTypeUser" = None,
        web_app: "types.WebAppInfo" = None,
        request_poll: "types.KeyboardButtonPollType" = None,
        request_managed_bot: "types.RequestPeerTypeManagedBot" = None,
        style: "enums.ButtonStyle" = None,
        icon_custom_emoji_id: int = None
    ):
        super().__init__()

        self.text = str(text)
        self.request_contact = request_contact
        self.request_location = request_location
        self.request_chat = request_chat
        self.request_user = request_user
        self.web_app = web_app
        self.request_poll = request_poll
        self.request_managed_bot = request_managed_bot
        self.style = style
        self.icon_custom_emoji_id = icon_custom_emoji_id

    @staticmethod
    def read(b: "raw.base.KeyboardButton"):
        style, icon_custom_emoji_id = utils.parse_button_style(b.style)

        # Plain text buttons without a style are read as their text, as before
        if isinstance(b.type, raw.types.ButtonTypeDefault) and style is None and icon_custom_emoji_id is None:
            return b.text

        button = KeyboardButton._read_type(b) or KeyboardButton(text=b.text)
        button.style = style
        button.icon_custom_emoji_id = icon_custom_emoji_id

        return button

    @staticmethod
    def _read_type(b: "raw.base.KeyboardButton"):
        button_type = b.type

        if isinstance(button_type, raw.types.ButtonTypeRequestPoll):
            return KeyboardButton(
                text=b.text,
                request_poll=types.KeyboardButtonPollType(
                    type=(
                        None if button_type.quiz is None
                        else enums.PollType.QUIZ if button_type.quiz
                        else enums.PollType.REGULAR
                    )
                )
            )

        if isinstance(button_type, raw.types.ButtonTypeRequestPhone):
            return KeyboardButton(
                text=b.text,
                request_contact=True
            )

        if isinstance(button_type, raw.types.ButtonTypeRequestGeoLocation):
            return KeyboardButton(
                text=b.text,
                request_location=True
            )

        if isinstance(button_type, raw.types.ButtonTypeSimpleWebView):
            return KeyboardButton(
                text=b.text,
                web_app=types.WebAppInfo(
                    url=button_type.url
                )
            )

        if isinstance(button_type, raw.types.ButtonTypeRequestPeer):
            if isinstance(button_type.peer_type, raw.types.RequestPeerTypeBroadcast):
                user_privileges = getattr(button_type.peer_type, "user_admin_rights", None)
                bot_privileges = getattr(button_type.peer_type, "bot_admin_rights", None)
                return KeyboardButton(
                    text=b.text,
                    request_chat=types.RequestPeerTypeChannel(
                        is_creator=button_type.peer_type.creator,
                        is_username=button_type.peer_type.has_username,
                        max=button_type.max_quantity,
                        user_privileges=user_privileges,
                        bot_privileges=bot_privileges
                    )
                )
            if isinstance(button_type.peer_type, raw.types.RequestPeerTypeChat):
                user_privileges = getattr(button_type.peer_type, "user_admin_rights", None)
                bot_privileges = getattr(button_type.peer_type, "bot_admin_rights", None)
                return KeyboardButton(
                    text=b.text,
                    request_chat=types.RequestPeerTypeChat(
                        is_creator=button_type.peer_type.creator,
                        is_bot_participant=button_type.peer_type.bot_participant,
                        is_username=button_type.peer_type.has_username,
                        is_forum=button_type.peer_type.forum,
                        max=button_type.max_quantity,
                        user_privileges=user_privileges,
                        bot_privileges=bot_privileges
                    )
                )

            if isinstance(button_type.peer_type, raw.types.RequestPeerTypeUser):
                return KeyboardButton(
                    text=b.text,
                    request_user=types.RequestPeerTypeUser(
                        is_bot=button_type.peer_type.bot,
                        is_premium=button_type.peer_type.premium,
                        max=button_type.max_quantity
                    )
            )

            if isinstance(button_type.peer_type, raw.types.RequestPeerTypeCreateBot):
                return KeyboardButton(
                    text=b.text,
                    request_managed_bot=types.RequestPeerTypeManagedBot(
                        button_id=button_type.button_id,
                        suggested_name=button_type.peer_type.suggested_name,
                        suggested_username=button_type.peer_type.suggested_username
                    )
                )

    def write(self):
        return raw.types.KeyboardButton(
            text=self.text,
            type=self._write_type(),
            style=utils.get_button_style(self.style, self.icon_custom_emoji_id)
        )

    def _write_type(self):
        if self.request_contact:
            return raw.types.ButtonTypeRequestPhone()
        elif self.request_location:
            return raw.types.ButtonTypeRequestGeoLocation()
        elif self.request_chat:
            user_privileges = self.request_chat.user_privileges
            bot_privileges = self.request_chat.bot_privileges

            user_admin_rights = raw.types.ChatAdminRights(
                    change_info=user_privileges.can_change_info,
                    post_messages=user_privileges.can_post_messages,
                    post_stories=user_privileges.can_post_stories,
                    edit_messages=user_privileges.can_edit_messages,
                    edit_stories=user_privileges.can_post_stories,
                    delete_messages=user_privileges.can_delete_messages,
                    delete_stories=user_privileges.can_delete_stories,
                    ban_users=user_privileges.can_restrict_members,
                    invite_users=user_privileges.can_invite_users,
                    pin_messages=user_privileges.can_pin_messages,
                    add_admins=user_privileges.can_promote_members,
                    anonymous=user_privileges.is_anonymous,
                    manage_call=user_privileges.can_manage_video_chats,
                    other=user_privileges.can_manage_chat,
                    manage_ranks=getattr(user_privileges, "can_manage_tags", None),
                    manage_welcome_messages=getattr(user_privileges, "can_send_welcome_messages", None)
            ) if user_privileges else None

            bot_admin_rights = raw.types.ChatAdminRights(
                    change_info=bot_privileges.can_change_info,
                    post_messages=bot_privileges.can_post_messages,
                    post_stories=bot_privileges.can_post_stories,
                    edit_messages=bot_privileges.can_edit_messages,
                    edit_stories=bot_privileges.can_post_stories,
                    delete_messages=bot_privileges.can_delete_messages,
                    delete_stories=bot_privileges.can_delete_stories,
                    ban_users=bot_privileges.can_restrict_members,
                    invite_users=bot_privileges.can_invite_users,
                    pin_messages=bot_privileges.can_pin_messages,
                    add_admins=bot_privileges.can_promote_members,
                    anonymous=bot_privileges.is_anonymous,
                    manage_call=bot_privileges.can_manage_video_chats,
                    other=bot_privileges.can_manage_chat,
                    manage_ranks=getattr(bot_privileges, "can_manage_tags", None),
                    manage_welcome_messages=getattr(bot_privileges, "can_send_welcome_messages", None)
            ) if bot_privileges else None

            if isinstance(self.request_chat, types.RequestPeerTypeChannel):
                return raw.types.InputButtonTypeRequestPeer(
                    button_id=self.request_chat.button_id,
                    peer_type=raw.types.RequestPeerTypeBroadcast(
                        creator=self.request_chat.is_creator,
                        has_username=self.request_chat.is_username,
                        user_admin_rights=user_admin_rights,
                        bot_admin_rights=bot_admin_rights
                    ),
                    max_quantity=self.request_chat.max,
                    name_requested=self.request_chat.is_name_requested,
                    username_requested=self.request_chat.is_username_requested,
                    photo_requested=self.request_chat.is_photo_requested
                )
            return raw.types.InputButtonTypeRequestPeer(
                button_id=self.request_chat.button_id,
                peer_type=raw.types.RequestPeerTypeChat(
                    creator=self.request_chat.is_creator,
                    bot_participant=self.request_chat.is_bot_participant,
                    has_username=self.request_chat.is_username,
                    forum=self.request_chat.is_forum,
                    user_admin_rights=user_admin_rights,
                    bot_admin_rights=bot_admin_rights
                ),
                max_quantity=self.request_chat.max,
                name_requested=self.request_chat.is_name_requested,
                username_requested=self.request_chat.is_username_requested,
                photo_requested=self.request_chat.is_photo_requested
            )
        elif self.request_user:
            return raw.types.InputButtonTypeRequestPeer(
                button_id=self.request_user.button_id,
                peer_type=raw.types.RequestPeerTypeUser(
                    bot=self.request_user.is_bot,
                    premium=self.request_user.is_premium
                ),
                max_quantity=self.request_user.max,
                name_requested=self.request_user.is_name_requested,
                username_requested=self.request_user.is_username_requested,
                photo_requested=self.request_user.is_photo_requested
            )
        elif self.web_app:
            return raw.types.ButtonTypeSimpleWebView(url=self.web_app.url)
        elif self.request_poll:
            # Like TDLib: no quiz flag allows any poll, quiz=True only quizzes, quiz=False only regular polls
            poll_type = self.request_poll.type
            return raw.types.ButtonTypeRequestPoll(
                quiz=None if poll_type is None else poll_type == enums.PollType.QUIZ
            )
        elif self.request_managed_bot:
            return raw.types.InputButtonTypeRequestPeer(
                button_id=self.request_managed_bot.button_id,
                # As TDLib's RequestedDialogType: always bot_managed, one bot, no name/username/photo requested
                peer_type=raw.types.RequestPeerTypeCreateBot(
                    bot_managed=True,
                    suggested_name=self.request_managed_bot.suggested_name,
                    suggested_username=self.request_managed_bot.suggested_username
                ),
                max_quantity=1
            )
        else:
            return raw.types.ButtonTypeDefault()
