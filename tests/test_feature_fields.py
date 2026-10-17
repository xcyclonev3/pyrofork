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

"""New fields of users, chats, usernames, keyboards and messages (Layer 221-230)."""

import pytest

from pyrogram import raw, types, utils
from tests.test_layer_roundtrip import (
    CHANNEL, PEERS, RAW_BOT, RAW_CHANNEL, RAW_USER, USER, call, empty_updates, offline_client, parse_message, roundtrip
)


@pytest.mark.parametrize("flags, expected", [
    (dict(bot_forum_view=True, bot_forum_can_manage_topics=True, bot_can_manage_bots=True, bot_guestchat=True,
          bot_guard=True), (True, True, True, True, True)),
    (dict(bot_forum_view=True), (True, False, False, False, False)),
    # Like TDLib, users can't manage the topics of a bot without topics
    (dict(bot_forum_can_manage_topics=True), (False, False, False, False, False)),
])
def test_bot_flags(flags, expected):
    bot = types.User._parse(None, roundtrip(raw.types.User(
        id=99, bot=True, bot_info_version=1, first_name="Bot", access_hash=3, **flags)))

    assert (bot.has_topics, bot.allows_users_to_create_topics, bot.can_manage_bots, bot.supports_guest_queries,
            bot.is_guard_bot) == expected


def test_users_have_no_bot_flags():
    user = types.User._parse(None, roundtrip(RAW_USER))
    assert (user.has_topics, user.can_manage_bots, user.supports_guest_queries, user.is_guard_bot) == (None,) * 4


def test_username_flags():
    bot = types.User._parse(None, roundtrip(raw.types.User(
        id=99, bot=True, bot_info_version=1, first_name="Bot", access_hash=3, usernames=[
            raw.types.Username(username="main_bot", editable=True, active=True),
            raw.types.Username(username="second_bot", active=True, deletable=True),
            raw.types.Username(username="old_bot", expired=True),
        ])))

    assert bot.username == "main_bot"
    second, old = bot.usernames
    assert (second.username, second.deletable, second.expired) == ("second_bot", True, False)
    assert (old.username, old.deletable, old.expired) == ("old_bot", False, True)


@pytest.mark.parametrize("markup", [
    types.InlineKeyboardMarkup([[types.InlineKeyboardButton("a", callback_data="a")]], force_reply=True),
    types.ReplyKeyboardMarkup([["a"]], force_reply=True),
])
def test_keyboards_forcing_a_reply(markup):
    written = roundtrip(call(lambda: markup.write(None)))
    assert written.force_reply is True
    assert type(markup).read(written).force_reply is True

    markup.force_reply = None
    written = roundtrip(call(lambda: markup.write(None)))
    assert written.force_reply is False and type(markup).read(written).force_reply is None


def test_full_user_fields():
    manager = raw.types.User(id=98, bot=True, bot_info_version=1, first_name="Manager", access_hash=4)
    full = raw.types.users.UserFull(
        full_user=raw.types.UserFull(
            id=99, settings=raw.types.PeerSettings(), notify_settings=raw.types.PeerNotifySettings(),
            common_chats_count=0, noforwards_peer_enabled=True, unofficial_security_risk=True, bot_manager_id=98),
        chats=[], users=[RAW_BOT, manager])
    client = offline_client({**PEERS, 99: raw.types.InputPeerUser(user_id=99, access_hash=3)}, lambda q: full)
    chat = call(lambda: client.get_chat(99))

    assert isinstance(client.sent[0], raw.functions.users.GetFullUser)
    assert chat.has_protected_content is True and chat.uses_unofficial_app is True
    assert chat.manager_bot.id == 98 and chat.manager_bot.first_name == "Manager"


def test_full_supergroup_guard_bot():
    full = raw.types.messages.ChatFull(
        full_chat=raw.types.ChannelFull(
            id=123, about="", read_inbox_max_id=0, read_outbox_max_id=0, unread_count=0,
            chat_photo=raw.types.PhotoEmpty(id=0), notify_settings=raw.types.PeerNotifySettings(), bot_info=[], pts=1,
            guard_bot_id=99, has_welcome_messages=True),
        chats=[RAW_CHANNEL], users=[RAW_BOT])
    client = offline_client(PEERS, lambda q: full)
    chat = call(lambda: client.get_chat(-100123))

    assert chat.guard_bot.id == 99 and chat.guard_bot.is_bot and chat.has_welcome_messages is True


@pytest.mark.parametrize("caller, check", [
    (raw.types.PeerUser(user_id=7), lambda c: c.id == 7 and c.first_name == "A"),
    (raw.types.PeerChannel(channel_id=555), lambda c: c is None),  # not in chats: unknown
])
def test_guest_bot_caller(caller, check):
    message = parse_message(raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=99), date=1, message="answer",
        guestchat_via_from=caller), users=[RAW_USER, RAW_BOT])

    assert check(message.guest_bot_caller)


def test_replies_to_poll_options():
    message = parse_message(raw.types.Message(
        id=2, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7), date=1, message="+1",
        reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=1, poll_option=b"\x01")), users=[RAW_USER])
    assert message.reply_to_message_id == 1 and message.reply_to_poll_option_data == b"\x01"

    option = types.PollOption("yes", data=b"\x02")
    client = offline_client(PEERS, lambda q: empty_updates())
    for poll_option in (b"\x02", option):
        call(lambda: client.send_message(7, "+1", reply_to_message_id=1, reply_to_poll_option=poll_option))

    assert len(client.sent) == 2
    for request in client.sent:
        assert isinstance(request.reply_to, raw.types.InputReplyToMessage)
        assert request.reply_to.reply_to_msg_id == 1 and request.reply_to.poll_option == b"\x02"


@pytest.mark.parametrize("kwargs, guard_bot, apply_to_invites", [
    (dict(enabled=True, guard_bot_id=99, apply_to_invite_links=True),
     raw.types.InputUser(user_id=99, access_hash=3), True),
    (dict(enabled=True), None, False),
    (dict(enabled=False, guard_bot_id=99), None, False),
])
def test_set_chat_join_by_request(kwargs, guard_bot, apply_to_invites):
    client = offline_client({**PEERS, 99: raw.types.InputPeerUser(user_id=99, access_hash=3)},
                            lambda q: empty_updates())
    assert call(lambda: client.set_chat_join_by_request(-100123, **kwargs)) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.channels.ToggleJoinRequest)
    assert request.channel == CHANNEL and request.enabled is kwargs["enabled"]
    assert request.guard_bot == guard_bot and request.apply_to_invites is apply_to_invites
