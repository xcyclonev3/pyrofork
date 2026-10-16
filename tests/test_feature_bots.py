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

"""Offline tests for the Layer 221-230 bot features: managed bots, prepared keyboard buttons,
guard bot answers, guest queries and business connected bots."""

import asyncio

import pytest

import pyrogram
from pyrogram import enums, raw, types
from tests.test_layer_roundtrip import offline_client, call, roundtrip, empty_updates, PEERS, USER, CHANNEL

BOT = raw.types.InputPeerUser(user_id=99, access_hash=3)
MANAGER_BOT = raw.types.InputPeerUser(user_id=98, access_hash=4)
BOT_PEERS = {**PEERS, 99: BOT, 98: MANAGER_BOT, "managerbot": MANAGER_BOT}

# bot and bot_info_version share flags.14: the server sends both
RAW_MANAGED_BOT = raw.types.User(
    id=99, bot=True, bot_info_version=1, first_name="My Bot", username="my_managed_bot", access_hash=3
)
RAW_MANAGER_BOT = raw.types.User(id=98, bot=True, bot_info_version=1, first_name="Manager", access_hash=4)
RAW_USER = raw.types.User(id=7, first_name="Alice", access_hash=2)


# Managed bots

def test_create_bot():
    client = offline_client(BOT_PEERS, lambda q: RAW_MANAGED_BOT)
    cached = []

    async def fetch_peers(peers):
        cached.extend(peers)
        return False

    client.fetch_peers = fetch_peers
    bot = call(lambda: client.create_bot("managerbot", "My Bot", "my_managed_bot", via_link=True))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.CreateBot)
    assert request.name == "My Bot" and request.username == "my_managed_bot" and request.via_deeplink
    assert request.manager_id.write() == MANAGER_BOT.write()
    assert isinstance(bot, types.User) and bot.id == 99 and bot.is_bot and bot.username == "my_managed_bot"
    assert [peer.id for peer in cached] == [99]


@pytest.mark.parametrize("revoke", [False, True])
def test_get_managed_bot_token(revoke):
    client = offline_client(BOT_PEERS, lambda q: raw.types.bots.ExportedBotToken(token="99:secret"))
    assert call(lambda: client.get_managed_bot_token(99, revoke=revoke)) == "99:secret"

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.ExportBotToken)
    assert request.bot.write() == BOT.write() and request.revoke is revoke


def test_get_managed_bot_access_settings():
    client = offline_client(BOT_PEERS, lambda q: raw.types.bots.AccessSettings(restricted=True, add_users=[RAW_USER]))
    settings = call(lambda: client.get_managed_bot_access_settings(99))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.GetAccessSettings) and request.bot.write() == BOT.write()
    assert isinstance(settings, types.BotAccessSettings) and settings.is_restricted is True
    assert [user.id for user in settings.added_users] == [7] and settings.added_users[0].first_name == "Alice"


def test_bot_access_settings_type_without_users():
    settings = types.BotAccessSettings._parse(None, roundtrip(raw.types.bots.AccessSettings()))
    assert settings.is_restricted is False and settings.added_users is None


@pytest.mark.parametrize("is_restricted, user_ids", [(True, [7]), (True, None), (False, None)])
def test_set_managed_bot_access_settings(is_restricted, user_ids):
    client = offline_client(BOT_PEERS, lambda q: True)
    assert call(lambda: client.set_managed_bot_access_settings(99, is_restricted, user_ids)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.EditAccessSettings) and request.bot.write() == BOT.write()
    assert request.restricted is is_restricted
    assert [user.write() for user in request.add_users] == [USER.write() for _ in user_ids or []]


# Bot usernames

@pytest.mark.parametrize("is_secondary", [False, True])
def test_check_bot_username(is_secondary):
    client = offline_client(BOT_PEERS, lambda q: False)
    assert call(lambda: client.check_bot_username("my_managed_bot", is_secondary=is_secondary)) is False

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.CheckUsername)
    assert request.username == "my_managed_bot" and request.additional is is_secondary


def test_add_bot_secondary_username():
    client = offline_client(BOT_PEERS, lambda q: True)
    assert call(lambda: client.add_bot_secondary_username(99, "my_shop")) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.AddUsername)
    assert request.bot.write() == BOT.write() and request.username == "my_shop"


def raw_bot_with_usernames(*usernames):
    return roundtrip(raw.types.User(
        id=99, bot=True, bot_info_version=1, first_name="My Bot", access_hash=3, usernames=list(usernames)
    ))


def test_delete_bot_secondary_username():
    bot = raw_bot_with_usernames(
        raw.types.Username(username="my_managed_bot", editable=True, active=True),
        raw.types.Username(username="my_shop", active=True, deletable=True),
    )
    client = offline_client(BOT_PEERS, lambda q: [bot] if isinstance(q, raw.functions.users.GetUsers) else True)
    assert call(lambda: client.delete_bot_secondary_username(99)) is True

    get_users, remove = client.sent
    assert isinstance(get_users, raw.functions.users.GetUsers) and get_users.id[0].write() == BOT.write()
    assert isinstance(remove, raw.functions.bots.RemoveUsername)
    assert remove.bot.write() == BOT.write() and remove.username == "my_shop"


def test_delete_bot_secondary_username_without_one():
    bot = raw_bot_with_usernames(raw.types.Username(username="my_managed_bot", editable=True, active=True))
    client = offline_client(BOT_PEERS, lambda q: [bot])
    assert call(lambda: client.delete_bot_secondary_username(99)) is True
    assert [type(query) for query in client.sent] == [raw.functions.users.GetUsers]


@pytest.mark.parametrize("is_active", [True, False])
def test_toggle_bot_username_is_active(is_active):
    client = offline_client(BOT_PEERS, lambda q: True)
    assert call(lambda: client.toggle_bot_username_is_active(99, "my_name", is_active)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.ToggleUsername)
    assert request.bot.write() == BOT.write() and request.username == "my_name" and request.active is is_active


# Prepared keyboard buttons

def test_reply_keyboard_read_keeps_request_button_ids():
    def request_peer(text, button_id, peer_type):
        return raw.types.KeyboardButton(text=text, type=raw.types.ButtonTypeRequestPeer(
            button_id=button_id, peer_type=peer_type, max_quantity=1
        ))

    markup = raw.types.ReplyKeyboardMarkup(rows=[raw.types.KeyboardButtonRow(buttons=[
        request_peer("user", 1, raw.types.RequestPeerTypeUser()),
        request_peer("group", 2, raw.types.RequestPeerTypeChat()),
        request_peer("channel", 3, raw.types.RequestPeerTypeBroadcast()),
    ])])
    [(user, group, channel)] = types.ReplyKeyboardMarkup.read(roundtrip(markup)).keyboard

    # Needed to share the chosen peers with share_users_with_bot / share_chat_with_bot
    assert user.request_user.button_id == 1
    assert group.request_chat.button_id == 2 and isinstance(group.request_chat, types.RequestPeerTypeChat)
    assert channel.request_chat.button_id == 3 and isinstance(channel.request_chat, types.RequestPeerTypeChannel)


def test_save_prepared_keyboard_button():
    client = offline_client(PEERS, lambda q: raw.types.bots.RequestedButton(webapp_req_id="req-1"))
    button = types.KeyboardButton(
        "Choose", request_user=types.RequestPeerTypeUser(button_id=1, is_bot=False, max=2),
        style=enums.ButtonStyle.SUCCESS
    )
    assert call(lambda: client.save_prepared_keyboard_button(7, button)) == "req-1"

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.RequestWebViewButton) and request.user_id.write() == USER.write()
    sent = request.button
    assert isinstance(sent, raw.types.KeyboardButton) and sent.text == "Choose" and sent.style.bg_success
    assert isinstance(sent.type, raw.types.InputButtonTypeRequestPeer)
    assert sent.type.button_id == 1 and sent.type.max_quantity == 2
    assert isinstance(sent.type.peer_type, raw.types.RequestPeerTypeUser) and sent.type.peer_type.bot is False


def test_save_prepared_keyboard_button_for_managed_bot():
    client = offline_client(PEERS, lambda q: raw.types.bots.RequestedButton(webapp_req_id="req-3"))
    button = types.KeyboardButton("Create", request_managed_bot=types.RequestPeerTypeManagedBot(
        button_id=3, suggested_name="Shop", suggested_username="shop_bot"
    ))
    assert call(lambda: client.save_prepared_keyboard_button(7, button)) == "req-3"

    peer_type = client.sent[-1].button.type.peer_type
    assert isinstance(peer_type, raw.types.RequestPeerTypeCreateBot) and peer_type.bot_managed
    assert peer_type.suggested_name == "Shop" and peer_type.suggested_username == "shop_bot"


def prepared_button(text, button_type, style=None):
    return raw.types.KeyboardButton(text=text, type=button_type, style=style)


def test_get_prepared_keyboard_button_request_users():
    client = offline_client(BOT_PEERS, lambda q: prepared_button("Choose", raw.types.ButtonTypeRequestPeer(
        button_id=1, peer_type=raw.types.RequestPeerTypeUser(bot=False), max_quantity=2
    ), raw.types.KeyboardButtonStyle(bg_success=True)))
    button = call(lambda: client.get_prepared_keyboard_button(99, "req-1"))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.GetRequestedWebViewButton)
    assert request.bot.write() == BOT.write() and request.webapp_req_id == "req-1"
    assert isinstance(button, types.KeyboardButton) and button.text == "Choose"
    assert button.style == enums.ButtonStyle.SUCCESS
    # The button identifier is needed to share the chosen users
    assert button.request_user.button_id == 1 and button.request_user.is_bot is False
    assert button.request_user.max == 2


def test_get_prepared_keyboard_button_request_chat():
    client = offline_client(BOT_PEERS, lambda q: prepared_button("Group", raw.types.ButtonTypeRequestPeer(
        button_id=2, peer_type=raw.types.RequestPeerTypeChat(forum=True), max_quantity=1
    )))
    button = call(lambda: client.get_prepared_keyboard_button(99, "req-2"))

    assert isinstance(button.request_chat, types.RequestPeerTypeChat)
    assert button.request_chat.button_id == 2 and button.request_chat.is_forum is True


def test_get_prepared_keyboard_button_request_managed_bot():
    client = offline_client(BOT_PEERS, lambda q: prepared_button("Create", raw.types.ButtonTypeRequestPeer(
        button_id=3, max_quantity=1,
        peer_type=raw.types.RequestPeerTypeCreateBot(bot_managed=True, suggested_username="shop_bot")
    )))
    button = call(lambda: client.get_prepared_keyboard_button(99, "req-3"))

    assert button.request_managed_bot.button_id == 3
    assert button.request_managed_bot.suggested_username == "shop_bot"


def test_get_prepared_keyboard_button_plain_text():
    client = offline_client(BOT_PEERS, lambda q: prepared_button("Hi", raw.types.ButtonTypeDefault()))
    button = call(lambda: client.get_prepared_keyboard_button(99, "req-4"))
    assert isinstance(button, types.KeyboardButton) and button.text == "Hi"


@pytest.mark.parametrize("source, msg_id, webapp_req_id", [
    (dict(message_id=10), 10, None),
    (dict(prepared_button_id="req-1"), None, "req-1"),
])
def test_share_users_with_bot(source, msg_id, webapp_req_id):
    client = offline_client(BOT_PEERS, lambda q: empty_updates())
    assert call(lambda: client.share_users_with_bot(99, 1, [7, 98], **source)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.SendBotRequestedPeer)
    assert request.peer.write() == BOT.write() and request.button_id == 1
    assert request.msg_id == msg_id and request.webapp_req_id == webapp_req_id
    assert [peer.write() for peer in request.requested_peers] == [USER.write(), MANAGER_BOT.write()]


def test_share_users_with_bot_single_user():
    client = offline_client(BOT_PEERS, lambda q: empty_updates())
    assert call(lambda: client.share_users_with_bot(99, 3, 98, prepared_button_id="req-3")) is True
    assert [peer.write() for peer in client.sent[-1].requested_peers] == [MANAGER_BOT.write()]


def test_share_chat_with_bot():
    client = offline_client(BOT_PEERS, lambda q: empty_updates())
    assert call(lambda: client.share_chat_with_bot(99, 2, -100123, message_id=11)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.SendBotRequestedPeer)
    assert request.peer.write() == BOT.write() and request.button_id == 2
    assert request.msg_id == 11 and request.webapp_req_id is None
    assert [peer.write() for peer in request.requested_peers] == [CHANNEL.write()]


@pytest.mark.parametrize("source", [{}, dict(message_id=10, prepared_button_id="req-1")])
def test_share_with_bot_needs_one_button_source(source):
    client = offline_client(BOT_PEERS, lambda q: empty_updates())
    with pytest.raises(ValueError):
        call(lambda: client.share_users_with_bot(99, 1, [7], **source))
    with pytest.raises(ValueError):
        call(lambda: client.share_chat_with_bot(99, 2, -100123, **source))
    assert client.sent == []


# Guest queries

RAW_GROUP = raw.types.Channel(
    id=123, access_hash=1, title="Group", photo=raw.types.ChatPhotoEmpty(), date=0, megagroup=True
)


def guest_result():
    return types.InlineQueryResultArticle("Answer", types.InputTextMessageContent("Hello"), id="r1")


def test_answer_guest_query():
    msg_id = raw.types.InputBotInlineMessageID64(dc_id=2, owner_id=7, id=10, access_hash=5)
    client = offline_client(PEERS, lambda q: msg_id)
    inline_message_id = call(lambda: client.answer_guest_query("321", guest_result()))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.SetBotGuestChatResult) and request.query_id == 321
    assert isinstance(request.result, raw.types.InputBotInlineResult)
    assert request.result.id == "r1" and request.result.title == "Answer"
    assert request.result.send_message.message == "Hello"
    assert inline_message_id == pyrogram.utils.pack_inline_message_id(msg_id)
    assert pyrogram.utils.unpack_inline_message_id(inline_message_id).write() == msg_id.write()


def test_guest_query_type():
    def group_message(message_id, text, **kwargs):
        return raw.types.Message(
            id=message_id, peer_id=raw.types.PeerChannel(channel_id=123), from_id=raw.types.PeerUser(user_id=7),
            date=message_id, message=text, **kwargs
        )

    update = roundtrip(raw.types.UpdateBotGuestChatQuery(
        query_id=321, qts=1,
        message=group_message(5, "@guest_bot what time is it?", reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=4)),
        reference_messages=[group_message(4, "Lunch at noon")]
    ))
    client = offline_client(PEERS, lambda q: None)
    client.me = None
    query = asyncio.run(types.GuestQuery._parse(client, update, {7: roundtrip(RAW_USER)}, {123: roundtrip(RAW_GROUP)}))

    assert isinstance(query, types.GuestQuery) and query.id == "321"
    assert query.message.id == 5 and query.message.text == "@guest_bot what time is it?"
    assert query.message.chat.id == pyrogram.utils.get_channel_id(123) and query.message.from_user.id == 7
    assert query.message.reply_to_message_id == 4
    [reference] = query.reference_messages
    assert reference.id == 4 and reference.text == "Lunch at noon"
    assert client.sent == []  # nothing is fetched from a chat the bot may not be a member of


def test_guest_query_type_without_reference_messages():
    update = roundtrip(raw.types.UpdateBotGuestChatQuery(query_id=1, qts=1, message=raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7), date=1, message="hi"
    )))
    client = offline_client(PEERS, lambda q: None)
    client.me = None
    query = asyncio.run(types.GuestQuery._parse(client, update, {7: roundtrip(RAW_USER)}, {}))
    assert query.message.text == "hi" and query.reference_messages is None


def test_guest_query_answer_bound_method():
    msg_id = raw.types.InputBotInlineMessageID(dc_id=2, id=10, access_hash=5)
    client = offline_client(PEERS, lambda q: msg_id)
    query = types.GuestQuery(id="321", message=None, client=client)
    assert call(lambda: query.answer(guest_result())) == pyrogram.utils.pack_inline_message_id(msg_id)

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.SetBotGuestChatResult) and request.query_id == 321


# Guard bots

@pytest.mark.parametrize("result, url, expected", [
    (enums.ChatJoinResultType.APPROVED, None, raw.types.JoinChatBotResultApproved),
    (enums.ChatJoinResultType.DECLINED, None, raw.types.JoinChatBotResultDeclined),
    (enums.ChatJoinResultType.QUEUED, None, raw.types.JoinChatBotResultQueued),
    (enums.ChatJoinResultType.WEB_APP, "https://example.org/check", raw.types.JoinChatBotResultWebView),
])
def test_answer_chat_join_request_query(result, url, expected):
    client = offline_client(PEERS, lambda q: True)
    assert call(lambda: client.answer_chat_join_request_query("555", result, url=url)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.SetJoinChatResults) and request.query_id == 555
    assert isinstance(request.result, expected)
    assert getattr(request.result, "url", None) == url


@pytest.mark.parametrize("result, url", [
    (enums.ChatJoinResultType.WEB_APP, None),
    (enums.ChatJoinResultType.APPROVED, "https://example.org/check"),
])
def test_answer_chat_join_request_query_checks_url(result, url):
    client = offline_client(PEERS, lambda q: True)
    with pytest.raises(ValueError):
        call(lambda: client.answer_chat_join_request_query(555, result, url=url))
    assert client.sent == []
