"""Offline round-trip tests: high-level -> raw -> TL bytes -> raw -> high-level."""
import asyncio
from io import BytesIO

import pytest

import pyrogram
from pyrogram import raw, types
from pyrogram.raw.core import TLObject


def to_raw(markup):
    async def main():  # inside a running loop write() stays a coroutine
        return await markup.write(None)
    raw_obj = asyncio.run(main())
    data = raw_obj.write()
    back = TLObject.read(BytesIO(data))
    assert back.write() == data, "TL serialization is not stable"
    return back


def test_inline_keyboard_roundtrip():
    markup = types.InlineKeyboardMarkup([[
        types.InlineKeyboardButton("cb", callback_data="x"),
        types.InlineKeyboardButton("url", url="https://telegram.org"),
    ]])
    parsed = types.InlineKeyboardMarkup.read(to_raw(markup))
    row = parsed.inline_keyboard[0]
    assert [b.text for b in row] == ["cb", "url"]
    assert row[0].callback_data == "x" and row[1].url == "https://telegram.org"


def test_reply_keyboard_roundtrip():
    markup = types.ReplyKeyboardMarkup(
        [[types.KeyboardButton("plain"), types.KeyboardButton("phone", request_contact=True)]],
        resize_keyboard=True,
    )
    parsed = types.ReplyKeyboardMarkup.read(to_raw(markup))
    texts = [b if isinstance(b, str) else b.text for b in parsed.keyboard[0]]
    assert texts == ["plain", "phone"]


class PeerResolver:
    """Minimal stand-in for Client.resolve_peer, enough for markup.write()."""

    async def resolve_peer(self, peer_id):
        if peer_id in ("self", "me"):
            return raw.types.InputPeerSelf()
        return raw.types.InputPeerUser(user_id=int(peer_id), access_hash=0)


def roundtrip(obj):
    # Unlike to_raw(), do not re-serialize what was read back: absent flag vectors are read
    # as [] and the generated write() then emits them without their flag (compiler issue).
    data = obj.write()
    buffer = BytesIO(data)
    back = TLObject.read(buffer)
    assert buffer.tell() == len(data), "TL bytes were not fully consumed"
    return back


def write_markup(markup, client=None):
    async def main():
        return await markup.write(client)
    return roundtrip(asyncio.run(main()))


def test_inline_keyboard_writes_layer_230_buttons():
    markup = types.InlineKeyboardMarkup([
        [
            types.InlineKeyboardButton("cb", callback_data=b"\xff", requires_password=True),
            types.InlineKeyboardButton("switch", switch_inline_query="q"),
            types.InlineKeyboardButton("here", switch_inline_query_current_chat="q2"),
        ],
        [
            types.InlineKeyboardButton("game", callback_game=types.CallbackGame()),
            types.InlineKeyboardButton("app", web_app=types.WebAppInfo(url="https://example.org/app")),
            types.InlineKeyboardButton("copy", copy_text="secret"),
        ],
        [
            types.InlineKeyboardButton("profile", user_id=42),
            types.InlineKeyboardButton("login", login_url=types.LoginUrl(
                url="https://example.org/login", forward_text="fwd", request_write_access=True)),
            types.InlineKeyboardButtonBuy("pay"),
        ],
    ])
    r = write_markup(markup, PeerResolver())

    assert isinstance(r, raw.types.ReplyInlineMarkup)
    assert all(isinstance(row, raw.types.KeyboardInlineButtonRow) for row in r.rows)
    buttons = [b for row in r.rows for b in row.buttons]
    assert all(isinstance(b, raw.types.KeyboardInlineButton) for b in buttons)
    assert [b.text for b in buttons] == ["cb", "switch", "here", "game", "app", "copy", "profile", "login", "pay"]
    kinds = [type(b.type) for b in buttons]
    assert kinds == [
        raw.types.InlineButtonTypeCallback,
        raw.types.InlineButtonTypeSwitchInline,
        raw.types.InlineButtonTypeSwitchInline,
        raw.types.InlineButtonTypeGame,
        raw.types.InlineButtonTypeWebView,
        raw.types.InlineButtonTypeCopy,
        raw.types.InputInlineButtonTypeUserProfile,
        raw.types.InputInlineButtonTypeUrlAuth,
        raw.types.InlineButtonTypeBuy,
    ]
    cb, switch, here, _, app, copy, profile, login, _ = (b.type for b in buttons)
    assert cb.data == b"\xff" and cb.requires_password
    assert switch.query == "q" and not switch.same_peer
    assert here.query == "q2" and here.same_peer
    assert app.url == "https://example.org/app"
    assert copy.copy_text == "secret"
    assert profile.user_id.user_id == 42
    assert login.url == "https://example.org/login" and login.fwd_text == "fwd"
    assert login.request_write_access and isinstance(login.bot, raw.types.InputPeerSelf)


def test_inline_keyboard_reads_layer_230_buttons():
    def button(text, button_type):
        return raw.types.KeyboardInlineButton(text=text, type=button_type)

    server_markup = raw.types.ReplyInlineMarkup(rows=[
        raw.types.KeyboardInlineButtonRow(buttons=[
            button("cb", raw.types.InlineButtonTypeCallback(data=b"\xff", requires_password=True)),
            button("profile", raw.types.InlineButtonTypeUserProfile(user_id=42)),
            button("login", raw.types.InlineButtonTypeUrlAuth(url="https://example.org", button_id=7, fwd_text="f")),
        ]),
        raw.types.KeyboardInlineButtonRow(buttons=[
            button("switch", raw.types.InlineButtonTypeSwitchInline(query="q", same_peer=True)),
            button("app", raw.types.InlineButtonTypeWebView(url="https://example.org/app")),
            button("copy", raw.types.InlineButtonTypeCopy(copy_text="secret")),
        ]),
        raw.types.KeyboardInlineButtonRow(buttons=[
            button("game", raw.types.InlineButtonTypeGame()),
            button("pay", raw.types.InlineButtonTypeBuy()),
            button("off", raw.types.InlineButtonTypeDisabled()),
        ]),
    ])
    parsed = types.InlineKeyboardMarkup.read(roundtrip(server_markup))
    (cb, profile, login), (switch, app, copy), (game, pay, off) = parsed.inline_keyboard

    assert cb.callback_data == b"\xff" and cb.requires_password
    assert profile.user_id == 42
    assert login.login_url.url == "https://example.org" and login.login_url.button_id == 7
    assert login.login_url.forward_text == "f"
    assert switch.switch_inline_query_current_chat == "q"
    assert app.web_app.url == "https://example.org/app"
    assert copy.copy_text == "secret"
    assert isinstance(game.callback_game, types.CallbackGame)
    assert isinstance(pay, types.InlineKeyboardButtonBuy) and pay.text == "pay"
    assert off.text == "off" and off.callback_data is None and off.url is None


def test_reply_keyboard_layer_230_button_types():
    markup = types.ReplyKeyboardMarkup(
        [
            ["plain", types.KeyboardButton("geo", request_location=True)],
            [types.KeyboardButton("app", web_app=types.WebAppInfo(url="https://example.org/app"))],
            [
                types.KeyboardButton("user", request_user=types.RequestPeerTypeUser(button_id=1, is_bot=False)),
                types.KeyboardButton("group", request_chat=types.RequestPeerTypeChat(button_id=2, is_forum=True)),
                types.KeyboardButton("channel", request_chat=types.RequestPeerTypeChannel(button_id=3, max=2)),
            ],
        ],
        is_persistent=True,
        placeholder="pick",
    )
    r = write_markup(markup)

    assert isinstance(r, raw.types.ReplyKeyboardMarkup) and r.persistent and r.placeholder == "pick"
    buttons = [b for row in r.rows for b in row.buttons]
    assert all(isinstance(b, raw.types.KeyboardButton) for b in buttons)
    assert [type(b.type) for b in buttons] == [
        raw.types.ButtonTypeDefault,
        raw.types.ButtonTypeRequestGeoLocation,
        raw.types.ButtonTypeSimpleWebView,
        raw.types.InputButtonTypeRequestPeer,
        raw.types.InputButtonTypeRequestPeer,
        raw.types.InputButtonTypeRequestPeer,
    ]
    user, group, channel = (b.type for b in buttons[3:])
    assert user.button_id == 1 and isinstance(user.peer_type, raw.types.RequestPeerTypeUser)
    assert group.button_id == 2 and group.peer_type.forum
    assert channel.button_id == 3 and channel.max_quantity == 2
    assert isinstance(channel.peer_type, raw.types.RequestPeerTypeBroadcast)
    assert user.name_requested and user.username_requested and user.photo_requested

    # What the server sends back for request_peer buttons is buttonTypeRequestPeer.
    server_markup = raw.types.ReplyKeyboardMarkup(rows=[
        r.rows[0],
        r.rows[1],
        raw.types.KeyboardButtonRow(buttons=[
            raw.types.KeyboardButton(text="user", type=raw.types.ButtonTypeRequestPeer(
                button_id=1, peer_type=raw.types.RequestPeerTypeUser(bot=False), max_quantity=1)),
            raw.types.KeyboardButton(text="channel", type=raw.types.ButtonTypeRequestPeer(
                button_id=3, peer_type=raw.types.RequestPeerTypeBroadcast(), max_quantity=2)),
        ]),
    ], placeholder="pick")
    parsed = types.ReplyKeyboardMarkup.read(roundtrip(server_markup))
    (plain, geo), (app,), (user_b, channel_b) = parsed.keyboard

    assert plain == "plain"
    assert geo.text == "geo" and geo.request_location
    assert app.web_app.url == "https://example.org/app"
    assert user_b.request_user.is_bot is False
    assert isinstance(channel_b.request_chat, types.RequestPeerTypeChannel) and channel_b.request_chat.max == 2


def offline_client(peers, respond):
    """A Client that never connects: queries and results go through a TL round trip."""
    client = pyrogram.Client("layer-roundtrip", api_id=1, api_hash="x", in_memory=True)
    client.sent = []

    async def resolve_peer(peer_id):
        return peers[peer_id]

    async def invoke(query, *args, **kwargs):
        client.sent.append(roundtrip(query))
        result = respond(query)
        return roundtrip(result) if isinstance(result, TLObject) else result  # Bool results are plain bools

    client.resolve_peer = resolve_peer
    client.invoke = invoke
    return client


def call(make_coro):
    """Run a Client method inside a running loop, so the sync wrapper hands back a coroutine."""
    async def main():
        return await make_coro()
    return asyncio.run(main())


def empty_updates(updates=(), users=(), chats=()):
    return raw.types.Updates(updates=list(updates), users=list(users), chats=list(chats), date=0, seq=0)


CHANNEL = raw.types.InputPeerChannel(channel_id=123, access_hash=1)
BASIC_GROUP = raw.types.InputPeerChat(chat_id=45)
USER = raw.types.InputPeerUser(user_id=7, access_hash=2)
PEERS = {-100123: CHANNEL, -45: BASIC_GROUP, 7: USER, "me": raw.types.InputPeerSelf()}


@pytest.mark.parametrize("chat_id, peer", [(-100123, CHANNEL), (-45, BASIC_GROUP)])
def test_transfer_chat_ownership_uses_edit_chat_creator(monkeypatch, chat_id, peer):
    password_check = raw.types.InputCheckPasswordSRP(srp_id=1, A=b"a", M1=b"m")
    monkeypatch.setattr(pyrogram.utils, "compute_password_check", lambda r, password: password_check)

    def respond(query):
        if isinstance(query, raw.functions.account.GetPassword):
            return raw.types.account.Password(
                new_algo=raw.types.PasswordKdfAlgoUnknown(),
                new_secure_algo=raw.types.SecurePasswordKdfAlgoUnknown(),
                secure_random=b""
            )
        return empty_updates()

    client = offline_client(PEERS, respond)
    assert call(lambda: client.transfer_chat_ownership(chat_id, 7, "password")) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.EditChatCreator)
    assert request.peer.write() == peer.write()
    assert request.user_id.write() == USER.write()
    assert request.password.write() == password_check.write()


RAW_CHANNEL = raw.types.Channel(id=123, access_hash=1, title="Test", photo=raw.types.ChatPhotoEmpty(), date=0)
RAW_BOT = raw.types.User(id=99, bot=True, bot_info_version=1, first_name="Guard", access_hash=3)


@pytest.mark.parametrize("chat_id, request_type", [
    ("https://t.me/+AbCdEf0123456789", raw.functions.messages.ImportChatInvite),
    (-100123, raw.functions.channels.JoinChannel),
])
def test_join_chat_unwraps_chat_invite_join_result(chat_id, request_type):
    client = offline_client(PEERS, lambda q: raw.types.messages.ChatInviteJoinResultOk(
        updates=empty_updates(chats=[RAW_CHANNEL])
    ))
    chat = call(lambda: client.join_chat(chat_id))

    assert isinstance(client.sent[-1], request_type)
    assert isinstance(chat, types.Chat) and chat.id == pyrogram.utils.get_channel_id(123) and chat.title == "Test"


def test_join_chat_guard_bot_web_view_raises():
    client = offline_client(PEERS, lambda q: raw.types.messages.ChatInviteJoinResultWebView(
        bot_id=99, query_id=555, users=[RAW_BOT]
    ))
    with pytest.raises(pyrogram.errors.GuardBotApprovalRequired) as e:
        call(lambda: client.join_chat("https://t.me/+AbCdEf0123456789"))

    assert e.value.bot_id == 99 and e.value.query_id == 555
    assert isinstance(e.value.bot, types.User) and e.value.bot.id == 99 and e.value.bot.is_bot


def test_send_poll_layer_230_request():
    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.send_poll(
        -100123, "Q?", [types.PollOption("a"), types.PollOption("b")],
        type=pyrogram.enums.PollType.QUIZ, correct_option_id=1
    ))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.SendMedia)
    media = request.media
    assert isinstance(media, raw.types.InputMediaPoll)
    assert media.poll.hash == 0 and media.poll.quiz
    assert media.correct_answers == [1]
    assert all(isinstance(a, raw.types.InputPollAnswer) for a in media.poll.answers)
    assert [a.text.text for a in media.poll.answers] == ["a", "b"]


def poll_results(*voters):
    # voters and recent_voters share flags.2: the server sends both or neither
    return raw.types.PollResults(results=[
        raw.types.PollAnswerVoters(
            option=bytes([i]), voters=v, recent_voters=None if v is None else [], chosen=(i == 0)
        )
        for i, v in enumerate(voters)
    ])


def test_stop_poll_sends_hash_and_parses_optional_voters():
    raw_poll = raw.types.Poll(
        id=777, closed=True, hash=5,
        question=raw.types.TextWithEntities(text="Q?", entities=[]),
        answers=[
            raw.types.PollAnswer(text=raw.types.TextWithEntities(text=t, entities=[]), option=bytes([i]))
            for i, t in enumerate(("a", "b"))
        ],
    )
    client = offline_client(PEERS, lambda q: empty_updates(updates=[
        raw.types.UpdateMessagePoll(poll_id=777, poll=raw_poll, results=poll_results(3, None))
    ]))

    async def get_messages(chat_id, message_ids):
        return types.Message(id=message_ids, poll=types.Poll(
            id="777", question="Q?", options=[], total_voter_count=0, is_closed=False
        ))

    client.get_messages = get_messages
    poll = call(lambda: client.stop_poll(-100123, 10))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.EditMessage)
    assert request.media.poll.id == 777 and request.media.poll.closed and request.media.poll.hash == 0
    assert poll.is_closed and [o.text for o in poll.options] == ["a", "b"]
    assert [o.voter_count for o in poll.options] == [3, 0]
    assert poll.chosen_option_id == 0


def test_poll_update_without_voters():
    update = roundtrip(raw.types.UpdateMessagePoll(poll_id=1, results=poll_results(None, 2)))
    poll = asyncio.run(types.Poll._parse_update(None, update, {}))
    assert [o.voter_count for o in poll.options] == [0, 2]


@pytest.mark.parametrize("rarity, expected", [
    (raw.types.StarGiftAttributeRarity(permille=15), 15),
    (raw.types.StarGiftAttributeRarityLegendary(), None),
])
def test_gift_attribute_rarity(rarity, expected):
    attr = roundtrip(raw.types.StarGiftAttributeBackdrop(
        name="Night", backdrop_id=1, center_color=1, edge_color=2, pattern_color=3, text_color=4, rarity=rarity
    ))
    parsed = asyncio.run(types.GiftAttribute._parse(None, attr, {}))
    assert parsed.type == pyrogram.enums.GiftAttributeType.BACKDROP
    assert parsed.name == "Night" and parsed.center_color == 1
    assert parsed.rarity == expected


def test_bot_verification_description_with_entities():
    verification = roundtrip(raw.types.BotVerification(
        bot_id=99, icon=12345,
        description=raw.types.TextWithEntities(
            text="Verified by Guard",
            entities=[raw.types.MessageEntityBold(offset=12, length=5)]
        )
    ))
    parsed = types.BotVerification._parse(None, verification, {99: roundtrip(RAW_BOT)})

    assert isinstance(parsed.description, str) and parsed.description == "Verified by Guard"
    assert parsed.custom_emoji_id == 12345 and parsed.bot.id == 99
    [entity] = parsed.description_entities
    assert entity.type == pyrogram.enums.MessageEntityType.BOLD and (entity.offset, entity.length) == (12, 5)



@pytest.mark.parametrize("method, args, request_type", [
    ("close_forum_topic", (-100123, 5), raw.functions.messages.EditForumTopic),
    ("edit_general_topic", (-100123, "General"), raw.functions.messages.EditForumTopic),
    ("unhide_general_topic", (-100123,), raw.functions.messages.EditForumTopic),
    ("delete_forum_topic", (-100123, 5), raw.functions.messages.DeleteTopicHistory),
])
def test_forum_topic_methods_use_peer(method, args, request_type):
    client = offline_client(PEERS, lambda q: raw.types.messages.AffectedHistory(pts=1, pts_count=1, offset=0)
                            if isinstance(q, raw.functions.messages.DeleteTopicHistory) else empty_updates())
    assert call(lambda: getattr(client, method)(*args)) is True

    request = client.sent[-1]
    assert isinstance(request, request_type) and request.peer.write() == CHANNEL.write()


def test_update_color_sends_peer_color():
    client = offline_client(PEERS, lambda q: True)
    assert call(lambda: client.update_color("me", pyrogram.enums.ReplyColor.RED, background_emoji_id=10)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.account.UpdateColor)
    assert request.color.color == pyrogram.enums.ReplyColor.RED.value and request.color.background_emoji_id == 10


def test_gift_listing_maps_exclude_limited():
    client = offline_client(PEERS, lambda q: raw.types.payments.SavedStarGifts(count=5, gifts=[], chats=[], users=[]))
    assert call(lambda: client.get_chat_gifts_count(-100123, exclude_limited=True)) == 5

    request = client.sent[-1]
    assert isinstance(request, raw.functions.payments.GetSavedStarGifts)
    assert request.exclude_upgradable and request.exclude_unupgradable and not request.exclude_unlimited


def test_set_gift_resale_price_sends_stars_amount():
    client = offline_client(PEERS, lambda q: empty_updates())
    assert call(lambda: client.set_gift_resale_price("123", 100)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.payments.UpdateStarGiftPrice)
    assert request.stargift.msg_id == 123
    assert isinstance(request.resell_amount, raw.types.StarsAmount)
    assert (request.resell_amount.amount, request.resell_amount.nanos) == (100, 0)


@pytest.mark.parametrize("message_ids, expected", [([5, 6], [5, 6]), (5, 5)])
def test_delete_scheduled_messages(message_ids, expected):
    ids = message_ids if isinstance(message_ids, list) else [message_ids]
    client = offline_client(PEERS, lambda q: empty_updates(updates=[
        raw.types.UpdateDeleteScheduledMessages(peer=raw.types.PeerChannel(channel_id=123), messages=ids)
    ]))
    assert call(lambda: client.delete_scheduled_messages(-100123, message_ids)) == expected

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.DeleteScheduledMessages) and request.id == ids


@pytest.mark.parametrize("days, months", [(30, 1), (90, 3), (91, 3), (182, 6), (365, 12), (31, 0), (7, 0)])
def test_premium_duration_month_count(days, months):
    assert pyrogram.utils.get_premium_duration_month_count(days) == months


def test_gift_code_parsers_read_days():
    action = roundtrip(raw.types.MessageActionGiftCode(days=91, slug="abc", via_giveaway=True))
    gift_code = types.GiftCode._parse(None, action, {})
    assert (gift_code.months, gift_code.days, gift_code.slug, gift_code.is_giveaway) == (3, 91, "abc", True)

    checked = roundtrip(raw.types.payments.CheckedGiftCode(date=1, days=182, chats=[], users=[]))
    checked_gift_code = types.CheckedGiftCode._parse(None, checked, {}, {})
    assert (checked_gift_code.months, checked_gift_code.days) == (6, 182)


@pytest.mark.parametrize("member", [
    raw.types.ChannelParticipant(user_id=7, date=1, rank="tag"),
    raw.types.ChannelParticipantSelf(user_id=7, inviter_id=7, date=1, rank="tag"),
    raw.types.ChatParticipant(user_id=7, inviter_id=7, date=1, rank="tag"),
    raw.types.ChatParticipantAdmin(user_id=7, inviter_id=7, date=1, rank="tag"),
    raw.types.ChatParticipantCreator(user_id=7, rank="tag"),
    raw.types.ChannelParticipantBanned(peer=raw.types.PeerUser(user_id=7), kicked_by=7, date=1,
                                       banned_rights=raw.types.ChatBannedRights(until_date=0), rank="tag"),
])
def test_chat_member_rank_is_custom_title(member):
    users = {7: roundtrip(raw.types.User(id=7, access_hash=2, first_name="A"))}
    parsed = types.ChatMember._parse(None, roundtrip(member), users, {})
    assert parsed.custom_title == "tag" and parsed.user.id == 7


@pytest.mark.parametrize("kwargs, upgradable, unupgradable", [
    ({"exclude_upgradable": True}, True, False),
    ({"exclude_unupgradable": True}, False, True),
    ({"exclude_limited": True, "exclude_upgradable": False}, True, True),
])
def test_gift_listing_upgradable_filters(kwargs, upgradable, unupgradable):
    client = offline_client(PEERS, lambda q: raw.types.payments.SavedStarGifts(count=0, gifts=[], chats=[], users=[]))
    assert call(lambda: client.get_chat_gifts_count(-100123, **kwargs)) == 0

    request = client.sent[-1]
    assert (request.exclude_upgradable, request.exclude_unupgradable) == (upgradable, unupgradable)


def test_unique_gift_action_message_and_hidden_name():
    unique = raw.types.StarGiftUnique(
        id=1, gift_id=2, title="Helmet", slug="Helmet-1", num=1, availability_issued=1, availability_total=10,
        attributes=[raw.types.StarGiftAttributeBackdrop(
            name="Night", backdrop_id=1, center_color=1, edge_color=2, pattern_color=3, text_color=4,
            rarity=raw.types.StarGiftAttributeRarityRare()
        )]
    )
    message = roundtrip(raw.types.MessageService(
        id=10, peer_id=raw.types.PeerUser(user_id=7), date=1,
        action=raw.types.MessageActionStarGiftUnique(
            gift=unique, name_hidden=True,
            message=raw.types.TextWithEntities(text="for you", entities=[])
        )
    ))
    gift = asyncio.run(types.Gift._parse_action(None, message, {}, {}))

    assert gift.name == "Helmet-1" and gift.upgrade_message_id == 10
    assert gift.is_name_hidden is True and gift.caption == "for you"
    assert gift.attributes[0].rarity is None


@pytest.mark.parametrize("days, months", [(365, 12), (91, 3), (7, 0)])
def test_gifted_premium_reads_days(days, months):
    action = roundtrip(raw.types.MessageActionGiftPremium(currency="USD", amount=100, days=days))
    gifted = asyncio.run(types.GiftedPremium._parse(None, action, 7))
    assert (gifted.month_count, gifted.day_count, gifted.gifter_user_id) == (months, days, 7)


def parse_message(raw_message, users=()):
    client = pyrogram.Client("layer-roundtrip", api_id=1, api_hash="x", in_memory=True)
    client.me = None
    users = {u.id: u for u in (roundtrip(u) for u in users)}
    return asyncio.run(types.Message._parse(client, roundtrip(raw_message), users, {}, replies=0))


RAW_USER = raw.types.User(id=7, access_hash=2, first_name="A")
ET = pyrogram.enums.MessageEntityType


@pytest.mark.parametrize("entity, entity_type", [
    (raw.types.MessageEntityFormattedDate(offset=0, length=4, date=1647531900,
                                          long_date=True, long_time=True, day_of_week=True), ET.FORMATTED_DATE),
    (raw.types.MessageEntityTonAddress(offset=0, length=4), ET.TON_ADDRESS),
    (raw.types.MessageEntityDiffInsert(offset=0, length=4), ET.DIFF_INSERT),
    (raw.types.MessageEntityDiffReplace(offset=0, length=4, old_text="old"), ET.DIFF_REPLACE),
    (raw.types.MessageEntityDiffDelete(offset=0, length=4), ET.DIFF_DELETE),
])
def test_messages_with_layer_230_entities(entity, entity_type):
    message = parse_message(raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7),
        date=1, message="text", entities=[entity]
    ), users=[RAW_USER])
    [parsed] = message.entities
    assert parsed.type == entity_type and (parsed.offset, parsed.length) == (0, 4)

    # Re-sending the message (e.g. Message.copy) rebuilds the very same raw entity
    assert call(lambda: parsed.write()).write() == entity.write()


def test_unknown_entity_type_falls_back_to_unknown():
    class FutureEntity(raw.types.MessageEntityBold):  # stands for a constructor of a later layer
        pass

    parsed = types.MessageEntity._parse(None, FutureEntity(offset=0, length=1), {})
    assert parsed.type == ET.UNKNOWN


def test_formatted_date_html_and_markdown():
    from pyrogram.parser.html import HTML
    from pyrogram.parser.markdown import Markdown

    [entity] = asyncio.run(HTML(None).parse(
        '<tg-time unix="1647531900" format="wDT">tomorrow</tg-time>'))["entities"]
    assert isinstance(entity, raw.types.MessageEntityFormattedDate) and entity.date == 1647531900
    assert entity.day_of_week and entity.long_date and entity.long_time and not entity.relative

    # The syntax of the Bot API and TDLib: like custom emoji
    [entity] = asyncio.run(Markdown(None).parse("![soon](tg://time?unix=1647531900&format=r)"))["entities"]
    assert isinstance(entity, raw.types.MessageEntityFormattedDate) and entity.relative

    parsed = asyncio.run(Markdown(None).parse("at ![22:45](tg://time?unix=1647531900&format=t)", strict=True))
    assert parsed["message"] == "at 22:45"
    [entity] = parsed["entities"]
    assert (entity.offset, entity.length, entity.date, entity.short_time) == (3, 5, 1647531900, True)

    date = types.MessageEntity(type=ET.FORMATTED_DATE, offset=0, length=8, date=1647531900,
                               day_of_week=True, long_date=True, long_time=True)
    assert HTML.unparse("tomorrow", [date]) == '<tg-time unix="1647531900" format="wDT">tomorrow</tg-time>'
    markdown = Markdown.unparse("tomorrow", [date])
    assert markdown == "![tomorrow](tg://time?unix=1647531900&format=wDT)"
    [entity] = asyncio.run(Markdown(None).parse(markdown))["entities"]
    assert (entity.date, entity.day_of_week, entity.long_date, entity.long_time) == (1647531900, True, True, True)

    # Like in TDLib, a plain link to tg://time is just a link
    [entity] = asyncio.run(Markdown(None).parse("[soon](tg://time?unix=1647531900)"))["entities"]
    assert isinstance(entity, raw.types.MessageEntityTextUrl) and entity.url == "tg://time?unix=1647531900"


@pytest.mark.parametrize("text", ['<tg-time unix="0">x</tg-time>', '<tg-time unix="abc" format="t">x</tg-time>',
                                  '<tg-time format="r">x</tg-time>', '<tg-time unix="4294967296">x</tg-time>'])
def test_formatted_date_without_valid_unix_time_is_dropped(text):
    from pyrogram.parser.html import HTML

    # As TDLib does for <tg-time>
    assert asyncio.run(HTML(None).parse(text)) == {"message": "x", "entities": None}


@pytest.mark.parametrize("parse_mode, text", [
    ("html", '<tg-time unix="1647531900" format="x">bad</tg-time>'),
    ("markdown", "![bad](tg://time?unix=1647531900&format=x)"),
    ("markdown", "![bad](tg://time?unix=0&format=t)"),
    ("markdown", "![bad](tg://time?format=t)"),
])
def test_invalid_formatted_dates(parse_mode, text):
    from pyrogram.parser.html import HTML
    from pyrogram.parser.markdown import Markdown

    parser = HTML(None) if parse_mode == "html" else Markdown(None)
    with pytest.raises(ValueError):
        asyncio.run(parser.parse(text))


class FakeSession:
    """Stands in for Session so that the real Client.invoke() (and its peer caching) runs offline."""

    def __init__(self, respond):
        self.respond = respond
        self.sent = []

    async def invoke(self, query, *args, **kwargs):
        self.sent.append(roundtrip(query))
        return roundtrip(self.respond(query))


def test_join_chat_caches_the_joined_chat():
    client = pyrogram.Client("layer-roundtrip", api_id=1, api_hash="x", in_memory=True)
    client.session = FakeSession(lambda q: raw.types.messages.ChatInviteJoinResultOk(
        updates=empty_updates(chats=[RAW_CHANNEL])
    ))
    client.is_connected = True

    async def main():
        await client.storage.open()
        try:
            chat = await client.join_chat("https://t.me/+AbCdEf0123456789")
            return chat, await client.storage.get_peer_by_id(chat.id)
        finally:
            await client.storage.close()

    chat, peer = asyncio.run(main())
    assert isinstance(client.session.sent[-1], raw.functions.messages.ImportChatInvite)
    assert isinstance(peer, raw.types.InputPeerChannel) and (peer.channel_id, peer.access_hash) == (123, 1)


@pytest.mark.parametrize("title, rank", [(None, None), ("", None), ("Boss", "Boss")])
def test_promote_chat_member_keeps_rank_unless_title_given(title, rank):
    client = offline_client(PEERS, lambda q: empty_updates())
    assert call(lambda: client.promote_chat_member(-100123, 7, title=title)) is True

    [request] = client.sent  # no channels.getParticipant round trip any more
    assert isinstance(request, raw.functions.channels.EditAdmin) and request.rank == rank


BS = pyrogram.enums.ButtonStyle


def test_inline_button_style_icon_and_disabled_roundtrip():
    markup = types.InlineKeyboardMarkup([[
        types.InlineKeyboardButton("ok", callback_data="x", style=BS.SUCCESS, icon_custom_emoji_id=55),
        types.InlineKeyboardButton("off", is_disabled=True, style=BS.DANGER),
        types.InlineKeyboardButton("plain", url="https://telegram.org"),
        types.InlineKeyboardButtonBuy("pay", style=BS.PRIMARY),
    ]])
    r = write_markup(markup)
    ok, off, plain, pay = r.rows[0].buttons
    assert ok.style.bg_success and not ok.style.bg_danger and ok.style.icon == 55
    assert isinstance(off.type, raw.types.InlineButtonTypeDisabled) and off.style.bg_danger
    assert plain.style is None
    assert isinstance(pay.type, raw.types.InlineButtonTypeBuy) and pay.style.bg_primary

    # The server sends the same constructors back: read them and write them again (e.g. Message.copy)
    parsed = types.InlineKeyboardMarkup.read(r)
    ok, off, plain, pay = parsed.inline_keyboard[0]
    assert (ok.style, ok.icon_custom_emoji_id, ok.callback_data) == (BS.SUCCESS, 55, "x")
    assert off.is_disabled and off.style == BS.DANGER
    assert plain.style is None and plain.icon_custom_emoji_id is None
    assert isinstance(pay, types.InlineKeyboardButtonBuy) and pay.style == BS.PRIMARY
    assert write_markup(parsed).write() == r.write()


def test_inline_button_without_action_raises():
    with pytest.raises(ValueError):
        write_markup(types.InlineKeyboardMarkup([[types.InlineKeyboardButton("nothing")]]))


def test_inline_switch_inline_reads_other_chat_query():
    server_markup = raw.types.ReplyInlineMarkup(rows=[raw.types.KeyboardInlineButtonRow(buttons=[
        raw.types.KeyboardInlineButton(text="share", type=raw.types.InlineButtonTypeSwitchInline(query="q"))
    ])])
    [[button]] = types.InlineKeyboardMarkup.read(roundtrip(server_markup)).inline_keyboard
    assert button.switch_inline_query == "q" and button.switch_inline_query_current_chat is None


@pytest.mark.parametrize("poll_type, quiz", [(None, None), (pyrogram.enums.PollType.QUIZ, True),
                                             (pyrogram.enums.PollType.REGULAR, False)])
def test_reply_request_poll_button(poll_type, quiz):
    markup = types.ReplyKeyboardMarkup([[types.KeyboardButton(
        "poll", request_poll=types.KeyboardButtonPollType(type=poll_type)
    )]])
    r = write_markup(markup)
    [[button]] = [row.buttons for row in r.rows]
    assert isinstance(button.type, raw.types.ButtonTypeRequestPoll) and button.type.quiz == quiz

    [[parsed]] = types.ReplyKeyboardMarkup.read(r).keyboard
    assert parsed.request_poll.type == poll_type


def test_reply_request_managed_bot_button():
    markup = types.ReplyKeyboardMarkup([[types.KeyboardButton(
        "new bot", request_managed_bot=types.RequestPeerTypeManagedBot(
            button_id=9, suggested_name="Helper", suggested_username="helper_bot")
    )]])
    [[sent]] = [row.buttons for row in write_markup(markup).rows]
    assert isinstance(sent.type, raw.types.InputButtonTypeRequestPeer)
    assert sent.type.button_id == 9 and sent.type.max_quantity == 1
    assert sent.type.peer_type.bot_managed and sent.type.peer_type.suggested_username == "helper_bot"

    server_markup = raw.types.ReplyKeyboardMarkup(rows=[raw.types.KeyboardButtonRow(buttons=[
        raw.types.KeyboardButton(text="new bot", type=raw.types.ButtonTypeRequestPeer(
            button_id=9, max_quantity=1, peer_type=raw.types.RequestPeerTypeCreateBot(
                bot_managed=True, suggested_name="Helper", suggested_username="helper_bot")))
    ])])
    [[parsed]] = types.ReplyKeyboardMarkup.read(roundtrip(server_markup)).keyboard
    request = parsed.request_managed_bot
    assert (request.button_id, request.suggested_name, request.suggested_username) == (9, "Helper", "helper_bot")


def test_reply_keyboard_reads_styles_chats_and_unknown_buttons():
    server_markup = roundtrip(raw.types.ReplyKeyboardMarkup(rows=[raw.types.KeyboardButtonRow(buttons=[
        raw.types.KeyboardButton(text="red", type=raw.types.ButtonTypeDefault(),
                                 style=raw.types.KeyboardButtonStyle(bg_danger=True)),
        raw.types.KeyboardButton(text="group", type=raw.types.ButtonTypeRequestPeer(
            button_id=2, max_quantity=1, peer_type=raw.types.RequestPeerTypeChat(forum=True, creator=True))),
    ])]))
    server_markup.rows[0].buttons.append(raw.types.KeyboardButton(text="future", type=object()))
    red, group, future = types.ReplyKeyboardMarkup.read(server_markup).keyboard[0]

    assert isinstance(red, types.KeyboardButton) and red.text == "red" and red.style == BS.DANGER
    assert isinstance(group.request_chat, types.RequestPeerTypeChat)
    assert group.request_chat.is_forum and group.request_chat.is_creator
    assert isinstance(future, types.KeyboardButton) and future.text == "future"


def test_poll_results_are_matched_by_option():
    raw_poll = raw.types.Poll(
        id=1, hash=0, question=raw.types.TextWithEntities(text="Q?", entities=[]),
        answers=[raw.types.PollAnswer(text=raw.types.TextWithEntities(text=t, entities=[]), option=o)
                 for t, o in (("a", b"A"), ("b", b"B"), ("c", b"C"))]
    )
    # Results in another order than the answers, and one answer (added later) without results yet
    results = raw.types.PollResults(results=[
        raw.types.PollAnswerVoters(option=b"B", voters=5, recent_voters=[], chosen=True),
        raw.types.PollAnswerVoters(option=b"A", voters=2, recent_voters=[], correct=True),
    ])
    media = roundtrip(raw.types.MessageMediaPoll(poll=raw_poll, results=results))
    poll = asyncio.run(types.Poll._parse(None, media, {}))

    assert [(o.text, o.voter_count) for o in poll.options] == [("a", 2), ("b", 5), ("c", 0)]
    assert (poll.chosen_option_id, poll.correct_option_id) == (1, 0)


def test_delete_scheduled_messages_short_update():
    client = offline_client(PEERS, lambda q: raw.types.UpdateShort(
        update=raw.types.UpdateDeleteScheduledMessages(peer=raw.types.PeerChannel(channel_id=123), messages=[5]),
        date=0
    ))
    assert call(lambda: client.delete_scheduled_messages(-100123, 5)) == 5


def test_delete_scheduled_messages_too_many_updates():
    client = offline_client(PEERS, lambda q: raw.types.UpdatesTooLong())
    assert call(lambda: client.delete_scheduled_messages(-100123, [5, 6])) == []
    assert call(lambda: client.delete_scheduled_messages(-100123, 5)) is None


from types import SimpleNamespace  # noqa: E402

from pyrogram.file_id import FileId, FileType, ThumbnailSource  # noqa: E402

DOCUMENT_FILE_ID = FileId(file_type=FileType.DOCUMENT, dc_id=2, media_id=1, access_hash=2, file_reference=b"ref").encode()
PHOTO_FILE_ID = FileId(
    file_type=FileType.PHOTO, dc_id=2, media_id=3, access_hash=4, file_reference=b"ref",
    thumbnail_source=ThumbnailSource.THUMBNAIL, thumbnail_file_type=FileType.PHOTO, thumbnail_size="y",
    volume_id=0, local_id=0
).encode()


def channel_messages_updates(*ids):
    return empty_updates(updates=[
        raw.types.UpdateNewChannelMessage(
            message=raw.types.Message(id=i, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message=""),
            pts=1, pts_count=1
        ) for i in ids
    ], chats=[RAW_CHANNEL])


def test_media_group_and_paid_media_parse_sent_messages():
    client = offline_client(PEERS, lambda q: channel_messages_updates(10, 11))

    messages = call(lambda: client.send_media_group(
        -100123, [types.InputMediaDocument(DOCUMENT_FILE_ID), types.InputMediaDocument(DOCUMENT_FILE_ID)]))
    assert isinstance(client.sent[-1], raw.functions.messages.SendMultiMedia)
    assert [m.id for m in messages] == [10, 11]

    async def get_media_group(chat_id, message_id):
        return [types.Message(id=5, document=types.Document(file_id=DOCUMENT_FILE_ID, file_unique_id="u"))]

    client.get_media_group = get_media_group
    messages = call(lambda: client.copy_media_group(-100123, -100123, 5))
    assert isinstance(client.sent[-1], raw.functions.messages.SendMultiMedia)
    assert [m.id for m in messages] == [10, 11]

    messages = call(lambda: client.send_paid_media(-100123, 10, [types.InputMediaPhoto(PHOTO_FILE_ID)]))
    assert isinstance(client.sent[-1].media, raw.types.InputMediaPaidMedia)
    assert [m.id for m in messages] == [10, 11]


def test_gift_listings_map_exclude_limited():
    client = offline_client(PEERS, lambda q: raw.types.payments.SavedStarGifts(count=0, gifts=[], chats=[], users=[]))

    async def get_business_connection(connection_id):
        return SimpleNamespace(user=SimpleNamespace(id=7))

    client.get_business_connection = get_business_connection

    async def main():
        [g async for g in client.get_chat_gifts(-100123, exclude_limited=True)]
        [g async for g in client.get_business_account_gifts("conn", exclude_limited=True)]

    asyncio.run(main())
    for request in client.sent:
        assert isinstance(request, raw.functions.payments.GetSavedStarGifts)
        assert request.exclude_upgradable and request.exclude_unupgradable


def test_gift_parses_saved_star_gifts():
    unique = raw.types.StarGiftUnique(
        id=1, gift_id=2, title="Helmet", slug="Helmet-1", num=7, availability_issued=1, availability_total=10,
        attributes=[]
    )
    saved = roundtrip(raw.types.SavedStarGift(date=1, gift=unique, saved_id=3))
    gift = asyncio.run(types.Gift._parse(None, saved, {}, {}))
    assert gift.name == "Helmet-1" and gift.collectible_id == 7
