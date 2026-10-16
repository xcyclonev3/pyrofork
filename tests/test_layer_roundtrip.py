"""Offline round-trip tests: high-level -> raw -> TL bytes -> raw -> high-level."""
import asyncio
from io import BytesIO

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
