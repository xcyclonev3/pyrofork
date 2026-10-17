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

"""Ephemeral messages, welcome messages and their updates (Layer 228-230)."""

import asyncio

import pytest

import pyrogram
from pyrogram import enums, handlers, raw, types, utils
from tests.test_layer_roundtrip import (
    CHANNEL, PEERS, RAW_BOT, RAW_CHANNEL, RAW_USER, USER, call, empty_updates, offline_client, roundtrip
)

CHAT_ID = utils.get_channel_id(123)
INPUT_USER = raw.types.InputUser(user_id=7, access_hash=2)


def raw_ephemeral(**kwargs):
    fields = dict(id=5, from_id=raw.types.PeerUser(user_id=99), peer_id=raw.types.PeerChannel(channel_id=123),
                  receiver_id=7, date=1700000000, message="only for you")
    fields.update(kwargs)
    return raw.types.EphemeralMessage(**fields)


def new_client():
    client = pyrogram.Client("ephemeral", api_id=1, api_hash="x", in_memory=True)
    client.me = None
    return client


def parse(message, client=None, users=(RAW_USER, RAW_BOT), chats=(RAW_CHANNEL,)):
    client = client or new_client()
    users = {u.id: roundtrip(u) for u in users}
    chats = {c.id: roundtrip(c) for c in chats}
    return asyncio.run(types.EphemeralMessage._parse(client, roundtrip(message), users, chats))


def test_ephemeral_message_parsing():
    client = new_client()
    message = parse(raw_ephemeral(
        entities=[raw.types.MessageEntityBold(offset=0, length=4)], top_msg_id=3, chat_instance=-77, out=True,
        noforwards=True, anchor_msg_id=40,
        reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=4, reply_to_ephemeral=True),
        reply_markup=raw.types.ReplyInlineMarkup(rows=[raw.types.KeyboardInlineButtonRow(buttons=[
            raw.types.KeyboardInlineButton(text="ok", type=raw.types.InlineButtonTypeCallback(data=b"ok"))
        ])])
    ), client)

    assert (message.id, message.chat.id, message.chat.title) == (5, CHAT_ID, "Test")
    assert message.from_user.id == 99 and message.receiver.id == 7 and message.date.timestamp() == 1700000000
    assert message.text == "only for you" and message.entities[0].type == enums.MessageEntityType.BOLD
    assert message.media is None and message.caption is None
    assert (message.message_thread_id, message.reply_to_ephemeral_message_id, message.reply_to_message_id) == (3, 4, None)
    assert (message.anchor_message_id, message.chat_instance, message.outgoing, message.has_protected_content) == (
        40, "-77", True, True)
    assert message.reply_markup.inline_keyboard[0][0].callback_data == "ok"
    # The content is parsed as a placeholder message that must not stay in the message cache
    assert client.message_cache.store == {}


def test_ephemeral_media_message():
    photo = raw.types.Photo(id=2001, access_hash=66, file_reference=b"p", date=1, dc_id=2, sizes=[
        raw.types.PhotoSize(type="m", w=320, h=200, size=100)])
    message = parse(raw_ephemeral(media=raw.types.MessageMediaPhoto(photo=photo, spoiler=True), message="look",
                                  reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=9)))

    assert message.media == enums.MessageMediaType.PHOTO and message.photo.width == 320
    assert message.caption == "look" and message.text is None and message.has_media_spoiler is True
    assert message.reply_to_message_id == 9 and message.reply_to_ephemeral_message_id is None


def test_ephemeral_message_without_known_chat_or_receiver():
    message = parse(raw_ephemeral(receiver_id=555), chats=())

    assert message.chat.id == CHAT_ID and message.chat.type == enums.ChatType.SUPERGROUP
    assert message.receiver.id == 555


def test_welcome_templates_and_messages_without_chat_are_ignored():
    # Like TDLib, which doesn't show them
    assert parse(raw_ephemeral(welcome_template=True)) is None
    assert parse(raw_ephemeral(peer_id=None)) is None


def sent_ephemeral(update_type=raw.types.UpdateNewEphemeralMessage, **kwargs):
    return empty_updates([update_type(message=raw_ephemeral(out=True, **kwargs))], users=[RAW_USER, RAW_BOT],
                         chats=[RAW_CHANNEL])


EPHEMERAL_PEERS = {**PEERS, CHAT_ID: CHANNEL}


def test_send_ephemeral_message():
    client = offline_client(EPHEMERAL_PEERS, lambda q: sent_ephemeral())
    sent = call(lambda: client.send_ephemeral_message(
        CHAT_ID, 7, "<b>hi</b>", parse_mode=enums.ParseMode.HTML, reply_to_ephemeral_message_id=4,
        callback_query_id="123", replace_callback_query_message=True, protect_content=True,
        reply_markup=types.InlineKeyboardMarkup([[types.InlineKeyboardButton("ok", callback_data="ok")]])
    ))

    [request] = client.sent
    assert isinstance(request, raw.functions.ephemeral.SendMessage)
    assert request.peer == CHANNEL and request.receiver_id == INPUT_USER
    assert request.message == "hi" and isinstance(request.entities[0], raw.types.MessageEntityBold)
    assert request.reply_to == raw.types.InputReplyToEphemeralMessage(id=4)
    assert (request.query_id, request.anchor, request.noforwards, request.welcome) == (123, True, True, False)
    assert request.media is None and request.rich_message is None and request.random_id
    assert isinstance(request.reply_markup, raw.types.ReplyInlineMarkup)
    assert isinstance(sent, types.EphemeralMessage) and sent.id == 5 and sent.outgoing


def test_send_ephemeral_message_reply_to_a_message_in_a_topic():
    client = offline_client(EPHEMERAL_PEERS, lambda q: sent_ephemeral())
    call(lambda: client.send_ephemeral_message(CHAT_ID, 7, "hi", reply_to_message_id=10, message_thread_id=3))

    reply_to = client.sent[0].reply_to
    assert isinstance(reply_to, raw.types.InputReplyToMessage)
    assert (reply_to.reply_to_msg_id, reply_to.top_msg_id) == (10, 3)


def test_send_ephemeral_media_and_rich_messages():
    photo_file_id = types.Photo._parse(None, roundtrip(raw.types.Photo(
        id=2001, access_hash=66, file_reference=b"p", date=1, dc_id=2,
        sizes=[raw.types.PhotoSize(type="m", w=1, h=1, size=1)]))).file_id
    client = offline_client(EPHEMERAL_PEERS, lambda q: sent_ephemeral())

    call(lambda: client.send_ephemeral_message(
        CHAT_ID, 7, media=types.InputMediaPhoto(photo_file_id, caption="pic", has_spoiler=True), invert_media=True))
    call(lambda: client.send_ephemeral_message(
        CHAT_ID, 7, rich_message=types.InputRichMessage(types.RichMessageSourceHtml("<p>rich</p>"))))

    media_request, rich_request = client.sent
    assert isinstance(media_request.media, raw.types.InputMediaPhoto) and media_request.media.spoiler
    assert media_request.media.id.id == 2001 and media_request.message == "pic" and media_request.invert_media
    assert isinstance(rich_request.rich_message, raw.types.InputRichMessageHTML) and rich_request.message == ""


@pytest.mark.parametrize("kwargs", [{}, dict(text="a", media=types.InputMediaDocument("x"))])
def test_send_ephemeral_message_needs_one_content(kwargs):
    client = offline_client(EPHEMERAL_PEERS, lambda q: sent_ephemeral())
    with pytest.raises(ValueError):
        call(lambda: client.send_ephemeral_message(CHAT_ID, 7, **kwargs))
    assert client.sent == []


def test_edit_ephemeral_message():
    client = offline_client(EPHEMERAL_PEERS, lambda q: sent_ephemeral(raw.types.UpdateEditEphemeralMessage))
    edited = call(lambda: client.edit_ephemeral_message(CHAT_ID, 7, 5, "new text"))
    call(lambda: client.edit_ephemeral_message(
        CHAT_ID, 7, 5, reply_markup=types.InlineKeyboardMarkup([[types.InlineKeyboardButton("b", callback_data="b")]])))
    call(lambda: client.edit_ephemeral_message_caption(CHAT_ID, 7, 5, "caption", invert_media=True))

    text_request, markup_request, caption_request = client.sent
    assert isinstance(text_request, raw.functions.ephemeral.EditMessage)
    assert (text_request.id, text_request.receiver_id, text_request.message) == (5, INPUT_USER, "new text")
    assert markup_request.message is None and markup_request.reply_markup is not None
    assert caption_request.message == "caption" and caption_request.invert_media is True
    assert isinstance(edited, types.EphemeralMessage) and edited.id == 5


def test_delete_ephemeral_message_and_callback_answers():
    def respond(query):
        if isinstance(query, raw.functions.ephemeral.GetCallbackAnswer):
            return raw.types.messages.BotCallbackAnswer(cache_time=0, message="pressed")
        return True

    client = offline_client(EPHEMERAL_PEERS, respond)
    assert call(lambda: client.delete_ephemeral_message(CHAT_ID, 7, 5)) is True
    answer = call(lambda: client.request_ephemeral_callback_answer(CHAT_ID, 5, "ok"))

    delete, get_answer = client.sent
    assert isinstance(delete, raw.functions.ephemeral.DeleteMessage)
    assert (delete.peer, delete.receiver_id, delete.id) == (CHANNEL, INPUT_USER, 5)
    assert (get_answer.peer, get_answer.id, get_answer.data) == (CHANNEL, 5, b"ok")
    assert answer.message == "pressed"


@pytest.mark.parametrize("result, check", [
    (raw.types.ReportResultChooseOption(title="Why?", options=[raw.types.MessageReportOption(text="Spam", option=b"1")]),
     lambda r: r.type == enums.ReportResultType.OPTION_REQUIRED and r.title == "Why?" and r.options[0].id == b"1"),
    (raw.types.ReportResultAddComment(option=b"1", optional=True),
     lambda r: r.type == enums.ReportResultType.TEXT_REQUIRED and r.option_id == b"1" and r.is_text_optional),
    (raw.types.ReportResultReported(), lambda r: r.type == enums.ReportResultType.REPORTED),
])
def test_report_ephemeral_message(result, check):
    client = offline_client(EPHEMERAL_PEERS, lambda q: result)
    report = call(lambda: client.report_ephemeral_message(CHAT_ID, 5, b"1", "details"))

    [request] = client.sent
    assert isinstance(request, raw.functions.ephemeral.ReportMessage)
    assert (request.peer, request.id, request.option, request.message) == (CHANNEL, 5, b"1", "details")
    assert check(report)


def test_welcome_messages():
    def respond(query):
        if isinstance(query, raw.functions.ephemeral.GetWelcomeMessages):
            return raw.types.ephemeral.WelcomeMessages(hash=1, messages=[
                raw_ephemeral(id=1, welcome_template=True, message="Welcome!", peer_id=None),
                raw_ephemeral(id=2, welcome_template=True, message="Rules", peer_id=None),
            ])
        if isinstance(query, raw.functions.ephemeral.SendMessage):
            return empty_updates()
        if isinstance(query, raw.functions.ephemeral.EditMessage):
            return empty_updates()
        return True

    client = offline_client(EPHEMERAL_PEERS, respond)
    welcome = call(lambda: client.get_chat_welcome_messages(CHAT_ID))
    assert call(lambda: client.add_chat_welcome_message(CHAT_ID, "Hello **newcomer**")) is True
    assert call(lambda: client.edit_chat_welcome_message(CHAT_ID, 2, "Read the rules")) is True
    assert call(lambda: client.delete_chat_welcome_message(CHAT_ID, 1)) is True
    assert call(lambda: client.delete_all_chat_welcome_messages(CHAT_ID)) is True

    assert [(m.id, m.text) for m in welcome] == [(1, "Welcome!"), (2, "Rules")]
    get, add, edit, delete, delete_all = client.sent
    assert get.peer == CHANNEL and get.hash == 0
    assert add.welcome is True and add.receiver_id == raw.types.InputUserEmpty() and add.message == "Hello newcomer"
    assert edit.welcome is True and edit.id == 2 and edit.message == "Read the rules"
    assert isinstance(delete, raw.functions.ephemeral.DeleteWelcomeMessage) and delete.id == 1
    assert isinstance(delete_all, raw.functions.ephemeral.DeleteAllWelcomeMessages) and delete_all.peer == CHANNEL


def dispatch(update, users=(RAW_USER, RAW_BOT), chats=(RAW_CHANNEL,)):
    client = new_client()
    update = roundtrip(update)
    parser = client.dispatcher.update_parsers[type(update)]
    users = {u.id: roundtrip(u) for u in users}
    chats = {c.id: roundtrip(c) for c in chats}
    return asyncio.run(parser(update, users, chats))


@pytest.mark.parametrize("update_type, handler", [
    (raw.types.UpdateNewEphemeralMessage, handlers.EphemeralMessageHandler),
    (raw.types.UpdateEditEphemeralMessage, handlers.EditedEphemeralMessageHandler),
])
def test_ephemeral_message_updates(update_type, handler):
    parsed, handler_type = dispatch(update_type(message=raw_ephemeral()))
    assert handler_type is handler and parsed.text == "only for you"

    # Welcome templates are not delivered
    parsed, handler_type = dispatch(update_type(message=raw_ephemeral(welcome_template=True)))
    assert parsed is None and handler_type is type(None)


def test_deleted_ephemeral_messages_update():
    parsed, handler_type = dispatch(raw.types.UpdateDeleteEphemeralMessages(
        peer=raw.types.PeerChannel(channel_id=123), ids=[5, 6]))

    assert handler_type is handlers.DeletedEphemeralMessagesHandler
    assert [(m.id, m.chat.id) for m in parsed] == [(5, CHAT_ID), (6, CHAT_ID)]


def test_ephemeral_callback_query():
    parsed, handler_type = dispatch(raw.types.UpdateEphemeralBotCallbackQuery(
        query_id=77, user_id=7, msg_id=5, data=b"ok", message=raw_ephemeral()))

    assert handler_type is handlers.CallbackQueryHandler
    assert parsed.id == "77" and parsed.data == "ok" and parsed.message is None and parsed.chat_instance is None
    assert parsed.ephemeral_message.id == 5 and parsed.ephemeral_message.chat.id == CHAT_ID


def test_callback_query_bound_methods_with_ephemeral_messages():
    query, _ = dispatch(raw.types.UpdateEphemeralBotCallbackQuery(
        query_id=77, user_id=7, msg_id=5, data=b"ok", chat_instance=3, message=raw_ephemeral()))
    calls = []

    async def fake(name, **kwargs):
        calls.append((name, kwargs))

    query._client.edit_ephemeral_message = lambda **kwargs: fake("edit", **kwargs)
    query._client.send_ephemeral_message = lambda **kwargs: fake("send", **kwargs)
    query.ephemeral_message._client = query._client

    call(lambda: query.edit_message_text("pressed"))
    call(lambda: query.edit_message_reply_markup(None))
    call(lambda: query.reply_with_ephemeral_message("details", replace_message=True))

    (edit_name, edit_text), (_, edit_markup), (send_name, send) = calls
    assert edit_name == "edit" and edit_text["text"] == "pressed" and edit_text["receiver_user_id"] == 7
    assert edit_markup["text"] is None and edit_markup["ephemeral_message_id"] == 5
    assert send_name == "send" and send["callback_query_id"] == "77" and send["replace_callback_query_message"]
    assert (send["chat_id"], send["receiver_user_id"], send["text"]) == (CHAT_ID, 7, "details")


def test_ephemeral_message_bound_methods():
    message = parse(raw_ephemeral())
    calls = []

    async def fake(name, **kwargs):
        calls.append((name, kwargs))

    message._client.send_ephemeral_message = lambda **kwargs: fake("send", **kwargs)
    message._client.edit_ephemeral_message = lambda **kwargs: fake("edit", **kwargs)
    message._client.delete_ephemeral_message = lambda **kwargs: fake("delete", **kwargs)

    call(lambda: message.reply("thanks"))
    call(lambda: message.edit("changed"))
    call(lambda: message.delete())

    (_, reply), (_, edit), (_, delete) = calls
    # A reply goes to the sender of the message; edits and deletions concern its receiver
    assert (reply["receiver_user_id"], reply["reply_to_ephemeral_message_id"], reply["text"]) == (99, 5, "thanks")
    assert (edit["receiver_user_id"], edit["ephemeral_message_id"], edit["text"]) == (7, 5, "changed")
    assert delete == dict(chat_id=CHAT_ID, receiver_user_id=7, ephemeral_message_id=5)


def test_messages_replying_to_ephemeral_messages():
    from tests.test_layer_roundtrip import parse_message

    message = parse_message(raw.types.Message(
        id=8, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7), date=1, message="hi",
        reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=5, reply_to_ephemeral=True)
    ), users=[RAW_USER])

    # parse_message parses without fetching replies; the replied id must not be taken for a message of the chat
    assert message.reply_to_ephemeral_message_id == 5 and message.reply_to_message_id is None


def test_ephemeral_bot_commands():
    command = types.BotCommand("secret", "Only the bot sees it", is_ephemeral=True)
    written = roundtrip(command.write())

    assert written.ephemeral is True
    assert types.BotCommand.read(written).is_ephemeral is True
    assert roundtrip(types.BotCommand("start", "Start").write()).ephemeral is False


def test_forward_ephemeral_messages():
    client = offline_client(EPHEMERAL_PEERS, lambda q: empty_updates())
    call(lambda: client.forward_messages("me", CHAT_ID, [5], from_ephemeral=True))

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.ForwardMessages) and request.from_ephemeral is True


def test_chat_has_welcome_messages():
    group = raw.types.Chat(id=45, title="Group", photo=raw.types.ChatPhotoEmpty(), participants_count=2, date=0,
                           version=1)
    full = raw.types.messages.ChatFull(
        full_chat=raw.types.ChatFull(
            id=45, about="", participants=raw.types.ChatParticipantsForbidden(chat_id=45),
            notify_settings=raw.types.PeerNotifySettings(), has_welcome_messages=True),
        chats=[group], users=[])
    client = offline_client(PEERS, lambda q: full)
    chat = call(lambda: client.get_chat(-45))

    assert isinstance(client.sent[0], raw.functions.messages.GetFullChat)
    assert chat.has_welcome_messages is True


# Review fixes

def test_deleted_ephemeral_messages_filters():
    from pyrogram import filters

    messages = types.List([types.EphemeralMessage(id=5, chat=types.Chat(id=CHAT_ID, type=enums.ChatType.SUPERGROUP))])
    client = new_client()

    matching = handlers.DeletedEphemeralMessagesHandler(lambda c, m: None, filters.chat(CHAT_ID))
    other = handlers.DeletedEphemeralMessagesHandler(lambda c, m: None, filters.chat(-1001))
    assert asyncio.run(matching.check(client, messages)) is True
    assert asyncio.run(other.check(client, messages)) is False


def test_ephemeral_callback_queries_without_chat_are_dropped():
    parsed, handler_type = dispatch(raw.types.UpdateEphemeralBotCallbackQuery(
        query_id=77, user_id=7, msg_id=5, data=b"ok", message=raw_ephemeral(peer_id=None)))

    assert parsed is None and handler_type is type(None)


def test_reply_to_ephemeral_messages_of_chats():
    message = parse(raw_ephemeral(from_id=raw.types.PeerChannel(channel_id=123)))

    assert message.from_user is None and message.sender_chat.id == CHAT_ID
    with pytest.raises(ValueError):
        call(lambda: message.reply("hi"))
