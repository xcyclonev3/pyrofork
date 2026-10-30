#  Pyrogram - Telegram MTProto API Client Library for Python
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

"""What reading a topic or a replied message does to updates, with the real Dispatcher, Client.invoke and Session.invoke."""

import asyncio
from io import BytesIO
from types import SimpleNamespace

import pytest

import pyrogram
from pyrogram import enums, errors, filters, raw, types, utils
from pyrogram.handlers import MessageHandler
from pyrogram.raw.core import TLObject
from pyrogram.session import Session

CHANNEL_ID = 123
CHAT_ID = utils.get_channel_id(CHANNEL_ID)
RAW_FORUM = raw.types.Channel(
    id=CHANNEL_ID, access_hash=1, title="t", photo=raw.types.ChatPhotoEmpty(), date=0, megagroup=True, forum=True,
    usernames=[], restriction_reason=[]
)
RAW_USER = raw.types.User(id=7, access_hash=2, first_name="u", usernames=[], restriction_reason=[])
IN_TOPIC = raw.types.MessageReplyHeader(reply_to_msg_id=7, forum_topic=True)
IN_OTHER_TOPIC = raw.types.MessageReplyHeader(reply_to_msg_id=8, forum_topic=True)
REPLY = raw.types.MessageReplyHeader(reply_to_msg_id=9)
AUTH_KEY = bytes(range(256))


def wire(obj):
    return TLObject.read(BytesIO(obj.write()))


def raw_topic(topic_id=7, title="x"):
    return raw.types.ForumTopic(
        id=topic_id, date=1, peer=raw.types.PeerChannel(channel_id=CHANNEL_ID), title=title, icon_color=1, top_message=1,
        read_inbox_max_id=0, read_outbox_max_id=0, unread_count=0, unread_mentions_count=0, unread_reactions_count=0,
        unread_poll_votes_count=0, from_id=raw.types.PeerChannel(channel_id=CHANNEL_ID),
        notify_settings=raw.types.PeerNotifySettings()
    )


def raw_message(message_id=1, reply_to=None):
    return raw.types.Message(
        id=message_id, peer_id=raw.types.PeerChannel(channel_id=CHANNEL_ID), date=1, message=f"m{message_id}", reply_to=reply_to
    )


def read_answer(query, topics=(7, 8), title="x"):
    """The messages asked for: even ones are in the other topic, and replied messages are always 9."""
    return wire(raw.types.messages.ChannelMessages(
        pts=1, count=len(query.id), topics=[raw_topic(i, title) for i in topics], chats=[RAW_FORUM], users=[RAW_USER],
        messages=[
            raw_message(ref.id, IN_OTHER_TOPIC if ref.id % 2 == 0 else IN_TOPIC)
            if isinstance(ref, raw.types.InputMessageID) else raw_message(9, IN_TOPIC)
            for ref in query.id
        ]
    ))


class Telegram:
    """Behind Session.send: answers reads, or raises what it's told to."""

    def __init__(self):
        self.reads = 0
        self.error = None
        self.title = "x"

    async def send(self, query, *args, **kwargs):
        assert isinstance(query, raw.functions.channels.GetMessages), type(query)
        self.reads += 1

        if self.error:
            raise self.error

        return read_answer(query, title=self.title)


@pytest.fixture
def telegram():
    return Telegram()


async def new_client(telegram, workers=4):
    client = pyrogram.Client("reads", api_id=1, api_hash="x", in_memory=True, workers=workers)
    await client.storage.open()
    client.is_connected = True
    client.me = types.User(id=999, is_self=True)
    session = Session(client, 2, AUTH_KEY, False)
    session.is_started.set()
    session.send = telegram.send
    client.session = session
    await client.fetch_peers([RAW_FORUM, RAW_USER])
    return client


def run(telegram, scenario, workers=4):
    async def main():
        client = await new_client(telegram, workers)
        try:
            return await scenario(client)
        finally:
            await client.storage.close()

    return asyncio.run(main())


async def from_update(client, message):
    update = raw.types.UpdateNewChannelMessage(message=wire(message), pts=1, pts_count=1)
    parsed, _ = await client.dispatcher.update_parsers[type(update)](update, {7: RAW_USER}, {CHANNEL_ID: RAW_FORUM})
    return parsed


def test_the_topic_is_read_once_for_all_messages_of_the_topic(telegram):
    async def scenario(client):
        return [await from_update(client, raw_message(i, IN_TOPIC)) for i in range(1, 11)]

    messages = run(telegram, scenario)

    assert telegram.reads == 1
    assert [m.topic.id for m in messages] == [7] * 10 and [m.message_thread_id for m in messages] == [7] * 10


def test_each_topic_is_read_on_its_own(telegram):
    async def scenario(client):
        first = await from_update(client, raw_message(1, IN_TOPIC))
        other = await from_update(client, raw_message(2, IN_OTHER_TOPIC))
        return first, other

    first, other = run(telegram, scenario)

    assert telegram.reads == 2 and (first.topic.id, other.topic.id) == (7, 8)


def test_a_topic_without_data_is_not_read_for_each_message(telegram):
    async def answer_without_topics(query, *args, **kwargs):
        telegram.reads += 1
        return read_answer(query, topics=())

    telegram.send = answer_without_topics

    async def scenario(client):
        return [await from_update(client, raw_message(i, IN_TOPIC)) for i in range(1, 4)]

    assert [m.topic for m in run(telegram, scenario)] == [None] * 3 and telegram.reads == 1


def test_the_topic_is_read_again_after_a_while(telegram, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(utils, "time", SimpleNamespace(monotonic=lambda: now[0]))

    async def scenario(client):
        await from_update(client, raw_message(1, IN_TOPIC))
        now[0] += utils.TOPIC_CACHE_SECONDS - 1
        await from_update(client, raw_message(2, IN_TOPIC))
        reads_before = telegram.reads
        now[0] += 2
        telegram.title = "new"
        return reads_before, await from_update(client, raw_message(3, IN_TOPIC))

    reads_before, message = run(telegram, scenario)

    assert reads_before == 1 and telegram.reads == 2 and message.topic.title == "new"


@pytest.mark.parametrize("action", [
    raw.types.MessageActionTopicEdit(title="new"),
    raw.types.MessageActionTopicEdit(closed=True),
    raw.types.MessageActionTopicEdit(closed=False),
], ids=["edited", "closed", "reopened"])
def test_a_message_saying_the_topic_changed_makes_it_read_again(telegram, action):
    async def scenario(client):
        await from_update(client, raw_message(1, IN_TOPIC))
        telegram.title = "new"
        await from_update(client, raw.types.MessageService(
            id=2, peer_id=raw.types.PeerChannel(channel_id=CHANNEL_ID), date=1, reply_to=IN_TOPIC, action=action
        ))
        return await from_update(client, raw_message(3, IN_TOPIC))

    message = run(telegram, scenario)

    assert telegram.reads == 2 and message.topic.title == "new"


async def handled_after(client, *messages):
    """Send updates through the dispatcher and return when the handler got the last one, or fail."""
    last = messages[-1].id
    seen = []
    done = asyncio.Event()

    async def on_message(c, m):
        seen.append(m)
        if m.id == last:
            done.set()

    client.add_handler(MessageHandler(on_message))
    await client.dispatcher.start()

    for message in messages:
        client.dispatcher.updates_queue.put_nowait((
            raw.types.UpdateNewChannelMessage(message=wire(message), pts=1, pts_count=1), {7: RAW_USER}, {CHANNEL_ID: RAW_FORUM}
        ))

    try:
        await asyncio.wait_for(done.wait(), 2)
    finally:
        await client.dispatcher.stop()

    return seen


@pytest.mark.parametrize("flood", [errors.FloodWait(value=3600), errors.FloodPremiumWait(value=3600)], ids=["FloodWait", "FloodPremiumWait"])
@pytest.mark.parametrize("reply_to", [IN_TOPIC, REPLY], ids=["topic", "reply"])
def test_a_long_flood_wait_does_not_stop_the_workers(telegram, flood, reply_to):
    telegram.error = flood

    async def scenario(client):
        return await handled_after(client, *[raw_message(i, reply_to) for i in range(1, 9)], raw_message(100))

    seen = run(telegram, scenario, workers=2)

    assert {m.id for m in seen} == {*range(1, 9), 100}
    assert all(m.topic is None and m.reply_to_message is None for m in seen)


def test_nothing_is_read_until_a_long_flood_wait_is_over(telegram, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(utils, "time", SimpleNamespace(monotonic=lambda: now[0]))
    telegram.error = errors.FloodWait(value=3600)

    async def scenario(client):
        first = await from_update(client, raw_message(1, REPLY))
        second = await from_update(client, raw_message(2, REPLY))
        reads_during = telegram.reads
        now[0] += 3601
        telegram.error = None
        return first, second, reads_during, await from_update(client, raw_message(3, REPLY))

    first, second, reads_during, third = run(telegram, scenario)

    assert reads_during == 1 and first.reply_to_message is None and second.reply_to_message is None
    assert first.reply_to_message_id == 9 and second.reply_to_message_id == 9
    assert third.reply_to_message.id == 9


def test_a_short_flood_wait_is_waited_for_a_replied_message(telegram, monkeypatch):
    real_sleep = asyncio.sleep
    slept = []

    async def fast_sleep(seconds, *args, **kwargs):
        slept.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    telegram.error = errors.FloodWait(value=5)
    send = telegram.send

    async def flood_once(query, *args, **kwargs):
        try:
            return await send(query, *args, **kwargs)
        finally:
            telegram.error = None

    telegram.send = flood_once

    async def scenario(client):
        return await from_update(client, raw_message(1, REPLY))

    message = run(telegram, scenario)

    assert message.reply_to_message.id == 9 and 5 in slept and telegram.reads == 2


def test_a_short_flood_wait_on_a_topic_stops_neither_the_message_nor_the_replies(telegram):
    telegram.error = errors.FloodWait(value=5)

    async def scenario(client):
        in_topic = await from_update(client, raw_message(1, IN_TOPIC))
        again = await from_update(client, raw_message(2, IN_TOPIC))
        reads_for_the_topic = telegram.reads
        telegram.error = None
        return in_topic, again, reads_for_the_topic, await from_update(client, raw_message(3, REPLY))

    in_topic, again, reads_for_the_topic, reply = run(telegram, scenario)

    assert in_topic.topic is None and again.topic is None and reads_for_the_topic == 1
    assert reply.reply_to_message.id == 9


@pytest.mark.parametrize("error", [
    errors.ChannelPrivate(), errors.MessageIdsEmpty(), errors.PeerIdInvalid(), errors.InternalServerError("x"),
    OSError("reset"), ConnectionError("closed"), TimeoutError("late"), asyncio.TimeoutError(),
], ids=lambda e: type(e).__name__)
@pytest.mark.parametrize("reply_to", [IN_TOPIC, REPLY], ids=["topic", "reply"])
def test_an_error_while_reading_does_not_lose_the_update(telegram, error, reply_to):
    telegram.error = error

    async def scenario(client):
        invoke = client.invoke

        async def invoke_without_retries(query, **kwargs):
            return await invoke(query, 0, **kwargs)

        client.invoke = invoke_without_retries
        return await handled_after(client, raw_message(1, reply_to))

    [message] = run(telegram, scenario)

    assert message.id == 1 and message.topic is None and message.reply_to_message is None


def thresholds_of_reads(client):
    """The sleep threshold each read reaches the session with."""
    thresholds = []
    invoke = client.session.invoke

    async def remember_threshold(query, retries, timeout, sleep_threshold):
        thresholds.append(sleep_threshold)
        return await invoke(query, retries, timeout, sleep_threshold)

    client.session.invoke = remember_threshold
    return thresholds


@pytest.mark.parametrize("kwargs, expected", [
    ({}, -1),
    ({"sleep_threshold": -1}, -1),
    ({"sleep_threshold": None}, Session.SLEEP_THRESHOLD),
    ({"sleep_threshold": 0}, 0),
    ({"sleep_threshold": 7}, 7),
], ids=["default", "-1", "None", "0", "7"])
def test_get_messages_passes_on_the_sleep_threshold(telegram, kwargs, expected):
    async def scenario(client):
        thresholds = thresholds_of_reads(client)
        await client.get_messages(CHAT_ID, 1, replies=0, **kwargs)
        return thresholds

    assert run(telegram, scenario) == [expected]


def test_get_messages_uses_the_same_sleep_threshold_for_replies(telegram):
    async def answer_with_a_reply(query, *args, **kwargs):
        return wire(raw.types.messages.ChannelMessages(
            pts=1, count=1, topics=[], chats=[RAW_FORUM], users=[RAW_USER],
            messages=[raw_message(1, REPLY) if isinstance(ref, raw.types.InputMessageID) else raw_message(9) for ref in query.id]
        ))

    telegram.send = answer_with_a_reply

    async def scenario(client):
        thresholds = thresholds_of_reads(client)
        return await client.get_messages(CHAT_ID, 1, sleep_threshold=5), thresholds

    message, thresholds = run(telegram, scenario)

    assert message.reply_to_message.id == 9 and thresholds == [5, 5]


@pytest.mark.parametrize("reply_to, threshold", [(IN_TOPIC, 0), (REPLY, Session.SLEEP_THRESHOLD)], ids=["topic", "reply"])
def test_reads_for_an_update_have_a_limit_on_flood_waits(telegram, reply_to, threshold):
    async def scenario(client):
        thresholds = thresholds_of_reads(client)
        await from_update(client, raw_message(1, reply_to))
        return thresholds

    assert run(telegram, scenario) == [threshold]


def test_get_messages_still_waits_for_all_flood_waits_by_default(telegram, monkeypatch):
    real_sleep = asyncio.sleep
    slept = []

    async def fast_sleep(seconds, *args, **kwargs):
        slept.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    telegram.error = errors.FloodWait(value=3600)
    send = telegram.send

    async def flood_once(query, *args, **kwargs):
        try:
            return await send(query, *args, **kwargs)
        finally:
            telegram.error = None

    telegram.send = flood_once

    async def scenario(client):
        return await client.get_messages(CHAT_ID, 1, replies=0)

    assert run(telegram, scenario).id == 1 and 3600 in slept


def test_a_callback_query_is_not_lost_when_its_message_cant_be_read(telegram):
    telegram.error = errors.FloodWait(value=3600)
    query = raw.types.UpdateBotCallbackQuery(
        query_id=1, user_id=7, peer=raw.types.PeerChannel(channel_id=CHANNEL_ID), msg_id=1, chat_instance=1, data=b"d"
    )

    async def scenario(client):
        return await types.CallbackQuery._parse(client, query, {7: RAW_USER})

    callback_query = run(telegram, scenario)

    assert callback_query.data == "d"
    assert callback_query.message.id == 1 and callback_query.message.chat.id == CHAT_ID


def test_a_pin_says_which_message_it_pinned_when_it_cant_be_read(telegram):
    telegram.error = errors.FloodWait(value=3600)

    async def scenario(client):
        return await from_update(client, raw.types.MessageService(
            id=2, peer_id=raw.types.PeerChannel(channel_id=CHANNEL_ID), date=1, action=raw.types.MessageActionPinMessage(),
            reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=9)
        ))

    message = run(telegram, scenario)

    assert message.service == enums.MessageServiceType.PINNED_MESSAGE
    assert message.pinned_message.id == 9 and message.pinned_message.chat.id == CHAT_ID


def test_a_deleted_message_says_nothing_about_its_topic(telegram):
    answers = [
        lambda query: wire(raw.types.messages.ChannelMessages(
            pts=1, count=1, topics=[], chats=[RAW_FORUM], users=[RAW_USER], messages=[raw.types.MessageEmpty(id=1)]
        )),
        read_answer
    ]

    async def send(query, *args, **kwargs):
        telegram.reads += 1
        return answers.pop(0)(query)

    telegram.send = send

    async def scenario(client):
        return await from_update(client, raw_message(1, IN_TOPIC)), await from_update(client, raw_message(3, IN_TOPIC))

    deleted, visible = run(telegram, scenario)

    assert deleted.topic is None and visible.topic.id == 7 and telegram.reads == 2


TOPIC_FILTER_CASES = [
    (IN_TOPIC, 7, True),
    (IN_TOPIC, 1, False),
    (IN_TOPIC, [1, 7], True),
    (raw.types.MessageReplyHeader(reply_to_msg_id=9, reply_to_top_id=7, forum_topic=True), 7, True),
    (None, 1, True),
    (None, 7, False),
    (REPLY, 1, True),
]


@pytest.mark.parametrize("reply_to, topics, expected", TOPIC_FILTER_CASES)
@pytest.mark.parametrize("error", [None, errors.FloodWait(value=3600), errors.ChannelPrivate()], ids=["read", "flood wait", "error"])
def test_the_topic_filter_does_not_depend_on_reading_the_topic(telegram, reply_to, topics, expected, error):
    telegram.error = error

    async def scenario(client):
        message = await from_update(client, raw_message(1, reply_to))
        return await filters.topic(topics)(client, message)

    assert bool(run(telegram, scenario)) is expected


def test_the_topic_filter_in_a_group_without_topics():
    chat = raw.types.Channel(
        id=CHANNEL_ID, access_hash=1, title="t", photo=raw.types.ChatPhotoEmpty(), date=0, megagroup=True,
        usernames=[], restriction_reason=[]
    )

    async def main():
        client = pyrogram.Client("reads", api_id=1, api_hash="x", in_memory=True)
        message = await types.Message._parse(client, wire(raw_message(1)), {7: RAW_USER}, {CHANNEL_ID: chat}, replies=0)
        return await filters.topic(1)(client, message)

    assert not asyncio.run(main())


def test_the_topic_filter_finds_service_messages_of_a_topic(telegram):
    async def scenario(client):
        message = await from_update(client, raw.types.MessageService(
            id=2, peer_id=raw.types.PeerChannel(channel_id=CHANNEL_ID), date=1, reply_to=IN_TOPIC,
            action=raw.types.MessageActionTopicEdit(title="new")
        ))
        return await filters.topic(7)(client, message), await filters.topic(1)(client, message)

    in_the_topic, in_general = run(telegram, scenario)

    assert in_the_topic and not in_general


def test_handlers_of_a_topic_get_its_messages_during_a_flood_wait(telegram):
    """Whoever can post and delete a message, or cause a flood wait, must not move messages to another topic."""
    telegram.error = errors.FloodWait(value=3600)
    seen = {"topic": [], "general": []}

    async def scenario(client):
        async def on_topic(c, m):
            seen["topic"].append(m.id)

        async def on_general(c, m):
            seen["general"].append(m.id)

        client.add_handler(MessageHandler(on_general, filters.topic(1)), group=1)
        client.add_handler(MessageHandler(on_topic, filters.topic(7)), group=2)
        await handled_after(client, *[raw_message(i, IN_TOPIC) for i in range(1, 6)], raw_message(100))

    run(telegram, scenario)

    assert sorted(seen["topic"]) == [1, 2, 3, 4, 5] and seen["general"] == [100]


def test_a_topic_missing_from_an_answer_is_read_again_soon(telegram, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(utils, "time", SimpleNamespace(monotonic=lambda: now[0]))
    answers = [read_answer_without_topics, None]

    async def send(query, *args, **kwargs):
        telegram.reads += 1
        answer = answers.pop(0)
        return answer(query) if answer else read_answer(query)

    telegram.send = send

    async def scenario(client):
        first = await from_update(client, raw_message(1, IN_TOPIC))
        now[0] += utils.TOPIC_RETRY_SECONDS + 1
        return first, await from_update(client, raw_message(3, IN_TOPIC))

    first, second = run(telegram, scenario)

    assert first.topic is None and second.topic.id == 7 and telegram.reads == 2


def read_answer_without_topics(query):
    return read_answer(query, topics=())


@pytest.mark.parametrize("error", [errors.BadMsgNotification(48), errors.SecurityCheckMismatch("x")], ids=lambda e: type(e).__name__)
def test_errors_of_the_session_while_reading_do_not_lose_the_update(telegram, error):
    telegram.error = error

    async def scenario(client):
        return await handled_after(client, raw_message(1, REPLY), raw_message(2, IN_TOPIC))

    assert sorted(m.id for m in run(telegram, scenario)) == [1, 2]
