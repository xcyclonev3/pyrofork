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

"""The dispatcher across stop() and start(), as in Client.restart(): handlers, conversations and worker locks."""

import asyncio
import logging

import pyrogram
from pyrogram import raw, types
from pyrogram.handlers import ErrorHandler, MessageHandler, RawUpdateHandler
from tests.test_layer_roundtrip import RAW_USER, roundtrip

WORKERS = 2


async def new_client():
    """A client that never connects, made in the running loop: its dispatcher uses the loop current at creation."""
    client = pyrogram.Client("lifecycle", api_id=1, api_hash="x", in_memory=True, workers=WORKERS)
    await client.storage.open()
    client.me = types.User(id=999, is_self=True)
    return client


async def start(client):
    """Start the dispatcher, then let the tasks of add_handler() and remove_handler() apply their changes."""
    await client.dispatcher.start()
    await asyncio.sleep(0.05)


async def handle_and_stop(client, *packets):
    """Queue (update, users, chats) as Client.handle_updates() does: stop() returns once the workers handled them."""
    for packet in packets:
        client.dispatcher.updates_queue.put_nowait(packet)
    await client.dispatcher.stop()


def new_message(message_id):
    """A message of user 7 in their private chat, as read from Telegram: parsed without the network (known user,
    no reply to fetch)."""
    message = raw.types.Message(id=message_id, peer_id=raw.types.PeerUser(user_id=7),
                                from_id=raw.types.PeerUser(user_id=7), date=1, message="hello")
    update = raw.types.UpdateNewMessage(message=message, pts=message_id, pts_count=1)
    return roundtrip(update), {RAW_USER.id: roundtrip(RAW_USER)}, {}


async def answer(waiting):
    """The message a wait_for_message() task got, or None when it timed out."""
    try:
        return await waiting
    except asyncio.TimeoutError:
        return None


def test_stop_drops_the_handlers_and_error_handlers_as_before():
    async def main():
        client = await new_client()
        handled = []

        async def on_message(_, message):
            handled.append(message.id)

        async def on_error(_, update, error):
            pass

        client.add_handler(MessageHandler(on_message))
        client.add_handler(MessageHandler(on_message), 1)
        client.add_handler(ErrorHandler(on_error))
        await start(client)
        await handle_and_stop(client, new_message(1))
        await start(client)
        await handle_and_stop(client, new_message(2))
        await client.storage.close()
        return client.dispatcher, handled

    dispatcher, handled = asyncio.run(main())
    # Unchanged, as in Pyrogram: handlers are added again after a restart (plugins are, by load_plugins())
    assert handled == [1, 1]
    assert dispatcher.error_handlers == []
    assert all(handler is dispatcher.conversation_handler for group in dispatcher.groups.values() for handler in group)


def test_the_conversation_handler_stays_first_in_group_0_with_its_waiters():
    async def main():
        client = await new_client()

        @pyrogram.Client.on_message()
        async def plugin(_, message):  # A smart plugin in group 0
            pass

        def load_plugins():  # What Client.initialize() does before every start
            for handler, group in plugin.handlers:
                client.add_handler(handler, group)

        load_plugins()
        await start(client)
        # Client.wait_for_message(), wait_for_callback_query() and Message.ask() rely on it
        before = asyncio.ensure_future(client.wait_for_message(7, timeout=1))
        await asyncio.sleep(0.05)  # The waiter is registered
        await client.dispatcher.stop()
        load_plugins()
        await start(client)
        group = list(client.dispatcher.groups[0])

        client.dispatcher.updates_queue.put_nowait(new_message(3))
        first = await answer(before)
        after = asyncio.ensure_future(client.wait_for_message(7, timeout=1))
        await asyncio.sleep(0.05)
        client.dispatcher.updates_queue.put_nowait(new_message(4))
        second = await answer(after)
        await client.dispatcher.stop()
        await client.storage.close()
        return client.dispatcher.conversation_handler, plugin.handlers[0][0], group, first, second

    conversation_handler, plugin_handler, group, first, second = asyncio.run(main())
    # As on a new client: the plugins loaded again at start come after it
    assert group == [conversation_handler, plugin_handler]
    # Waits started before and after the restart get the messages that come after it
    assert getattr(first, "id", None) == 3
    assert getattr(second, "id", None) == 4


def test_worker_locks_do_not_pile_up():
    async def main():
        client = await new_client()
        locks = []
        for _ in range(3):
            await client.dispatcher.start()
            await client.dispatcher.stop()
            locks.append(list(client.dispatcher.locks_list))
        await client.storage.close()
        return locks

    locks = asyncio.run(main())
    # One lock per worker, the same at every start: every start used to add as many as there are workers
    assert [len(start_locks) for start_locks in locks] == [WORKERS] * 3
    assert all(start_locks == locks[0] for start_locks in locks)


def test_handler_changes_waiting_for_busy_workers_at_a_restart_all_end():
    async def main():
        client = await new_client()
        release = asyncio.Event()

        async def busy(_, update, users, chats):
            await release.wait()

        async def on_raw_update(_, update, users, chats):
            pass

        client.add_handler(RawUpdateHandler(busy))
        await start(client)
        for _ in range(WORKERS):  # Each worker holds its lock while it handles an update
            client.dispatcher.updates_queue.put_nowait((raw.types.UpdateConfig(), {}, {}))
        await asyncio.sleep(0.05)
        for group in (1, 2, 3):  # Their tasks wait for the locks of the workers
            client.add_handler(RawUpdateHandler(on_raw_update), group)
        await asyncio.sleep(0.05)
        release.set()
        await client.dispatcher.stop()
        await start(client)  # Right away: nothing else runs between stop() and start()

        handler = RawUpdateHandler(on_raw_update)
        client.add_handler(handler, 4)  # Applied once its task holds every lock
        await asyncio.sleep(0.05)
        added = handler in client.dispatcher.groups.get(4, [])
        try:  # Each worker takes its lock
            await asyncio.wait_for(handle_and_stop(client, *[(raw.types.UpdateConfig(), {}, {})] * WORKERS), 1)
            stopped = True
        except asyncio.TimeoutError:
            stopped = False
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        await client.storage.close()
        return len(client.dispatcher.locks_list), added, stopped, pending

    locks, added, stopped, pending = asyncio.run(main())
    assert locks == WORKERS
    # With new locks at every start, the tasks still waiting for the old ones failed or never ended,
    # and could keep a lock of the new workers forever
    assert added and stopped and pending == []


def test_errors_of_raw_handlers_are_logged_for_updates_without_parser(caplog):
    update = raw.types.UpdateConfig()

    async def main():
        client = await new_client()
        handled = []

        async def failing(_, update, users, chats):
            raise ValueError("raw handler failed")

        async def same_group(_, update, users, chats):
            handled.append("same group")

        async def next_group(_, update, users, chats):
            handled.append("next group")

        client.add_handler(RawUpdateHandler(failing))
        client.add_handler(RawUpdateHandler(same_group))
        client.add_handler(RawUpdateHandler(next_group), 1)
        await start(client)
        await handle_and_stop(client, (update, {}, {}))
        await client.storage.close()
        return client.dispatcher.update_parsers, handled

    with caplog.at_level(logging.ERROR, logger="pyrogram.dispatcher"):
        update_parsers, handled = asyncio.run(main())

    assert type(update) not in update_parsers  # Not parsed: only raw handlers get it
    # The group of the failing handler ends there, as for parsed updates
    assert handled == ["next group"]
    # The error used to be dropped without any log
    records = [r for r in caplog.records if r.name == "pyrogram.dispatcher" and r.levelno == logging.ERROR]
    assert [r.getMessage() for r in records] == ["Unhandled exception: raw handler failed"]
    assert records[0].exc_info[0] is ValueError
