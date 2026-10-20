#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
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

"""Message.ask and stopping the pyromod listeners, offline."""

import asyncio

import pytest

import pyrogram
from pyrogram import enums, types
from pyrogram.errors import ListenerStopped
from pyrogram.utils import PyromodConfig


def new_client():
    """A client that is never started. Created inside the running event loop, it uses that loop."""
    return pyrogram.Client("pyromod", api_id=1, api_hash="x", in_memory=True)


def send(chat_type, method="ask", **kwargs):
    """Calls a bound method of the message 1 of the chat 7, of this type, with the text "Name?": returns the
    arguments of the message it sent and its result."""

    async def main():
        client = new_client()
        chat = types.Chat(id=7, type=chat_type)
        sent = []

        async def send_message(**arguments):
            sent.append(arguments)
            return types.Message(id=2, chat=chat)

        async def wait_for_message(chat_id, filters=None, timeout=None):  # The answer arrives at once
            return types.Message(id=3, chat=chat)

        client.send_message = send_message
        client.wait_for_message = wait_for_message
        message = types.Message(client=client, id=1, chat=chat)
        result = await getattr(message, method)("Name?", **kwargs)
        return sent[0], result

    return asyncio.run(main())


@pytest.mark.parametrize("chat_type, reply_to_message_id", [
    (enums.ChatType.PRIVATE, None),
    (enums.ChatType.GROUP, 1),
    (enums.ChatType.SUPERGROUP, 1),
])
def test_ask_quotes_by_default_in_groups_only(chat_type, reply_to_message_id):
    sent, answer = send(chat_type)
    # chat.type was compared with "private", which a ChatType never equals: the question always quoted the message
    assert sent["reply_to_message_id"] == reply_to_message_id
    assert answer.id == 3
    assert answer.request.id == 2


@pytest.mark.parametrize("chat_type", list(enums.ChatType))
def test_ask_quotes_by_default_as_reply_text_does(chat_type):
    question, _ = send(chat_type)
    reply, _ = send(chat_type, method="reply_text")
    assert question["reply_to_message_id"] == reply["reply_to_message_id"]


@pytest.mark.parametrize("chat_type", [enums.ChatType.PRIVATE, enums.ChatType.SUPERGROUP])
@pytest.mark.parametrize("kwargs, reply_to_message_id", [
    (dict(quote=True), 1),
    (dict(quote=False), None),
    (dict(reply_to_message_id=5), 5),
], ids=["quote", "no-quote", "reply_to_message_id"])
def test_ask_quotes_as_asked(chat_type, kwargs, reply_to_message_id):
    sent, _ = send(chat_type, **kwargs)
    assert sent["reply_to_message_id"] == reply_to_message_id


async def next_step(client, message):
    pass


def message_listeners(client):
    return client.listeners[enums.ListenerTypes.MESSAGE]


async def start_listening(client, chat_id):
    """Runs client.listen in a task: returns the task and the listener it registered."""
    task = asyncio.ensure_future(client.listen(chat_id=chat_id))
    await asyncio.sleep(0)  # listen registers its listener, then waits
    return task, message_listeners(client)[-1]


def stopped_handler(calls, asynchronous):
    """A PyromodConfig.stopped_handler that records its calls. stop_listener awaits a callable whose __call__ is a
    coroutine function, which a plain async function's is not, and runs any other one in an executor."""

    class Handler:
        def __call__(self, identifier, listener):
            calls.append((identifier, listener))

    class AsyncHandler:
        async def __call__(self, identifier, listener):
            calls.append((identifier, listener))

    return AsyncHandler() if asynchronous else Handler()


@pytest.mark.parametrize("with_stopped_handler", [False, True])
def test_stop_listener_removes_a_next_step_listener(monkeypatch, with_stopped_handler):
    calls = []
    handler = stopped_handler(calls, False) if with_stopped_handler else None
    monkeypatch.setattr(PyromodConfig, "stopped_handler", handler)
    monkeypatch.setattr(PyromodConfig, "throw_exceptions", True)

    async def main():
        client = new_client()
        client.register_next_step_handler(next_step, chat_id=7)
        # Used to raise AttributeError: 'NoneType' object has no attribute 'done', as such a listener has no future
        await client.stop_listener(message_listeners(client)[0])
        return message_listeners(client)

    assert asyncio.run(main()) == []
    assert calls == []  # Nobody waits for it: there is nobody to tell


def test_stop_listening_stops_next_step_and_waiting_listeners(monkeypatch):
    monkeypatch.setattr(PyromodConfig, "stopped_handler", None)
    monkeypatch.setattr(PyromodConfig, "throw_exceptions", True)

    async def main():
        client = new_client()
        client.register_next_step_handler(next_step, chat_id=7)
        task, _ = await start_listening(client, 7)
        client.register_next_step_handler(next_step, chat_id=8)
        # The next step listener of the chat 7 used to raise AttributeError before the listen() call was stopped
        await client.stop_listening(chat_id=7)

        with pytest.raises(ListenerStopped):
            await task

        return message_listeners(client)

    assert [listener.identifier.chat_id for listener in asyncio.run(main())] == [8]


def test_stop_listener_raises_listener_stopped_in_listen(monkeypatch):
    monkeypatch.setattr(PyromodConfig, "stopped_handler", None)
    monkeypatch.setattr(PyromodConfig, "throw_exceptions", True)

    async def main():
        client = new_client()
        task, listener = await start_listening(client, 7)
        await client.stop_listener(listener)
        assert message_listeners(client) == []

        with pytest.raises(ListenerStopped):
            await task

    asyncio.run(main())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_stop_listener_calls_the_stopped_handler(monkeypatch, asynchronous):
    calls = []
    monkeypatch.setattr(PyromodConfig, "stopped_handler", stopped_handler(calls, asynchronous))
    monkeypatch.setattr(PyromodConfig, "throw_exceptions", True)

    async def main():
        client = new_client()
        task, listener = await start_listening(client, 7)
        await client.stop_listener(listener)
        assert message_listeners(client) == []
        assert not listener.future.done()  # The handler is called instead of setting ListenerStopped
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        return listener

    listener = asyncio.run(main())
    assert calls == [(None, listener)]


def test_plain_async_functions_as_stopped_and_timeout_handlers(monkeypatch):
    calls = []

    async def stopped_handler(identifier, listener):
        calls.append(("stopped", listener.identifier.chat_id))

    async def timeout_handler(identifier, listener, timeout):
        calls.append(("timeout", timeout))

    # A plain async function's __call__ is not a coroutine function: these handlers were never awaited
    monkeypatch.setattr(PyromodConfig, "stopped_handler", stopped_handler)
    monkeypatch.setattr(PyromodConfig, "timeout_handler", timeout_handler)

    async def main():
        client = new_client()
        listening = asyncio.ensure_future(client.listen(chat_id=7))
        await asyncio.sleep(0)
        await client.stop_listening(chat_id=7)
        listening.cancel()
        await asyncio.gather(listening, return_exceptions=True)
        assert await client.listen(chat_id=8, timeout=0.01) is None

    asyncio.run(main())
    assert calls == [("stopped", 7), ("timeout", 0.01)]
