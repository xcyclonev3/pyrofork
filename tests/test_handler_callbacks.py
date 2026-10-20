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

"""How MessageHandler, CallbackQueryHandler and BotBusinessMessageHandler run their callbacks."""

import asyncio
import logging
import threading

import pytest

import pyrogram
from pyrogram import enums, handlers, types

HANDLERS = [handlers.MessageHandler, handlers.CallbackQueryHandler, handlers.BotBusinessMessageHandler]


@pytest.fixture(autouse=True)
def capture_every_pyrogram_log(caplog):
    caplog.set_level(logging.DEBUG, logger="pyrogram")


def new_update(handler_class, client):
    message = types.Message(client=client, id=1, chat=types.Chat(id=1, type=enums.ChatType.PRIVATE))

    if handler_class is handlers.CallbackQueryHandler:
        return types.CallbackQuery(
            client=client, id="1", from_user=types.User(id=2), chat_instance="1", message=message
        )

    return message


def dispatch(handler_class, *callbacks):
    """Dispatch a new update to handlers with *callbacks*, all in one group; returns it and the loop's thread."""
    async def main():
        # Created in the running loop, which the client binds to
        client = pyrogram.Client("handler-callbacks", api_id=1, api_hash="x", in_memory=True)
        update = new_update(handler_class, client)
        client.dispatcher.groups[0].extend(handler_class(callback) for callback in callbacks)
        await client.dispatcher._dispatch_to_handlers(None, {}, {}, update, handler_class)
        return update, threading.current_thread()

    return asyncio.run(main())


@pytest.mark.parametrize("handler_class", HANDLERS)
def test_plain_function_runs_in_the_loop(handler_class, caplog):
    calls = []

    def callback(client, update):
        calls.append((update, threading.current_thread()))

    update, loop_thread = dispatch(handler_class, callback)

    assert calls == [(update, loop_thread)]
    # It used to fail after running, with TypeError: object NoneType can't be used in 'await' expression, which the
    # dispatcher logged as an unhandled exception
    assert caplog.records == []


def coroutine_function(calls):
    async def callback(client, update):
        calls.append((update, threading.current_thread()))

    return callback


def plain_function_returning_a_coroutine(calls):
    callback = coroutine_function(calls)

    def wrapper(client, update):  # Like a decorator that isn't async
        return callback(client, update)

    return wrapper


def object_with_an_async_call(calls):
    class Callback:
        async def __call__(self, client, update):
            calls.append((update, threading.current_thread()))

    return Callback()


@pytest.mark.parametrize("handler_class", HANDLERS)
@pytest.mark.parametrize(
    "new_callback", [coroutine_function, plain_function_returning_a_coroutine, object_with_an_async_call]
)
def test_awaitable_returned_by_a_callback_is_awaited_in_the_loop(handler_class, new_callback, caplog):
    calls = []
    update, loop_thread = dispatch(handler_class, new_callback(calls))
    assert calls == [(update, loop_thread)]
    assert caplog.records == []


@pytest.mark.parametrize("handler_class", HANDLERS)
def test_bound_method_call_returned_by_a_plain_function_is_awaited_in_the_loop(handler_class, caplog, monkeypatch):
    requests = []

    async def request(client, **kwargs):  # Instead of sending it to Telegram
        requests.append((threading.current_thread(), asyncio.get_running_loop() is client.loop))

    monkeypatch.setattr(pyrogram.Client, "send_message", request)
    monkeypatch.setattr(pyrogram.Client, "answer_callback_query", request)

    # Like a decorator that isn't async. Called in the loop, Pyrogram's methods give a coroutine
    def wrapper(client, update):
        if isinstance(update, types.CallbackQuery):
            return update.answer("No")

        return update.reply("No")

    _, loop_thread = dispatch(handler_class, wrapper)

    assert requests == [(loop_thread, True)]
    assert caplog.records == []


@pytest.mark.parametrize("handler_class", HANDLERS)
def test_plain_function_still_stops_or_continues_the_propagation(handler_class, caplog):
    calls = []

    def stop(client, update):
        raise pyrogram.StopPropagation

    def go_on(client, update):
        raise pyrogram.ContinuePropagation

    async def next_callback(client, update):
        calls.append(update)

    with pytest.raises(pyrogram.StopPropagation):
        dispatch(handler_class, stop, next_callback)

    update, _ = dispatch(handler_class, go_on, next_callback)

    assert calls == [update]  # The next handler of the group only got the update after ContinuePropagation
    assert caplog.records == []


@pytest.mark.parametrize("handler_class", HANDLERS)
def test_matching_listener_gets_the_update_instead_of_the_callback(handler_class):
    calls = []

    def callback(client, update):
        calls.append(update)

    async def main():
        client = pyrogram.Client("handler-callbacks", api_id=1, api_hash="x", in_memory=True)
        update = new_update(handler_class, client)
        listener_type = (
            enums.ListenerTypes.CALLBACK_QUERY
            if handler_class is handlers.CallbackQueryHandler
            else enums.ListenerTypes.MESSAGE
        )
        listener = types.Listener(
            listener_type=listener_type,
            filters=None,
            unallowed_click_alert=True,
            identifier=types.Identifier(chat_id=1),
            future=asyncio.get_running_loop().create_future(),
        )
        client.listeners[listener_type].append(listener)
        client.dispatcher.groups[0].append(handler_class(callback))

        with pytest.raises(pyrogram.StopPropagation):
            await client.dispatcher._dispatch_to_handlers(None, {}, {}, update, handler_class)

        return update, listener, client.listeners[listener_type]

    update, listener, listeners = asyncio.run(main())
    assert listener.future.result() is update
    assert listeners == []
    assert calls == []
