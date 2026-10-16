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

"""Dispatcher wiring of the Layer 221-230 updates and service messages."""

import asyncio

import pytest

import pyrogram
from pyrogram import enums, handlers, raw, types
from tests.test_layer_roundtrip import RAW_BOT, RAW_CHANNEL, RAW_USER, roundtrip

RAW_MANAGED_BOT = raw.types.User(id=98, bot=True, bot_info_version=1, first_name="Managed", access_hash=4)


def new_client():
    client = pyrogram.Client("updates", api_id=1, api_hash="x", in_memory=True)
    client.me = None
    return client


def dispatch(update, users=(), chats=()):
    """Run the dispatcher parser of *update*: returns (parsed update, handler class)."""
    client = new_client()
    update = roundtrip(update)
    parser = client.dispatcher.update_parsers[type(update)]
    users = {u.id: u for u in (roundtrip(u) for u in users)}
    chats = {c.id: c for c in (roundtrip(c) for c in chats)}
    return asyncio.run(parser(update, users, chats))


def test_guest_query_update():
    message = raw.types.Message(id=5, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7),
                                date=1, message="@bot hello")
    parsed, handler = dispatch(raw.types.UpdateBotGuestChatQuery(query_id=42, message=message, qts=1), [RAW_USER])

    assert handler is handlers.GuestQueryHandler
    assert isinstance(parsed, types.GuestQuery) and parsed.id == "42" and parsed.message.text == "@bot hello"


def test_managed_bot_update():
    parsed, handler = dispatch(raw.types.UpdateManagedBot(user_id=7, bot_id=98, qts=1), [RAW_USER, RAW_MANAGED_BOT])

    assert handler is handlers.ManagedBotUpdatedHandler
    assert parsed.user.id == 7 and parsed.bot.id == 98


def test_new_bot_connection_update():
    parsed, handler = dispatch(raw.types.UpdateNewBotConnection(
        bot_id=99, confirmed=True, date=1700000000, device="Desktop", location="Earth"), [RAW_BOT])

    assert handler is handlers.BusinessConnectedBotHandler
    assert parsed.bot.id == 99 and parsed.is_confirmed is True and parsed.device_model == "Desktop"


@pytest.mark.parametrize("action, handler, parsed_type", [
    (raw.types.SendMessageTextDraftAction(random_id=11, text=raw.types.TextWithEntities(text="Hel", entities=[]),
                                          can_stop=True),
     handlers.PendingMessageHandler, types.PendingMessage),
    (raw.types.SendMessageStopDraftAction(random_id=11), handlers.MessageDraftStoppedHandler, types.MessageDraftStopped),
    (raw.types.SendMessageStopDraftAction(random_id=0), type(None), type(None)),
    (raw.types.SendMessageTypingAction(), type(None), type(None)),
])
def test_user_typing_updates(action, handler, parsed_type):
    parsed, handler_type = dispatch(raw.types.UpdateUserTyping(user_id=99, action=action), [RAW_BOT])

    assert handler_type is handler and isinstance(parsed, parsed_type)
    if parsed is not None:
        assert parsed.chat.id == 99 and parsed.draft_id == 11


def test_typing_updates_still_reach_raw_handlers():
    client = new_client()
    received = []

    async def raw_handler(_, update, users, chats):
        received.append(update)

    async def main():
        client.dispatcher.groups[0].append(handlers.RawUpdateHandler(raw_handler))
        update = roundtrip(raw.types.UpdateUserTyping(user_id=99, action=raw.types.SendMessageTypingAction()))
        await client.dispatcher._handle_packet((update, {}, {}), asyncio.Lock())

    asyncio.run(main())
    assert len(received) == 1


@pytest.mark.parametrize("decorator, handler", [
    ("on_guest_query", handlers.GuestQueryHandler),
    ("on_managed_bot_updated", handlers.ManagedBotUpdatedHandler),
    ("on_business_connected_bot", handlers.BusinessConnectedBotHandler),
    ("on_pending_message", handlers.PendingMessageHandler),
    ("on_message_draft_stopped", handlers.MessageDraftStoppedHandler),
])
def test_decorators(decorator, handler):
    @getattr(pyrogram.Client, decorator)(group=3)
    async def callback(_, update):
        pass

    [(registered, group)] = callback.handlers
    assert isinstance(registered, handler) and registered.callback is callback and group == 3


@pytest.mark.parametrize("query_id", [None, 77])
def test_chat_join_request_query_id(query_id):
    parsed, handler = dispatch(raw.types.UpdateBotChatInviteRequester(
        peer=raw.types.PeerChannel(channel_id=123), date=1, user_id=7, about="", invite=raw.types.ChatInvitePublicJoinRequests(),
        qts=1, query_id=query_id), [RAW_USER], [RAW_CHANNEL])

    assert handler is handlers.ChatJoinRequestHandler and parsed.query_id == query_id


@pytest.mark.parametrize("out, manager_bot_id", [(True, 99), (False, None)])
def test_managed_bot_created_message(out, manager_bot_id):
    client = new_client()
    message = roundtrip(raw.types.MessageService(
        id=1, peer_id=raw.types.PeerUser(user_id=99), from_id=raw.types.PeerUser(user_id=7), date=0, out=out,
        action=raw.types.MessageActionManagedBotCreated(bot_id=98)))
    users = {u.id: roundtrip(u) for u in (RAW_USER, RAW_BOT, RAW_MANAGED_BOT)}
    parsed = asyncio.run(types.Message._parse(client, message, users, {}, replies=0))

    assert parsed.service == enums.MessageServiceType.MANAGED_BOT_CREATED
    assert parsed.managed_bot_created.bot.id == 98
    # For messages received by the bot itself the manager bot is client.me (None here)
    assert getattr(parsed.managed_bot_created.manager_bot, "id", None) == manager_bot_id
