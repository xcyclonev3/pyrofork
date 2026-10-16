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


def parse_service(action, reply_to=None):
    client = new_client()
    message = roundtrip(raw.types.MessageService(
        id=3, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7), date=0, action=action,
        reply_to=reply_to))
    users = {u.id: roundtrip(u) for u in (RAW_USER, RAW_BOT)}
    return asyncio.run(types.Message._parse(client, message, users, {}, replies=0))


@pytest.mark.parametrize("new_creator_id, new_owner_id", [(99, 99), (0, None)])
def test_chat_owner_left_message(new_creator_id, new_owner_id):
    message = parse_service(raw.types.MessageActionNewCreatorPending(new_creator_id=new_creator_id))

    assert message.service == enums.MessageServiceType.CHAT_OWNER_LEFT
    assert getattr(message.chat_owner_left.new_owner, "id", None) == new_owner_id


def test_chat_owner_changed_message():
    message = parse_service(raw.types.MessageActionChangeCreator(new_creator_id=99))

    assert message.service == enums.MessageServiceType.CHAT_OWNER_CHANGED
    assert message.chat_owner_changed.new_owner.id == 99


@pytest.mark.parametrize("reply_to, request_message_id", [
    (raw.types.MessageReplyHeader(reply_to_msg_id=2), 2),
    (raw.types.MessageReplyHeader(reply_to_msg_id=2, reply_to_peer_id=raw.types.PeerUser(user_id=99)), None),
    (None, None),
])
def test_chat_has_protected_content_toggled_message(reply_to, request_message_id):
    message = parse_service(raw.types.MessageActionNoForwardsToggle(prev_value=True, new_value=False), reply_to)

    toggled = message.chat_has_protected_content_toggled
    assert message.service == enums.MessageServiceType.CHAT_HAS_PROTECTED_CONTENT_TOGGLED
    assert (toggled.request_message_id, toggled.old_has_protected_content, toggled.new_has_protected_content) == (
        request_message_id, True, False)


@pytest.mark.parametrize("expired", [True, False])
def test_chat_has_protected_content_disable_requested_message(expired):
    message = parse_service(raw.types.MessageActionNoForwardsRequest(prev_value=True, new_value=False, expired=expired))

    assert message.service == enums.MessageServiceType.CHAT_HAS_PROTECTED_CONTENT_DISABLE_REQUESTED
    assert message.chat_has_protected_content_disable_requested.is_expired is expired


def test_requests_to_enable_protected_content_are_unsupported():
    # Like TDLib, which takes them as unsupported messages
    message = parse_service(raw.types.MessageActionNoForwardsRequest(prev_value=False, new_value=True))

    assert message.service is None and message.chat_has_protected_content_disable_requested is None


def test_ton_wallet_transfer_message():
    message = parse_service(raw.types.MessageActionGramTransfer(
        amount=5000000000, peer_address="EQxyz", transaction_id="tx1", comment="thanks"))

    assert message.service == enums.MessageServiceType.TON_WALLET_TRANSFER
    transfer = message.ton_wallet_transfer
    assert (transfer.amount, transfer.peer_address, transfer.transaction_id, transfer.comment) == (
        5000000000, "EQxyz", "tx1", "thanks")


def test_ton_connect_request_message():
    message = parse_service(raw.types.MessageActionWalletTonConnectRequest(
        session_id=12, expires=1700000000, accepted=True, dapp_name="dApp", topic="sendTransaction"))

    assert message.service == enums.MessageServiceType.TON_CONNECT_REQUEST
    request = message.ton_connect_request
    assert (request.session_id, request.message_id, request.dapp_name, request.is_accepted) == (12, 3, "dApp", True)


@pytest.mark.parametrize("approve", [True, False])
def test_process_chat_protected_content_disable_request(approve):
    from tests.test_layer_roundtrip import PEERS, call, empty_updates, offline_client

    client = offline_client(PEERS, lambda q: empty_updates())
    assert call(lambda: client.process_chat_protected_content_disable_request(7, 2, approve=approve)) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.ToggleNoForwards)
    assert request.enabled is (not approve) and request.request_msg_id == 2


@pytest.mark.parametrize("update, handler, check", [
    (raw.types.UpdateWalletState(state=raw.types.WalletState(
        address="EQabc", public_key=b"k" * 32, balance=7, backup_enabled=True)),
     handlers.TonWalletStateHandler, lambda s: s.address == "EQabc" and s.balance == 7 and s.is_backup_enabled),
    (raw.types.UpdateWalletState(state=raw.types.WalletStateEmpty(creating=True)),
     handlers.TonWalletStateHandler, lambda s: s.is_being_created is True),
    (raw.types.UpdateWalletGaslessInfo(available=True, left=3, reset_at=1700000000, min_amount=10, relayer_address="EQr"),
     handlers.TonWalletGaslessTransfersInfoHandler, lambda i: i.is_available and i.left_count == 3),
    (raw.types.UpdateWalletTonConnectSession(session=raw.types.TonConnectSession(
        id=5, dapp_client_id="dapp", nonce=b"n", date=1700000000, pending=True)),
     handlers.TonConnectSessionHandler, lambda s: s.id == 5 and s.state == enums.TonConnectSessionState.PENDING),
    (raw.types.UpdateWalletTonConnectPendingDisconnect(session_ids=[5, 6]),
     handlers.TonConnectPendingDisconnectHandler, lambda d: d.session_ids == [5, 6]),
])
def test_wallet_updates(update, handler, check):
    parsed, handler_type = dispatch(update)

    assert handler_type is handler and check(parsed)


@pytest.mark.parametrize("decorator, handler", [
    ("on_ton_wallet_state", handlers.TonWalletStateHandler),
    ("on_ton_wallet_gasless_transfers_info", handlers.TonWalletGaslessTransfersInfoHandler),
    ("on_ton_connect_session", handlers.TonConnectSessionHandler),
    ("on_ton_connect_pending_disconnect", handlers.TonConnectPendingDisconnectHandler),
])
def test_wallet_decorators(decorator, handler):
    @getattr(pyrogram.Client, decorator)()
    async def callback(_, update):
        pass

    [(registered, group)] = callback.handlers
    assert isinstance(registered, handler) and group == 0
