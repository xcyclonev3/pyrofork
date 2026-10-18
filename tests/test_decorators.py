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

"""The on_* decorators, used on a client (@app.on_message) and in plugins (@Client.on_message)."""

import asyncio
import sys

import pytest

import pyrogram
from pyrogram import filters, handlers
from pyrogram.handlers.handler import Handler

FILTER_DECORATORS = [
    ("on_bot_business_connect", handlers.BotBusinessConnectHandler),
    ("on_bot_business_message", handlers.BotBusinessMessageHandler),
    ("on_business_connected_bot", handlers.BusinessConnectedBotHandler),
    ("on_callback_query", handlers.CallbackQueryHandler),
    ("on_chat_join_request", handlers.ChatJoinRequestHandler),
    ("on_chat_join_result", handlers.ChatJoinResultHandler),
    ("on_chat_member_updated", handlers.ChatMemberUpdatedHandler),
    ("on_chosen_inline_result", handlers.ChosenInlineResultHandler),
    ("on_deleted_bot_business_messages", handlers.DeletedBotBusinessMessagesHandler),
    ("on_deleted_ephemeral_messages", handlers.DeletedEphemeralMessagesHandler),
    ("on_deleted_messages", handlers.DeletedMessagesHandler),
    ("on_edited_bot_business_message", handlers.EditedBotBusinessMessageHandler),
    ("on_edited_ephemeral_message", handlers.EditedEphemeralMessageHandler),
    ("on_edited_message", handlers.EditedMessageHandler),
    ("on_ephemeral_message", handlers.EphemeralMessageHandler),
    ("on_guest_query", handlers.GuestQueryHandler),
    ("on_inline_query", handlers.InlineQueryHandler),
    ("on_managed_bot_updated", handlers.ManagedBotUpdatedHandler),
    ("on_message", handlers.MessageHandler),
    ("on_message_draft_stopped", handlers.MessageDraftStoppedHandler),
    ("on_message_reaction_count_updated", handlers.MessageReactionCountUpdatedHandler),
    ("on_message_reaction_updated", handlers.MessageReactionUpdatedHandler),
    ("on_pending_message", handlers.PendingMessageHandler),
    ("on_poll", handlers.PollHandler),
    ("on_pre_checkout_query", handlers.PreCheckoutQueryHandler),
    ("on_purchased_paid_media", handlers.PurchasedPaidMediaHandler),
    ("on_raw_update", handlers.RawUpdateHandler),
    ("on_shipping_query", handlers.ShippingQueryHandler),
    ("on_story", handlers.StoryHandler),
    ("on_ton_connect_pending_disconnect", handlers.TonConnectPendingDisconnectHandler),
    ("on_ton_connect_session", handlers.TonConnectSessionHandler),
    ("on_ton_wallet_gasless_transfers_info", handlers.TonWalletGaslessTransfersInfoHandler),
    ("on_ton_wallet_state", handlers.TonWalletStateHandler),
    ("on_user_status", handlers.UserStatusHandler),
    ("on_user_subscription_updated", handlers.UserSubscriptionUpdatedHandler),
]

F = filters.private

# Arguments of a filter decorator, and the filters and group of the handler it registers
FILTER_FORMS = [
    pytest.param((), {}, None, 0, id="()"),
    pytest.param((F,), {}, F, 0, id="(f)"),
    pytest.param((F, 2), {}, F, 2, id="(f, 2)"),
    pytest.param((F,), {"group": 2}, F, 2, id="(f, group=2)"),
    pytest.param((None, 2), {}, None, 2, id="(None, 2)"),
    pytest.param((), {"group": 2}, None, 2, id="(group=2)"),
    pytest.param((), {"filters": F}, F, 0, id="(filters=f)"),
    pytest.param((), {"filters": F, "group": 2}, F, 2, id="(filters=f, group=2)"),
]

# Arguments of on_error, and the exceptions and group of the error handler it registers
ERROR_FORMS = [
    pytest.param((), {}, (Exception,), 0, id="()"),
    pytest.param((), {"group": 2}, (Exception,), 2, id="(group=2)"),
    pytest.param((ValueError,), {}, (ValueError,), 0, id="(E)"),
    pytest.param(((ValueError, KeyError),), {}, (ValueError, KeyError), 0, id="((E1, E2))"),
    pytest.param(([ValueError, KeyError],), {}, (ValueError, KeyError), 0, id="([E1, E2])"),
    pytest.param((ValueError,), {"group": 2}, (ValueError,), 2, id="(E, group=2)"),
    pytest.param((), {"errors": ValueError}, (ValueError,), 0, id="(errors=E)"),
    pytest.param(
        (), {"errors": (ValueError, KeyError), "group": 2}, (ValueError, KeyError), 2,
        id="(errors=(E1, E2), group=2)"
    ),
]


def plugin(decorator, *args, **kwargs):
    """Decorate as in a plugin, @Client.on_...(...): return the callback and the pairs load_plugins reads."""

    async def callback(*_):
        pass

    assert getattr(pyrogram.Client, decorator)(*args, **kwargs)(callback) is callback
    return callback, callback.handlers


def bound(decorator, *args, **kwargs):
    """Decorate on a client, @app.on_...(...): return the callback and the calls to app.add_handler."""
    calls = []

    async def callback(*_):
        pass

    async def main():
        app = pyrogram.Client("decorators", api_id=1, api_hash="x", in_memory=True)
        app.add_handler = lambda handler, group=0: calls.append((handler, group))
        assert getattr(app, decorator)(*args, **kwargs)(callback) is callback

    asyncio.run(main())
    assert not hasattr(callback, "handlers")
    return callback, calls


USES = [pytest.param(plugin, id="plugin"), pytest.param(bound, id="bound")]


def user_callback(handler):
    # Handlers with listeners (message, callback query, ...) wrap the callback
    return getattr(handler, "original_callback", handler.callback)


def test_every_decorator_is_covered():
    decorators = {name for name in dir(pyrogram.Client) if name.startswith("on_")}

    assert decorators == {name for name, _ in FILTER_DECORATORS} | {"on_error", "on_disconnect"}


@pytest.mark.parametrize("args, kwargs, expected_filters, expected_group", FILTER_FORMS)
@pytest.mark.parametrize("decorator, handler_type", FILTER_DECORATORS)
@pytest.mark.parametrize("use", USES)
def test_filter_decorators(use, decorator, handler_type, args, kwargs, expected_filters, expected_group):
    callback, registered = use(decorator, *args, **kwargs)

    [(handler, group)] = registered
    # Client.load_plugins registers only such pairs
    assert isinstance(handler, Handler) and isinstance(group, int)
    assert type(handler) is handler_type and user_callback(handler) is callback
    assert handler.filters is expected_filters and group == expected_group


@pytest.mark.parametrize("args, kwargs, exceptions, expected_group", ERROR_FORMS)
@pytest.mark.parametrize("use", USES)
def test_on_error(use, args, kwargs, exceptions, expected_group):
    callback, registered = use("on_error", *args, **kwargs)

    [(handler, group)] = registered
    assert isinstance(handler, Handler) and isinstance(group, int)
    assert type(handler) is handlers.ErrorHandler and user_callback(handler) is callback
    assert handler.exceptions == exceptions and group == expected_group


def test_on_error_takes_the_group_only_by_keyword_in_a_plugin():
    callback, registered = plugin("on_error", None, 2)

    [(handler, group)] = registered
    assert type(handler) is handlers.ErrorHandler and user_callback(handler) is callback
    assert handler.exceptions == (Exception,) and group == 0


@pytest.mark.parametrize("use", USES)
def test_on_disconnect(use):
    callback, registered = use("on_disconnect")

    [(handler, group)] = registered
    assert isinstance(handler, Handler) and isinstance(group, int)
    assert type(handler) is handlers.DisconnectHandler and user_callback(handler) is callback and group == 0


PLUGIN = """\
from pyrogram import Client, filters


@Client.on_message(filters=filters.private)
async def keyword_filters(client, message):
    pass


@Client.on_message(filters=filters.private, group=1)
async def keyword_filters_and_group(client, message):
    pass


@Client.on_message(filters.private, 2)
async def positional_filters_and_group(client, message):
    pass


@Client.on_error(ValueError)
async def value_error(client, update, error):
    pass


@Client.on_error(errors=(KeyError, IndexError))
async def lookup_error(client, update, error):
    pass
"""


def test_load_plugins(tmp_path, monkeypatch):
    root = "decorators_plugins"
    (tmp_path / root).mkdir()
    (tmp_path / root / "handlers.py").write_text(PLUGIN)
    monkeypatch.chdir(tmp_path)  # load_plugins looks for the root from the working directory
    monkeypatch.syspath_prepend(str(tmp_path))

    async def main():
        client = pyrogram.Client("plugins", api_id=1, api_hash="x", in_memory=True, plugins=dict(root=root))
        tasks = asyncio.all_tasks()
        client.load_plugins()
        # The dispatcher adds the handlers in tasks
        await asyncio.gather(*(asyncio.all_tasks() - tasks))
        return client.dispatcher

    try:
        dispatcher = asyncio.run(main())
    finally:
        for name in [name for name in sys.modules if name == root or name.startswith(root + ".")]:
            del sys.modules[name]

    assert {
        (user_callback(handler).__name__, handler.filters, group)
        for group, group_handlers in dispatcher.groups.items()
        for handler in group_handlers
        if type(handler) is handlers.MessageHandler
    } == {
        ("keyword_filters", filters.private, 0),
        ("keyword_filters_and_group", filters.private, 1),
        ("positional_filters_and_group", filters.private, 2),
    }
    assert {user_callback(handler).__name__: handler.exceptions for handler in dispatcher.error_handlers} == {
        "value_error": (ValueError,),
        "lookup_error": (KeyError, IndexError),
    }
