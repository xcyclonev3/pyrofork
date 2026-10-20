# Pyrogram guide for AI agents: usage and internal flows

This file explains how to use Pyrogram and how its main flows work inside, with the file and function behind every step, so that an AI (or a human) can trace a behaviour, review code that uses the library, or debug a log without reading the whole code base. It was written against Pyrogram `2.3.70+layer230` (MTProto layer 230). When this guide and the code disagree, the code is right: every section names the files to check.

## Contents

1. [How to use this guide](#1-how-to-use-this-guide)
2. [Quick facts](#2-quick-facts)
3. [Minimal programs](#3-minimal-programs)
4. [Client configuration](#4-client-configuration)
5. [Code map](#5-code-map)
6. [Flow: lifecycle (start, stop, restart, run, idle)](#6-flow-lifecycle-start-stop-restart-run-idle)
7. [Authorization, sessions and storage](#7-authorization-sessions-and-storage)
8. [Flow: an outgoing request](#8-flow-an-outgoing-request)
9. [Flow: an incoming update, from the socket to your handler](#9-flow-an-incoming-update-from-the-socket-to-your-handler)
10. [Handlers, groups, propagation, filters and listeners](#10-handlers-groups-propagation-filters-and-listeners)
11. [Peers, IDs and the peer cache](#11-peers-ids-and-the-peer-cache)
12. [Messages, formatting, keyboards and bots](#12-messages-formatting-keyboards-and-bots)
13. [Files: downloads, uploads, streaming](#13-files-downloads-uploads-streaming)
14. [Sessions, data centers and reconnects](#14-sessions-data-centers-and-reconnects)
15. [Errors](#15-errors)
16. [Concurrency and performance](#16-concurrency-and-performance)
17. [Durability and crash semantics](#17-durability-and-crash-semantics)
18. [Raw API and generated code](#18-raw-api-and-generated-code)
19. [Troubleshooting](#19-troubleshooting-log-line--cause--action)
20. [Known pitfalls in this version](#20-known-pitfalls-in-this-version)
- [Appendix A: Client methods by category](#appendix-a-client-methods-by-category)
- [Appendix B: decorators, filters, bound methods, enums, types, errors](#appendix-b-decorators-filters-bound-methods-enums-types-errors)

## 1. How to use this guide

- Terms: `app` is a `pyrogram.Client`; "users" are user accounts (userbots, logged in with a phone number), "bots" are bot accounts (bot token); "raw" means the generated MTProto API in `pyrogram.raw` (`functions`, `types`, `base`).
- Paths are relative to the repository root. `file.py` `Class.method` points to the code of a step.
- Steps are listed in execution order. "Task" means an `asyncio` task running concurrently.
- To answer "where does X happen?":

| Question | Look at |
|---|---|
| what a public method does | `pyrogram/methods/<category>/<method>.py` (appendix A) |
| how an update becomes a `Message` | `pyrogram/client.py` `handle_updates`, `pyrogram/dispatcher.py`, `pyrogram/types/messages_and_media/message.py` `Message._parse` |
| why a handler did not run | section 10, `pyrogram/dispatcher.py` `_dispatch_to_handlers`, the handler's `check` in `pyrogram/handlers/` |
| request retries, timeouts, flood waits | `pyrogram/session/session.py` `Session.invoke` / `send` |
| connection, reconnects | `pyrogram/session/session.py` `start/stop/restart/recv_worker/ping_worker`, `pyrogram/connection/` |
| encryption and security checks | `pyrogram/crypto/mtproto.py` |
| login prompts | `pyrogram/client.py` `Client.authorize`, `pyrogram/methods/auth/` |
| session storage | `pyrogram/storage/` |
| a raw TL type or function | `compiler/api/source/main_api.tl` (generated into `pyrogram/raw/`) |

## 2. Quick facts

| Item | Value |
|---|---|
| Import name | `pyrogram` |
| Install | from GitHub: `pip install -U "git+https://github.com/xcyclonev3/pyrofork.git"` (not from PyPI) |
| Version, layer | `2.3.70+layer230`, `pyrogram.raw.all.layer == 230` |
| Python | `>=3.10` (`requires-python = "~=3.10"`) |
| Dependencies | `pyaes`, `pysocks`, `pymediainfo-pyrofork` |
| Optional extras | `speedup` = `tgcrypto-pyrofork` + `uvloop`; `pymongo` for `MongoStorage`; `qrcode` for QR login |
| Concurrency model | `asyncio`; every Client method is a coroutine (sync wrappers exist, see below) |
| Origin | Pyrogram by Dan (delivrance); the copyright holders are named in `NOTICE` and in the file headers |
| License | LGPL-3.0-or-later |

Sync usage: `pyrogram/sync.py` wraps every public coroutine of `Client` and of the types. Inside a running event loop you must `await` them; from plain synchronous code with no running loop, calling them runs them to completion on the main loop (`app.start()`, `app.send_message(...)` without `await`). Mixing both styles in one program is a common source of bugs: prefer async code everywhere.

## 3. Minimal programs

Bot, blocking, simplest form:

```python
from pyrogram import Client, filters

app = Client("my_bot", api_id=12345, api_hash="0123456789abcdef0123456789abcdef", bot_token="123456:ABC-DEF")

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    await message.reply("Hello!")

app.run()  # start(), idle() until SIGINT/SIGTERM/SIGABRT, stop()
```

Async program (recommended for anything that does more than handle updates):

```python
import asyncio
from pyrogram import Client, idle

async def main():
    # Create the Client inside the running loop: it binds to the loop current at construction
    app = Client("my_account", api_id=12345, api_hash="0123456789abcdef0123456789abcdef")

    @app.on_message()
    async def log_all(client, message):
        print(message.chat.id, message.text)

    async with app:                      # start() ... stop(), even on errors
        await app.send_message("me", "started")
        await idle()                     # wait for a stop signal; handlers run meanwhile

asyncio.run(main())
```

Several clients: `await compose([app1, app2])` (`pyrogram/methods/utilities/compose.py`) starts them all, waits in `idle()`, then stops them.

Note: `app.run(coroutine)` only runs `coroutine` on the client's loop; it does not start or stop the client. Use `async with app:` (or `await app.start()` / `await app.stop()`) inside it.

## 4. Client configuration

`Client(name, **kwargs)` — `pyrogram/client.py`. Every argument except `name` is keyword-friendly and optional.

| Parameter | Type | Default | Meaning (from the docstring) |
|---|---|---|---|
| `name` | str | required | A name for the client, e.g.: "my_account". |
| `api_id` | int or str | `None` | The *api_id* part of the Telegram API key, as integer or string. |
| `api_hash` | str | `None` | The *api_hash* part of the Telegram API key, as string. |
| `app_version` | str | `Client.APP_VERSION` (`"Pyrogram <version>"`) | Application version. |
| `device_model` | str | `Client.DEVICE_MODEL` (from `platform`) | Device model. |
| `system_version` | str | `Client.SYSTEM_VERSION` (from `platform`) | Operating System version. |
| `system_lang_code` | str | `'en-US'` | System language code sent in `InitConnection`. |
| `lang_code` | str | `'en'` | Code of the language used on the client, in ISO 639-1 standard. |
| `lang_pack` | str | `''` | Language pack name sent in `InitConnection` (empty for third-party apps). |
| `ipv6` | bool | `False` | Pass True to connect to Telegram using IPv6. |
| `alt_port` | bool | `False` | Pass True to connect to Telegram using alternative port (5222). |
| `proxy` | dict | `None` | The Proxy settings as dict. |
| `test_mode` | bool | `False` | Enable or disable login to the test servers. |
| `bot_token` | str | `None` | Pass the Bot API token to create a bot session, e.g.: "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11" Only applicable for new sessions. |
| `session_string` | str | `None` | Pass a session string to load the session in-memory. |
| `use_qrcode` | bool | `False` | Pass True to login using a QR code. |
| `in_memory` | bool | `None` | Pass True to start an in-memory session that will be discarded as soon as the client stops. |
| `mongodb` | dict | `None` | Mongodb config as dict, e.g.: *dict(connection=async_pymongo.AsyncClient("mongodb://..."), remove_peers=False)*. |
| `storage` | storage.Storage | `None` | Custom session storage. |
| `phone_number` | str | `None` | Pass the phone number as string (with the Country Code prefix included) to avoid entering it manually. |
| `phone_code` | str | `None` | Pass the phone code as string (for test numbers only) to avoid entering it manually. |
| `password` | str | `None` | Pass the Two-Step Verification password as string (if required) to avoid entering it manually. |
| `workers` | int | `Client.WORKERS` = `min(32, cpu_count + 4)` | Number of maximum concurrent workers for handling incoming updates. |
| `workdir` | str | `Client.WORKDIR` (directory of the main script) | Define a custom working directory. |
| `plugins` | dict | `None` | Smart Plugins settings as dict, e.g.: *dict(root="plugins")*. |
| `parse_mode` | enums.ParseMode | `pyrogram.enums.ParseMode.DEFAULT` | Set the global parse mode of the client. |
| `no_updates` | bool | `None` | Pass True to disable incoming updates. |
| `skip_updates` | bool | `True` | Pass True to skip pending updates that arrived while the client was offline. |
| `takeout` | bool | `None` | Use a takeout session (`account.initTakeoutSession`); despite the docstring it does not set `no_updates`. |
| `sleep_threshold` | int | `10` | Set a sleep threshold for flood wait exceptions happening globally in this client instance, below which any request that raises a flood wait will be automatically invoked again after sleeping for the required amount of time. |
| `hide_password` | bool | `True` | Pass True to hide the password when typing it during the login. |
| `max_concurrent_transmissions` | int | `1` | Set the maximum amount of concurrent transmissions (uploads & downloads). |
| `client_platform` | enums.ClientPlatform | `pyrogram.enums.ClientPlatform.OTHER` | The platform where this client is running. |
| `max_message_cache_size` | int | `10000` | Set the maximum size of the message cache. |
| `max_business_user_connection_cache_size` | int | `10000` | Maximum size of the business connection cache. |

Notes:
- `api_id`/`api_hash` are only required to create a new authorization (empty session); bot tokens still need them.
- `workdir` default: the directory of the main script (`Client.PARENT_DIR = Path(sys.argv[0]).parent`, not resolved; `python bot.py` gives `.`). The session file is `<workdir>/<name>.session`.
- `workers` default: `Client.WORKERS = min(32, os.cpu_count() + 4)`.
- `device_model`, `system_version`, `app_version` default to values from `platform` and the Pyrogram version.
- `takeout=True` wraps requests in `InvokeWithTakeout` (user accounts only) but does not disable updates: pass `no_updates=True` as well. File transfers use media sessions and are not part of the takeout session.

Class attributes you may change before creating clients: `Client.DOWNLOAD_PARTS_IN_FLIGHT = 4`, `Client.UPLOAD_PARTS_IN_FLIGHT = 4`, `Client.UPDATES_WATCHDOG_INTERVAL = 900` (seconds without updates before `updates.GetState`), `Client.MAX_CONCURRENT_TRANSMISSIONS = 1`, `Client.MAX_CACHE_SIZE = 10000`; storage: `pyrogram.storage.sqlite_storage.SQLiteStorage.COMMIT_DELAY = 1`, `pyrogram.storage.FileStorage.VACUUM_FREE_RATIO = 0.25`; network: the `Session` and `TCP` constants in section 14.

Runtime attributes of a started client: `app.me` (`types.User`), `app.is_connected`, `app.is_initialized`, `app.session` (main `Session`), `app.media_sessions` / `app.dc_sessions` (`{dc_id: Session}`), `app.storage`, `app.dispatcher`, `app.executor` (`ThreadPoolExecutor(workers)`), `app.loop`, `app.message_cache`, `app.listeners`, `app.parse_mode` (change with `app.set_parse_mode(mode)`).

## 5. Code map

| Path | Role |
|---|---|
| `pyrogram/client.py` | `Client`: constructor, `authorize`, `load_session`, `load_plugins`, `fetch_peers`, `handle_updates`, `recover_gaps`, `handle_download`, `get_file`, `Cache` |
| `pyrogram/methods/<category>/` | one file per public method, mixed into `Client` through `pyrogram/methods/__init__.py` `Methods` |
| `pyrogram/methods/decorators/` | `on_*` decorators |
| `pyrogram/dispatcher.py` | update queue, parsers, handler groups, error handlers |
| `pyrogram/handlers/` | handler classes (`check` + callback) |
| `pyrogram/filters.py` | built-in filters and `filters.create` |
| `pyrogram/types/` | high-level objects (`Message`, `Chat`, `User`, keyboards…) and their bound methods |
| `pyrogram/enums/` | enumerations (`ChatType`, `ParseMode`, `MessageMediaType`…) |
| `pyrogram/parser/` | Markdown/HTML ↔ entities |
| `pyrogram/session/` | `Session` (MTProto session), `Auth` (auth key creation), message factory, msg ids |
| `pyrogram/connection/` | `Connection`, transports (`TCPAbridged` default, `TCPFull`, `TCPIntermediate`, obfuscated variants) |
| `pyrogram/crypto/` | AES (TgCrypto or pyaes), `mtproto.pack/unpack`, RSA, prime |
| `pyrogram/storage/` | `FileStorage`, `MemoryStorage`, `MongoStorage`, base `Storage` |
| `pyrogram/file_id.py` | `file_id` / `file_unique_id` encoding |
| `pyrogram/errors/` | `RPCError` and generated exception classes |
| `pyrogram/raw/` | generated MTProto API (`all.py`, `base/`, `functions/`, `types/`) plus `core/` (TL primitives, written by hand) |
| `pyrogram/sync.py` | sync wrappers |
| `compiler/api/`, `compiler/errors/`, `compiler/docs/` | generators for `raw/`, `errors/exceptions/`, docs |
| `tests/`, `benchmarks/` | tests (pytest) and offline benchmarks |

## 6. Flow: lifecycle (start, stop, restart, run, idle)

`await app.start()` (`pyrogram/methods/utilities/start.py`):
1. `connect()` (`pyrogram/methods/auth/connect.py`): `load_session()` opens the storage; for an empty session it needs `api_id`/`api_hash`, sets DC 2 and creates an auth key (`Auth.create`, `pyrogram/session/auth.py`: Diffie-Hellman exchange); then creates `app.session = Session(...)` and `await session.start()` (section 14); returns whether a user id is stored (authorized).
2. If not authorized: `Client.authorize()` (section 7).
3. With `takeout=True` on a user account: `account.InitTakeoutSession`.
4. `updates.GetState` (tells the server to start sending updates) and `get_me()` (`users.GetUsers` for the own account), sent together: one round trip for both.
5. If any step above fails: `disconnect()` and re-raise.
6. `app.me` is the account `get_me()` returned.
7. `initialize()` (`pyrogram/methods/auth/initialize.py`): `load_plugins()`, `dispatcher.start()` (creates `workers` dispatcher tasks; when `skip_updates=False`, runs `recover_gaps()`), starts `updates_watchdog`.

`await app.stop(block=True)` (`pyrogram/methods/utilities/stop.py`):
1. `terminate()` (`pyrogram/methods/auth/terminate.py`): finish takeout, `storage.save()`, `dispatcher.stop()` (stops the workers and **clears the handler groups and error handlers**, keeping only the internal `ConversationHandler`), stop and clear `media_sessions` and `dc_sessions`, stop the watchdog.
2. `disconnect()` (`pyrogram/methods/auth/disconnect.py`): `session.stop()` (awaits your `on_disconnect` callback), `storage.close()` (commits pending peers/states).
`block=False` schedules it as a task and returns at once: use it to stop a client from inside one of its own handlers (awaiting `stop()` there would wait for the handler itself).

`await app.restart(block=True)`: `stop()` then `start()`. Because `stop()` clears the handler groups (as upstream Pyrogram does), only plugin handlers come back; handlers added with `@app.on_…`/`add_handler` and error handlers must be added again after a restart. The internal `ConversationHandler` and pending `wait_for_message`/`ask` waiters are kept. With `in_memory=True` and no session string the authorization is lost too (fresh in-memory database).

`app.run(coroutine=None)` (`pyrogram/methods/utilities/run.py`): uses the current event loop's `run_until_complete`, so it cannot be called from a running loop. Without argument: `start()`, `idle()`, `stop()`. With a coroutine: runs only that coroutine.

`await idle()` (`pyrogram/methods/utilities/idle.py`): installs handlers for SIGINT, SIGTERM and SIGABRT (main thread only, replacing existing ones) and sleeps until one arrives. It does not stop clients by itself.

Context managers: `async with app:` = `await start()` … `await stop()` (a `ConnectionError` from `stop` is ignored); `with app:` is the sync equivalent for code without a running loop.

`await app.log_out()`: `auth.LogOut`, `stop()`, then deletes the storage (session file).

## 7. Authorization, sessions and storage

Login (`Client.authorize`, `pyrogram/client.py`), used by `start()` when the storage has no user id:
- **bot**: with `bot_token` → `sign_in_bot` (`auth.ImportBotAuthorization`, switches DC on `UserMigrate`). No prompts.
- **user**, interactive unless `phone_number`, `phone_code`, `password` are given:
  1. prompt `Enter phone number or bot token: ` (a value containing `:` is taken as a bot token; `qrcode` switches to QR login) and a confirmation;
  2. `send_code` (switches DC on `PhoneMigrate`/`NetworkMigrate`), prints where the code was sent;
  3. prompt `Enter confirmation code: ` → `sign_in`; an unregistered number raises `PhoneNumberUnoccupied` (no sign-up flow);
  4. on `SessionPasswordNeeded`: prints the hint, prompt `Enter password (empty to recover): ` (hidden when `hide_password=True`) → `check_password`; empty → recovery code by email.
- **QR**: `use_qrcode=True` (needs the `qrcode` package) prints a QR code for `tg://login?token=...` and waits for `UpdateLoginToken`, refreshing expired tokens.
- Non-interactive services: log in once (locally), then deploy the session file or a session string; never let a server process wait on `input()`.

Storage selection (`Client.__init__`), first match wins:
1. `storage=` a custom `pyrogram.storage.Storage` instance;
2. `session_string=` → `MemoryStorage(name, session_string)`;
3. `in_memory=True` → `MemoryStorage(name)` (discarded on stop);
4. `mongodb=dict(connection=<async Mongo client>, remove_peers=False)` → `MongoStorage` (falls back to `MemoryStorage` with a warning when `pymongo` is missing);
5. otherwise `FileStorage(name, workdir)` → SQLite file `<workdir>/<name>.session`.

`FileStorage`/`MemoryStorage` (`pyrogram/storage/sqlite_storage.py`): tables `sessions` (dc_id, api_id, test_mode, auth_key, date, user_id, is_bot), `peers` (id, access_hash, type, username, phone_number, last_update_on), `usernames`, `update_state` (pts per chat, 0 = common box), `version` (4). The file is opened with `check_same_thread=False`, `timeout=1`; `open()` compacts it (`VACUUM`, which rewrites the whole file in the event loop) only when more than `FileStorage.VACUUM_FREE_RATIO` (0.25) of its pages are free. Commit timing: section 17.

Session strings: `await app.export_session_string()` returns URL-safe base64 (no padding, 362 characters) of `struct.pack(">BI?256sQ?", dc_id, api_id, test_mode, auth_key, user_id, is_bot)`. Older 351/356-character strings (without `api_id`) are still accepted. A session string is a full credential: whoever has it controls the account.

Rules:
- one authorization = one running client: using the same session file or string from two processes at once causes `AUTH_KEY_DUPLICATED` and broken updates;
- the session file must stay writable; deleting it logs the account out locally (a new login is needed).

## 8. Flow: an outgoing request

Every high-level method ends in `Client.invoke`. Example: `await message.reply("hi")` → `Message.reply_text` → `Client.send_message` → `Client.invoke(raw.functions.messages.SendMessage(...))`.

1. `pyrogram/methods/advanced/invoke.py` `Client.invoke(query, retries=10, timeout=15, sleep_threshold=None)`
   - raises `ConnectionError("Client has not been started yet")` if `not app.is_connected`;
   - wraps the query in `InvokeWithoutUpdates` when `no_updates=True`, and in `InvokeWithTakeout` when a takeout session is active;
   - calls `self.session.invoke(query, retries, timeout, sleep_threshold)` with the call's `sleep_threshold`, or `app.sleep_threshold` when it is `None`;
   - afterwards stores every `users`/`chats` of the result (and of a nested `Updates`) in the peer cache with `fetch_peers` — this is how peers become resolvable later (section 11).
2. `pyrogram/session/session.py` `Session.invoke`
   - waits up to `WAIT_TIMEOUT` (15 s) for `is_started` (a restart in progress), then continues anyway;
   - loop: `return await self.send(query, timeout=timeout)`;
   - `FloodWait` / `FloodPremiumWait`: if `e.value > sleep_threshold >= 0` the error is raised to the caller, otherwise it logs `[<client name>] Waiting for N seconds before continuing (required by "<query>")`, sleeps and retries (no limit on how many times);
   - `OSError` (includes `TimeoutError` and connection errors), `InternalServerError` (5xx), `ServiceUnavailable`: retried up to `retries` times (default `MAX_RETRIES = 10`) after 0.5 s, by calling `invoke` again (so each retry waits for `is_started` again). It logs `[<n>] Retrying "<query>" due to: <error>`, where `<n>` is the retry number (1–10); only the last retry is logged at WARNING, the others at INFO. After the last retry the error is raised to the caller;
   - any other `RPCError` is raised immediately.
3. `Session.send(data, wait_response=True, timeout=15)`
   - `msg_factory.new(data)` assigns `msg_id` and `seq_no` and serializes the body once;
   - for requests with `RESULT_VECTOR` (bare vectors of primitives), remembers the element type to parse the answer;
   - encrypts with `mtproto.pack` (inline for bodies ≤ 64 KiB when nothing of this session is queued in `pyrogram.crypto_executor`, otherwise in that single-thread executor, so payloads keep `msg_id` order);
   - `await self.connection.send(payload)`; an `OSError` there removes the pending result and is raised;
   - waits up to `timeout` for the answer (`self.results[msg_id].event`); no answer → `TimeoutError("Request timed out")`;
   - an `RpcError` answer is turned into the matching exception class (`RPCError.raise_it`), e.g. `FloodWait`, `PeerIdInvalid`.
4. Transport: `pyrogram/connection/connection.py` `Connection.send` → `pyrogram/connection/transport/tcp/tcp_abridged.py` (default `TCPAbridged`) → `TCP.send` (`writer.write` + `drain` under a lock). Any exception there is re-raised as `OSError(e)` (with uvloop a closed transport reads `unable to perform operation on <TCPTransport closed=True ...>; the handler is closed`).
5. Answers come back through the receiving loop (section 9 step 1–3): `Session.handle_packet` matches `rpc_result.req_msg_id` to `self.results[...]`, sets the value and the event.

Consequences for callers:
- a request can take up to `timeout` (15 s) × attempts; with a dead connection and no restart, one call can block for minutes before raising;
- write operations are not idempotent: a retry after a timeout may execute twice on Telegram's side (e.g. two identical messages) if the first attempt arrived but its answer was lost;
- the check is `if e.value > sleep_threshold >= 0: raise`: `sleep_threshold=0` raises every `FloodWait`, a negative value sleeps through every `FloodWait` however long.

## 9. Flow: an incoming update, from the socket to your handler

1. **Receive** (`pyrogram/session/session.py`): `recv_worker` reads one packet at a time (`Connection.recv` → `TCPAbridged.recv` → `TCP.recv`) and starts a task `handle_packet(packet)` for each.
2. **Decrypt and check** (`handle_packet` → `pyrogram/crypto/mtproto.py` `unpack`): packets ≤ 64 KiB are unpacked inline, bigger ones in `pyrogram.crypto_executor` (order kept). Security checks as in section 14. Each inner message is acked later (`MsgsAck`).
3. **Route**: `RpcResult`, `Pong`, `BadServerSalt`, `BadMsgNotification`, `FutureSalts` resolve pending requests (section 8). Every other body (an `Updates` container) becomes a task `app.handle_updates(body)`. Containers can therefore be handled concurrently.
4. **`Client.handle_updates`** (`pyrogram/client.py`):
   - `Updates` / `UpdatesCombined`: `fetch_peers(users)` and `fetch_peers(chats)` (peer cache, section 11), builds `users = {id: raw User}` and `chats = {id: raw Chat/Channel}` (raw ids, not `-100…` ids). Then, for each update in the container:
     - if it has a `pts`, saves the state `(channel id or 0, pts, None, date, None)` with `storage.update_state`;
     - for an `UpdateNewChannelMessage` whose peers were `min`, calls `channels.GetChannelDifference` for that one message to get full peers;
     - enqueues `(update, users, chats)` on `app.dispatcher.updates_queue`.
   - `UpdateShortMessage` / `UpdateShortChatMessage`: saves the state, calls `updates.GetDifference` and enqueues the first new message (as `UpdateNewMessage`) or the first other update.
   - `UpdateShort`: enqueues `(update.update, {}, {})`.
   - `UpdatesTooLong`: only logged (no difference is fetched); a watchdog (`Client.updates_watchdog`) calls `updates.GetState` after 15 minutes without updates.
5. **Dispatcher workers** (`pyrogram/dispatcher.py`): `workers` tasks loop on `updates_queue.get()`. For each item: find the parser for the exact raw type (`Dispatcher.update_parsers`), build the high-level object (e.g. `types.Message._parse`), then go through the handler groups (section 10).
6. **Your callback** runs with `(client, parsed_update)`, or `(client, update, users, chats)` for `RawUpdateHandler`.

Raw update → parsed type → handler class (from `Dispatcher.__init__`; raw types without a parser reach only `RawUpdateHandler`):

| Raw update type(s) | Handler class (decorator) | Callback receives |
|---|---|---|
| `UpdateNewMessage`, `UpdateNewChannelMessage`, `UpdateNewScheduledMessage` | `MessageHandler` (`on_message`) | `Message` |
| `UpdateEditMessage`, `UpdateEditChannelMessage` | `EditedMessageHandler` (`on_edited_message`) | `Message` |
| `UpdateDeleteMessages`, `UpdateDeleteChannelMessages` | `DeletedMessagesHandler` (`on_deleted_messages`) | `list[Message]` (only ids/chat known) |
| `UpdateBotCallbackQuery`, `UpdateInlineBotCallbackQuery`, `UpdateBusinessBotCallbackQuery`, `UpdateEphemeralBotCallbackQuery` | `CallbackQueryHandler` (`on_callback_query`) | `CallbackQuery` |
| `UpdateBotInlineQuery` | `InlineQueryHandler` (`on_inline_query`) | `InlineQuery` |
| `UpdateBotInlineSend` | `ChosenInlineResultHandler` (`on_chosen_inline_result`) | `ChosenInlineResult` |
| `UpdateUserStatus` | `UserStatusHandler` (`on_user_status`) | `User` |
| `UpdateMessagePoll` | `PollHandler` (`on_poll`) | `Poll` |
| `UpdateChatParticipant`, `UpdateChannelParticipant`, `UpdateBotStopped` | `ChatMemberUpdatedHandler` (`on_chat_member_updated`) | `ChatMemberUpdated` |
| `UpdateBotChatInviteRequester` | `ChatJoinRequestHandler` (`on_chat_join_request`) | `ChatJoinRequest` |
| `UpdateJoinChatWebViewDecision` | `ChatJoinResultHandler` (`on_chat_join_result`) | join result |
| `UpdateStory` | `StoryHandler` (`on_story`) | `Story` |
| `UpdateBotShippingQuery` / `UpdateBotPrecheckoutQuery` | `ShippingQueryHandler` / `PreCheckoutQueryHandler` | `ShippingQuery` / `PreCheckoutQuery` |
| `UpdateBotMessageReaction` / `UpdateBotMessageReactions` | `MessageReactionUpdatedHandler` / `MessageReactionCountUpdatedHandler` | reaction updates |
| `UpdateBotNewBusinessMessage`, `UpdateBotEditBusinessMessage`, `UpdateBotDeleteBusinessMessage`, `UpdateBotBusinessConnect`, `UpdateNewBotConnection` | `BotBusinessMessageHandler`, `EditedBotBusinessMessageHandler`, `DeletedBotBusinessMessagesHandler`, `BotBusinessConnectHandler`, `BusinessConnectedBotHandler` | business objects |
| `UpdateBotPurchasedPaidMedia` | `PurchasedPaidMediaHandler` | `PurchasedPaidMedia` |
| `UpdateBotStarsSubscription` | `UserSubscriptionUpdatedHandler` | subscription update |
| `UpdateBotGuestChatQuery`, `UpdateManagedBot` | `GuestQueryHandler`, `ManagedBotUpdatedHandler` | |
| `UpdateUserTyping` (some actions) | `PendingMessageHandler`, `MessageDraftStoppedHandler` | |
| `UpdateNewEphemeralMessage`, `UpdateEditEphemeralMessage`, `UpdateDeleteEphemeralMessages` | `EphemeralMessageHandler`, `EditedEphemeralMessageHandler`, `DeletedEphemeralMessagesHandler` | |
| `UpdateWalletState`, `UpdateWalletGaslessInfo`, `UpdateWalletTonConnectSession`, `UpdateWalletTonConnectPendingDisconnect` | `TonWalletStateHandler`, `TonWalletGaslessTransfersInfoHandler`, `TonConnectSessionHandler`, `TonConnectPendingDisconnectHandler` | |
| anything else | `RawUpdateHandler` (`on_raw_update`) | `(update, users, chats)` |

`DisconnectHandler` (`on_disconnect`) is not dispatched: its async callback is awaited by `Session.stop()` of the main session. `ErrorHandler` (`on_error`) is described below.

## 10. Handlers, groups, propagation, filters and listeners

Registration:
- `@app.on_message(filters=None, group=0)` (and every `on_*` decorator) calls `app.add_handler(MessageHandler(func, filters), group)` and returns `func` unchanged.
- `@Client.on_message(...)` (on the class, inside plugin modules) only marks the function; `load_plugins` registers it at start (section 6). The filter can be passed positionally or as `filters=`, the group as the second positional argument or `group=`; `@Client.on_error(SomeError)` / `errors=` work too.
- `app.add_handler(handler, group=0)` returns `(handler, group)`; the change is applied by a background task once every worker is idle, so a handler added right before an update may miss it. `app.remove_handler(handler, group)` works the same way.

Dispatch (`Dispatcher._dispatch_to_handlers`), for each update:
1. groups run in ascending order (`group=-1` before `0` before `1`); handlers inside a group run in insertion order;
2. a handler matches when it is of the update's handler class (or a `RawUpdateHandler`) and `await handler.check(client, update)` is true (its filters pass). Exceptions in filters are logged and count as "no match";
3. **after the first matching handler of a group runs, the rest of that group is skipped** and dispatch goes on with the next group;
4. `raise pyrogram.StopPropagation` (or `update.stop_propagation()`) in a callback stops all later groups; `raise pyrogram.ContinuePropagation` (or `update.continue_propagation()`) lets the next handlers of the same group be checked;
5. any other exception in a callback goes to the error handlers (for raw update types without a parser, which only `RawUpdateHandler`s receive, it is logged as `Unhandled exception` instead), then the group ends as in step 3.

Error handlers: `@app.on_error(errors=None)` / `ErrorHandler(callback, errors)` with `errors` an exception class or a tuple (default `Exception`). The first one whose class matches is awaited as `callback(client, update, exception)` (must be `async def`). Without a matching error handler the dispatcher logs `Unhandled exception: <error>` with the traceback (logger `pyrogram.dispatcher`).

Callback style:
- **Use `async def` callbacks.** `MessageHandler`, `CallbackQueryHandler` and `BotBusinessMessageHandler` call the callback on the event-loop thread (`pyrogram/handlers/handler.py` `run_callback`) and await what it returns when that is awaitable: a plain `def` callback blocks the loop while it runs, and a Pyrogram call made inside it only runs if it is returned (`return message.reply(...)`). Other handler classes run plain `def` callbacks in `app.executor` (`Dispatcher._execute_handler`).
- Sync filters run in an executor; async filters are awaited.

Filters (`pyrogram/filters.py`): combine with `&`, `|`, `~`. Custom filters:

```python
from pyrogram import filters

def admin_only(ids):
    async def func(flt, client, update):            # sync functions work too
        return update.from_user and update.from_user.id in flt.ids
    return filters.create(func, "AdminOnly", ids=set(ids))   # extra kwargs become attributes of flt

@app.on_message(filters.command("ban", prefixes=["/", "!"]) & admin_only([123456]))
async def ban(client, message):
    target = message.command[1] if len(message.command) > 1 else None   # command parts are set by filters.command
```

`filters.command(commands, prefixes="/", case_sensitive=False)` sets `message.command` (list: command name then arguments); `filters.regex(pattern, flags=0)` sets `update.matches` (list of match objects). The full list of filters is in appendix B.

Listeners (pyromod-style, `pyrogram/methods/pyromod/`, types `Listener` and `Identifier` in `pyrogram/types/pyromod/`):
- `await app.listen(filters=None, listener_type=ListenerTypes.MESSAGE, timeout=None, unallowed_click_alert=True, chat_id=None, user_id=None, message_id=None, inline_message_id=None)` returns the next matching `Message` (or `CallbackQuery` with `ListenerTypes.CALLBACK_QUERY`);
- `await app.ask(chat_id, text, filters=None, listener_type=..., timeout=None, ...)` sends `text` then listens; the answer has `.sent_message`;
- `await app.stop_listening(listener_type=..., chat_id=None, user_id=None, message_id=None, inline_message_id=None)`, `app.register_next_step_handler(callback, filters=None, ...)`;
- bound helpers: `chat.listen/ask/stop_listening`, `user.listen/ask/stop_listening`, `message.wait_for_click(from_user_id=None, timeout=None, filters=None, alert=True)`;
- on timeout: `pyrogram.errors.ListenerTimeout` (when `pyrogram.utils.PyromodConfig.throw_exceptions` is true, the default), `ListenerStopped` when stopped; configure `PyromodConfig.timeout_handler`, `stopped_handler` (sync or async functions), `unallowed_click_alert`, `unallowed_click_alert_text`; `stop_listener` on a listener from `register_next_step_handler` just removes it;
- how they resolve: only `MessageHandler`, `CallbackQueryHandler` and `BotBusinessMessageHandler` look at listeners. When a listener matches the update (chat/user/message ids from the `Identifier`, `None` = any) and its filters pass, the handler's `check` succeeds even if the handler's own filters fail, the listener's future gets the update, **your callback is not called**, and propagation stops. So a `listen()` for messages needs at least one `MessageHandler` registered (any filters), and a callback-query listener needs a `CallbackQueryHandler`;
- `app.wait_for_message(chat_id, filters=None, timeout=None)`, `app.wait_for_callback_query(...)` and `message.ask(text, ...)` use a separate mechanism: the internal `ConversationHandler` that sits first in group 0 (one waiter per chat; a timeout raises `asyncio.TimeoutError`). `message.ask` quotes the message by default in groups, not in private chats.

## 11. Peers, IDs and the peer cache

ID formats (`pyrogram/utils.py`):

| Peer | High-level id | Raw id | Notes |
|---|---|---|---|
| user / bot | `id > 0` (≤ 999999999999) | `user_id` | needs `access_hash` |
| basic group | `-999999999999 ≤ id < 0` | `chat_id = -id` | no access hash |
| channel / supergroup / forum | `-100999999999999 ≤ id < -1000000000000` | `channel_id = -1000000000000 - id`, i.e. `-100<channel_id>` | needs `access_hash` |

`utils.get_peer_type(id)` returns `"user"`, `"chat"` or `"channel"`; `utils.get_channel_id(id)` converts between the two channel forms.

Every method that takes `chat_id`/`user_id` accepts an `int` id, a `"@username"`/`"username"`, a `t.me/username` link, a phone number (users only, for saved contacts) or `"me"`/`"self"`. They all go through `Client.resolve_peer` (`pyrogram/methods/advanced/resolve_peer.py`):

1. look the id up in the storage (`storage.get_peer_by_id`);
2. strings: `"me"`/`"self"` → `InputPeerSelf`; usernames → storage (`get_peer_by_username`, entries older than `USERNAME_TTL` = 8 h are ignored), else `contacts.ResolveUsername` and read the storage again; digits-only strings → phone number lookup in storage;
3. ints not in the storage: `users.GetUsers` / `messages.GetChats` / `channels.GetChannels` with `access_hash=0` (works for bots and for peers Telegram lets you see), then the storage again;
4. otherwise `PeerIdInvalid`.

The storage learns peers from every update (`Client.handle_updates` → `fetch_peers`) and every `invoke` result. Practical rules:
- a user account can only address a user/channel it has "met" (in a dialog, a common chat, a message, or by username); call `get_chat("@username")` or `get_users(...)` once, or iterate `get_dialogs()`, to fill the cache;
- `min` peers (partial info in big groups) are not stored;
- for a new session (in-memory or a new session file) the cache starts empty.

## 12. Messages, formatting, keyboards and bots

Formatting (`pyrogram/parser/`, enum `enums.ParseMode`): every method taking `text`/`caption` also takes `parse_mode` and `entities`.
- `ParseMode.DEFAULT` (the client default): Markdown **and** HTML in the same text;
- `ParseMode.MARKDOWN`: Markdown only (HTML special characters are escaped first);
- `ParseMode.HTML`: HTML only; `ParseMode.DISABLED`: plain text;
- explicit `entities=[types.MessageEntity(...)]` override `parse_mode`;
- the text is stripped of leading/trailing whitespace before parsing;
- Markdown: `**bold**`, `__italic__`, `--underline--`, `~~strike~~`, `||spoiler||`, `` `code` ``, ```` ```lang ```` blocks, `>` quote, `**>` expandable quote, `[text](url)`, `[name](tg://user?id=123)`, `![x](tg://emoji?id=N)` custom emoji;
- HTML: `<b>`/`<strong>`, `<i>`/`<em>`, `<u>`, `<s>`/`<del>`/`<strike>`, `<code>`, `<pre language="py">`, `<blockquote expandable>`, `<spoiler>`, `<a href="...">`, `<a href="tg://user?id=123">` (mention, needs a resolvable peer), `<emoji id="N">`, `<tg-time unix="..." format="...">`;
- reading back: `message.text.markdown` / `message.text.html` rebuild the formatted text from entities.

Sending and editing: `send_message`, `send_photo`/`send_video`/`send_document`/… (path, URL, `file_id` or file-like object), `send_media_group`, `edit_message_text`, `delete_messages`, `forward_messages`, `copy_message`; on a `Message`: `reply` (= `reply_text`), `reply_photo`…, `edit` (= `edit_text`), `delete`, `forward`, `copy`, `download`, `pin`, `react`, `click`. Replies: pass `reply_to_message_id` (optionally `reply_to_chat_id`, `quote_text`, and `message_thread_id` for forum topics); `message.reply*(..., quote=None)` replies to that message by default in groups and not in private chats.

Keyboards (`pyrogram/types/bots_and_keyboards/`), bots only (user accounts cannot send keyboards):
- `InlineKeyboardMarkup([[InlineKeyboardButton(text, callback_data=... | url=... | web_app=WebAppInfo(url) | switch_inline_query=... | copy_text=...)]])`: buttons under a message;
- `ReplyKeyboardMarkup([[KeyboardButton(text, request_contact=..., request_location=..., web_app=WebAppInfo(url), ...)]], resize_keyboard=True, one_time_keyboard=..., is_persistent=..., placeholder=...)`, `ReplyKeyboardRemove()`, `ForceReply()`;
- callback buttons: `@app.on_callback_query(filters.regex("^page:"))`, then `await callback_query.answer(text=None, show_alert=None, url=None, cache_time=0)` (always answer, or the client shows a spinner) and `callback_query.edit_message_text(...)` / `edit_message_reply_markup(...)`; `callback_data` is limited to 64 bytes.

Mini Apps (Web Apps):
- `KeyboardButton(text, web_app=WebAppInfo(url=...))` (private chats): `Telegram.WebApp.sendData(...)` arrives as a service `Message` with `message.web_app_data` (`.data`, `.button_text`); there is no built-in filter: `filters.create(lambda _, __, m: m.web_app_data is not None)`;
- `InlineKeyboardButton(text, web_app=WebAppInfo(url=...))` (private chats): the Mini App sends `query_id` to your backend, which answers with `await app.answer_web_app_query(query_id, types.InlineQueryResultArticle(title, types.InputTextMessageContent(text)))`;
- menu button: `await app.set_chat_menu_button(menu_button=types.MenuButtonWebApp(text, web_app=WebAppInfo(url=...)))`.

Inline mode: `@app.on_inline_query()` then `await inline_query.answer(results=[types.InlineQueryResultArticle(...)], cache_time=300)`; chosen results arrive through `on_chosen_inline_result` (enable inline feedback in BotFather).

Iterating: `get_chat_history(chat_id, limit=0, offset_id=0, ...)` (users only), `get_dialogs()`, `get_chat_members()`, `search_messages()` are async generators (`async for m in app.get_chat_history(chat_id, limit=100): ...`).

## 13. Files: downloads, uploads, streaming

Key limits (class attributes of `Client`, `pyrogram/client.py`):

| Name | Default | Meaning |
|---|---|---|
| `max_concurrent_transmissions` (constructor) | `1` | number of files transferred at once (one `asyncio.Semaphore` for uploads, one for downloads) |
| `Client.DOWNLOAD_PARTS_IN_FLIGHT` | `4` | parts of one file requested ahead while downloading |
| `Client.UPLOAD_PARTS_IN_FLIGHT` | `4` | parts of one file uploaded at once |
| part size | 1 MiB down, 512 KiB up | `upload.getFile` limit / `upload.saveFilePart` |

Download flow (`download_media` → `handle_download` → `get_file`, `pyrogram/client.py`):
1. `download_media(message_or_file_id, file_name="downloads/", in_memory=False, block=True, progress=None, progress_args=())` decodes the `file_id` (`pyrogram/file_id.py` `FileId.decode`) into a raw `InputFileLocation`;
2. `get_file` takes the download semaphore, gets a media session for the file's DC (`get_media_session`, `pyrogram/methods/messages/inline_session.py`: same DC → a `Session(is_media=True)` with the main auth key; other DC → a new auth key plus `auth.exportAuthorization`/`importAuthorization`), cached in `app.media_sessions` until `stop()`;
3. it keeps up to `DOWNLOAD_PARTS_IN_FLIGHT` `upload.GetFile` requests running ahead (on the DC stored in the `file_id`) and yields 1 MiB chunks in order; CDN redirects (`upload.FileCdnRedirect`) use a sequential path. Errors: `StopTransmission` and `FloodWait` are raised; an exception raised by your `progress` callback is logged once and the download goes on; any other error (e.g. `FileReferenceExpired`, network errors after the session's retries) is logged and ends the download: `download_media` deletes the temp file and returns `None`, while `stream_media` simply ends early without raising;
4. `handle_download` writes chunks to `<file>.temp` (or a `BytesIO` when `in_memory=True`) and renames it once every chunk arrived; on a failure, or `StopTransmission` (raise it inside `progress`, or call `client.stop_transmission()`), it deletes the temp file and returns `None`; `FloodWait` and `CancelledError` delete it and are raised. `progress(current, total, *progress_args)` is called per chunk (sync callbacks run in `app.executor`);
5. `block=False` returns immediately (download runs as a task).

`stream_media(message, limit=0, offset=0)` is an async generator over the same `get_file` (1 MiB chunks), for partial or on-the-fly downloads.

Upload flow (`send_document`/`send_photo`/… → `save_file`, `pyrogram/methods/advanced/save_file.py`):
1. files ≤ 10 MiB use `upload.saveFilePart` (with MD5 computed in order), bigger ones `upload.saveBigFilePart`;
2. `UPLOAD_PARTS_IN_FLIGHT` workers send 512 KiB parts concurrently on the media session of the current DC;
3. `progress` and `StopTransmission` behave as in downloads;
4. the resulting `InputFile`/`InputFileBig` is then used in `messages.sendMedia` (or `messages.uploadMedia`).

`file_id` / `file_unique_id` (`pyrogram/file_id.py`): base64url of a packed `FileId` (dc, media id, access hash, file reference, thumbnail source…). A `file_id` is bound to the account that received it (access hash and file reference); `file_unique_id` is stable across accounts but cannot be used to download.

## 14. Sessions, data centers and reconnects

Objects (all in `pyrogram/session/` and `pyrogram/connection/`):
- `app.session`: main `Session` to the account's DC (from storage `dc_id`), created in `connect()`;
- `app.media_sessions[dc_id]`: media sessions for transfers (see section 13), plus sessions to other DCs used by inline message edits (`get_session`);
- `app.dc_sessions[dc_id]`: regular sessions to other DCs (TON wallet requests);
- `Session` owns one `Connection` (`Connection.connect` tries 3 times, 1 s apart, then `ConnectionError`), one receiving task (`recv_worker`), one ping task (`ping_worker`) and the pending results.

Timing constants (`Session`, `TCP`):

| Constant | Value | Role |
|---|---|---|
| `TCP.TIMEOUT` | 10 s | connect timeout, and max silence on a read before the connection counts as lost |
| `Session.START_TIMEOUT` | 2 s | answer timeout for `Ping`/`InitConnection` while starting |
| `Session.WAIT_TIMEOUT` | 15 s | default request timeout; max wait for `is_started` in `invoke` |
| `Session.PING_INTERVAL` | 5 s | `PingDelayDisconnect(disconnect_delay=WAIT_TIMEOUT + 10)` cadence |
| `Session.MAX_RETRIES` | 10 | retries of a request on network/5xx errors |
| `Session.SLEEP_THRESHOLD` | 10 s | default flood-wait auto-sleep limit (the Client passes its own `sleep_threshold`) |
| `Session.ACKS_THRESHOLD` | 10 | pending acks before a `MsgsAck` is sent |

`Session.start()` loop: create a `Connection` → `connect()` → start `recv_worker` → `InvokeWithLayer(InitConnection(GetConfig))` (2 s; as the first request of the session it gets `BadServerSalt` and is sent again with the server salt, the salt starting at 0; CDN sessions send a `Ping` instead) → start `ping_worker` → `is_started.set()`. Any `OSError`/`RPCError` on the way → `stop()` and try again from the top (no limit); `AuthKeyDuplicated` and other exceptions → `stop()` and raise.

`Session.stop()`: clear `is_started`, stop the ping task, close the connection, wait for `recv_worker` to end, call `disconnect_handler` (main session only).

Restart triggers (each schedules `Session.restart()` = `stop()` + `start()`):
1. `recv_worker` reads `None` (EOF, error, or 10 s without data) or a 4-byte transport error while `is_started` is set; any other exception while reading is logged as `Unable to receive: ...` and handled the same way;
2. `ping_worker` fails to send (`OSError`);
3. `handle_packet` cannot unpack a packet (`ValueError`, e.g. unknown constructor).
A restart asked for while another one runs is ignored (`Session.is_restarting`). A 4-byte error `-404` raises `Unauthorized` ("Auth key not found…") from `recv_worker`; the session stays stopped.

Security checks on every received packet (`pyrogram/crypto/mtproto.py` `unpack`): auth key id, session id, `msg_key` (SHA-256), padding length (12–1024), message length, and `msg_id` (odd, not stale/too far in the future, not seen before) — a mismatch raises `SecurityCheckMismatch` and the packet is dropped.

## 15. Errors

All Telegram errors derive from `pyrogram.errors.RPCError` (`pyrogram/errors/rpc_error.py`); classes are generated per error message (`pyrogram/errors/exceptions/`, from `compiler/errors/source/*.tsv`). Each has `.CODE`, `.ID` (e.g. `"FLOOD_WAIT_X"`), `.MESSAGE`, and `.value` for errors with a number (seconds for `FloodWait`, DC for `*Migrate`).

Base classes (`pyrogram.errors`), each with many generated subclasses:

- `SeeOther` (code 303, 4 subclasses)
- `BadRequest` (code 400, 560 subclasses)
- `Unauthorized` (code 401, 9 subclasses)
- `Forbidden` (code 403, 38 subclasses)
- `NotAcceptable` (code 406, 20 subclasses)
- `Flood` (code 420, 9 subclasses)
- `InternalServerError` (code 500, 49 subclasses)
- `ServiceUnavailable` (code 503, 3 subclasses)

Frequent subclasses (all exist in this version): `FloodWait`, `FloodPremiumWait`, `SlowmodeWait`, `PeerIdInvalid`, `ChannelInvalid`, `ChannelPrivate`, `ChatAdminRequired`, `ChatWriteForbidden`, `UserIsBlocked`, `UserDeactivated`, `UserBannedInChannel`, `MessageNotModified`, `MessageIdInvalid`, `MessageDeleteForbidden`, `UsernameNotOccupied`, `UsernameInvalid`, `AuthKeyUnregistered`, `AuthKeyDuplicated`, `SessionPasswordNeeded`, `PhoneCodeInvalid`, `PhoneCodeExpired`, `PhoneMigrate`, `NetworkMigrate`, `FileMigrate`, `FileReferenceExpired`, `QueryIdInvalid`, `BotMethodInvalid`, `UserNotParticipant`, `InviteHashExpired`, `SessionRevoked`, `MessageTooLong`, `MediaCaptionTooLong`, `PeerFlood`, `WebpageMediaEmpty`, `ButtonDataInvalid`, `ReplyMarkupInvalid`


Handling patterns:

```python
import asyncio
from pyrogram.errors import FloodWait, RPCError, PeerIdInvalid, UserIsBlocked, ChatWriteForbidden

async def safe_send(app, chat_id, text):
    try:
        return await app.send_message(chat_id, text)
    except FloodWait as e:            # only raised when e.value > sleep_threshold
        await asyncio.sleep(e.value)
        return await app.send_message(chat_id, text)
    except (UserIsBlocked, ChatWriteForbidden, PeerIdInvalid):
        return None                    # cannot write there: don't retry
    except RPCError as e:
        print(e.ID, e.CODE, e)         # any other Telegram error
```

Non-Telegram exceptions callers see: `ConnectionError` (client not started, already started/stopped), `OSError`/`TimeoutError` (network, after all retries), `asyncio.CancelledError` (task cancelled), `ValueError`/`TypeError` (bad arguments, e.g. invalid peer id format). Reconnection failures do not surface: `Session.start` retries connecting without limit.

## 16. Concurrency and performance

- Everything runs on one asyncio event loop (one CPU core for Python code, GIL). Receiving, decrypting small messages, parsing and handlers share it.
- `async def` handlers run as tasks on that loop: never block in them (no `time.sleep`, no blocking I/O, no heavy CPU); use `await asyncio.sleep`, async libraries, or `await app.loop.run_in_executor(...)` / a `ProcessPoolExecutor` for CPU work.
- plain `def` handlers (except `MessageHandler`, `CallbackQueryHandler` and `BotBusinessMessageHandler`, which call them on the loop thread, see section 10) and sync progress callbacks run in `app.executor` (a `ThreadPoolExecutor` with `workers` threads); they still hold the GIL for Python code.
- `workers` (default `min(32, cpu_count + 4)`) is also the number of dispatcher tasks consuming updates; they dispatch concurrently (each holds only its own lock, which `add_handler`/`remove_handler` take to change the handler list safely), so a slow handler only occupies its own worker and update order across workers is not guaranteed. Set `workers=1` for strictly sequential handling.
- AES (TgCrypto) and SHA-256 release the GIL for data ≥ 2 KiB; messages > 64 KiB (file parts) are packed/unpacked in `pyrogram.crypto_executor`, so file crypto overlaps with the loop.
- Install the speedups: `pip install -U tgcrypto-pyrofork uvloop` (the `speedup` extra of the package lists the same two). Without TgCrypto, crypto falls back to pure-Python `pyaes` and is much slower. uvloop is never enabled by Pyrogram itself: call `uvloop.install()` before creating the `Client` (or `uvloop.run(main())` on Python 3.12+).
- Measured on one core (Python 3.10, offline benchmark `benchmarks/bench_pipeline.py`): ~3,600 updates/s from packet to handler with an in-memory session, ~3,900/s with a session file; Python 3.13 ~5,100/s. Optional app-level gains: `uvloop` (~8%), `gc.collect(); gc.freeze()` after `start()` (~20% less time, the API module creates ~114k long-lived objects).
- Transfers: raise `max_concurrent_transmissions` to move several files at once; `DOWNLOAD_PARTS_IN_FLIGHT`/`UPLOAD_PARTS_IN_FLIGHT` are conservative (4) on purpose (flood limits for normal accounts).
- Proxies: the proxy handshake (`TCP._connect_via_proxy`, PySocks) runs synchronously and blocks the event loop during every (re)connect.

## 17. Durability and crash semantics

- Session values (`dc_id`, `auth_key`, `user_id`, `is_bot`, `date`, …) are committed to the storage immediately.
- Peers (`update_peers`, `update_usernames`) and update states (`update_state`, pts per channel) are committed together at most `SQLiteStorage.COMMIT_DELAY` (1 s) after they change, and on `save()` / `close()` (`Client.stop` calls both). A hard kill (SIGKILL, OOM, power loss) can lose the last second of them; `SQLiteStorage.COMMIT_DELAY = 0` commits on the next loop iteration instead.
- `skip_updates=True` (default): updates that arrived while the client was offline are not fetched. `skip_updates=False`: at start the dispatcher calls `Client.recover_gaps()` which runs `updates.getDifference`/`updates.getChannelDifference` from the stored states; after a hard kill, updates of the last second may be delivered again (handlers must tolerate duplicates).
- `idle()` handles SIGINT, SIGTERM and SIGABRT, so `systemctl stop`/`docker stop` end with a normal `stop()`.

## 18. Raw API and generated code

- Call any MTProto function: `await app.invoke(raw.functions.messages.GetFullChat(chat_id=123))`; input peers come from `await app.resolve_peer(chat_id)`; results are raw types (`raw.types.*`), whose `users`/`chats` are cached automatically.
- Raw updates: `@app.on_raw_update()` receives `(client, update, users, chats)` for every update, including those with a high-level handler (put it in its own group if both must run).
- Generated code: `pyrogram/raw/all.py`, `raw/base`, `raw/functions`, `raw/types` come from `compiler/api/source/main_api.tl` (`make api`, or `python compiler/api/compiler.py`); `pyrogram/errors/exceptions/` from `compiler/errors/source/*.tsv`. They are not committed: a source checkout must run the generator before import. Never edit generated files; change the `.tl`/`.tsv` sources and regenerate.
- TL serialization: `pyrogram/raw/core/` (`TLObject.read/write`, primitives `Int`, `Long`, `Bytes`, `String`, `Vector`…). `len(raw_obj)` serializes the object, and `bool(raw_obj)` falls back to it (always True for non-list objects).

## 19. Troubleshooting (log line → cause → action)

| Symptom | Cause | Action |
|---|---|---|
| `[<n>] Retrying "<method>" due to: ...` | network error / 5xx on attempt `<n>` of 10 (`Session.invoke`) | transient: nothing; persistent: check connectivity, proxy, DC |
| `... the handler is closed` / `TCPTransport closed=True` (uvloop) on every request | the session's connection is closed and no restart brought it back | update Pyrogram (restart fixes `854a0026`, `5e32baa4`); restart the process as a workaround |
| `Task exception was never retrieved ... Session.recv_worker ... 'NoneType' object has no attribute 'read'` or `read() called while another coroutine is already waiting` | two restarts of one session at once (fixed in `854a0026`) | update Pyrogram |
| `Waiting for N seconds before continuing (required by "...")` | `FloodWait` ≤ `sleep_threshold`, auto-sleep | slow down; raise/lower `sleep_threshold` |
| `FloodWait` raised to your code | wait > `sleep_threshold` | sleep `e.value` seconds; reduce request rate |
| `PeerIdInvalid` / `ValueError: Peer id invalid` | peer not in the cache, or wrong id format | meet the peer first (section 11); use `-100…` ids for channels |
| `AuthKeyUnregistered`, `SessionRevoked`, `UserDeactivated` | session logged out/terminated | log in again; a new session is required |
| `AuthKeyDuplicated` | the same session used from two places at once | never run two processes on one session file/string |
| `Unauthorized: Auth key not found in the system` (from `recv_worker`) | server forgot the auth key (`-404`) | delete the session and log in again |
| `Unable to receive: <Exception>` | unexpected error while reading; the session restarts | if frequent, report with the exception |
| `Server sent transport error: 429 (transport flood)` | too many connections | fewer clients/sessions per IP |
| `download_media` returns `None` | the download failed (the error is logged just before) or was stopped with `StopTransmission` | read the logged error; refresh the message for an expired file reference and retry |
| `Client has not been started yet` (`ConnectionError`) | method called before `start()` / after `stop()` | `await app.start()` first or use `async with app:` |
| handlers never run | `no_updates=True`; filters don't match; an earlier handler of the same group matched (only one per group runs) or a lower group raised `StopPropagation`; a listener consumed the update | check section 10 |
| tasks hang when using `asyncio.run(main())` | `Client` created outside the running loop (binds to another loop) | create the `Client` inside `main()` or use `app.run(main())` |
| event loop blocked (your own watchdog reports stalls) | blocking code in handlers, proxy reconnects, huge sync progress callbacks | move work off the loop (section 16) |

## 20. Known pitfalls in this version

Behaviours checked in the code of this version that surprise users. Treat them as facts when reviewing code; they may change later (check the code).

| # | Pitfall | Where | What to do |
|---|---|---|---|
| 1 | A plain `def` callback for `MessageHandler`, `CallbackQueryHandler` or `BotBusinessMessageHandler` runs on the event-loop thread and blocks it while it runs (an awaitable it returns is awaited); plain `def` callbacks of the other handler classes run in `app.executor` | `pyrogram/handlers/handler.py` `run_callback`, `pyrogram/dispatcher.py` `_execute_handler` | use `async def` callbacks |
| 2 | `restart()` (or `stop()` + `start()`) drops every handler added with `@app.on_…`/`add_handler` and every error handler (as upstream Pyrogram does); only plugins are registered again. The internal `ConversationHandler` and pending `wait_for_message`/`ask` waiters are kept | `pyrogram/dispatcher.py` `Dispatcher.stop` | use plugins, or add your handlers again after a restart |
| 3 | `add_handler`/`remove_handler` apply asynchronously (a task waiting for all dispatcher workers); errors of `remove_handler` are raised inside that task, not to the caller | `pyrogram/dispatcher.py` | register handlers before `start()`; don't rely on immediate effect |
| 4 | `UpdatesTooLong` is only logged: the gap is not fetched until the watchdog's `updates.GetState` (15 min without updates) | `pyrogram/client.py` `handle_updates` | missed updates are only recovered at the next start with `skip_updates=False` |
| 5 | One `wait_for_message`/`wait_for_callback_query` waiter per chat: a second call for the same chat replaces the first | `pyrogram/handlers/conversation_handler.py` | use `app.listen`/`app.ask` for concurrent conversations |
| 6 | Without `include`, `load_plugins` imports modules in `os.walk` order (not sorted) and does not catch import errors: a broken plugin makes `start()` fail | `pyrogram/client.py` `load_plugins` | use explicit `group` numbers for ordering; keep plugins importable |
| 7 | `Client` binds to the event loop current at construction (`self.loop = asyncio.get_event_loop()`); creating it outside the loop that runs it leaves its tasks on another loop (hangs) | `pyrogram/client.py`, `pyrogram/dispatcher.py` | create the Client inside the coroutine passed to `asyncio.run`, or use `app.run()` |
| 8 | Connecting through a proxy blocks the event loop during the proxy handshake on every (re)connect | `pyrogram/connection/transport/tcp/tcp.py` `_connect_via_proxy` | expect short stalls on reconnects when using proxies |
| 9 | In plugin form, passing the filter twice (`@Client.on_message(f, filters=g)`) registers nothing | `pyrogram/methods/decorators/` | pass it once |

## Appendix A: Client methods by category

Generated from `pyrogram/methods/` (signature: required parameters, then optional ones; "users"/"bots" = who may call it, from the docstring). All are coroutines: `await app.<method>(...)`.

Total public Client methods: 407.

### advanced (3)

- `invoke(query; optional: retries, timeout, sleep_threshold)` — Invoke raw Telegram functions. *(users, bots)*
- `resolve_peer(peer_id)` — Get the InputPeer of a known peer id. *(users, bots)*
- `save_file(path; optional: file_id, file_part, progress, progress_args)` — Upload a file onto Telegram servers, without actually sending the message to anyone. *(users, bots)*

### auth (25)

- `accept_oauth_request(url; optional: match_code, allow_write_access, allow_phone_number_access)` — Accept an OAuth authorization request. *(users)*
- `check_oauth_request_match_code(url, match_code)` — Check the code chosen by the user for an OAuth authorization request. *(users)*
- `check_password(password)` — Check your Two-Step Verification password and log in. *(users)*
- `connect()` — Connect the client to Telegram servers.
- `decline_oauth_request(url)` — Decline an OAuth authorization request, notifying Telegram that the user refused to log in. *(users)*
- `disconnect()` — Disconnect the client from Telegram servers.
- `dismiss_web_token(token)` — Dismiss a received web authorization token without using it; for official Telegram apps only. *(users)*
- `finish_firebase_pnv_login(google_token)` — Finish a login with Firebase Phone Number Verification; for official Telegram apps only. *(users)*
- `firebase_pnv_sign_up(first_name; optional: last_name, disable_notification)` — Register a new Telegram account with the phone number verified with Firebase Phone Number Verification; for official Telegram apps only. *(users)*
- `get_active_sessions()` — Returns all active sessions of the current user. *(users)*
- `get_external_link(link; optional: allow_write_access, allow_phone_number_access)` — Get an HTTP URL which automatically authorizes the current user on a website after opening an HTTP link. *(users)*
- `get_external_link_info(link)` — Get information about an action to be done when the current user opens an HTTP link. *(users)*
- `get_oauth_link_info(url; optional: in_app_origin)` — Get information about an OAuth authorization request (login with Telegram on a website or in an app). *(users)*
- `get_password_hint()` — Get your Two-Step Verification password hint. *(users)*
- `init_firebase_pnv_login(optional: except_ids)` — Start a login with Firebase Phone Number Verification; for official Telegram apps only. *(users)*
- `initialize()` — Initialize the client by starting up workers.
- `log_out()` — Log out from Telegram and delete the *\*.session* file. *(users, bots)*
- `recover_password(recovery_code)` — Recover your password with a recovery code and log in. *(users)*
- `resend_code(phone_number, phone_code_hash)` — Re-send the confirmation code using a different type. *(users)*
- `send_code(phone_number)` — Send the confirmation code to the given phone number. *(users)*
- `send_recovery_code()` — Send a code to your email to recover your password. *(users)*
- `sign_in(phone_number, phone_code_hash, phone_code)` — Authorize a user in Telegram with a valid confirmation code. *(users)*
- `sign_in_bot(bot_token)` — Authorize a bot using its bot token generated by BotFather. *(bots)*
- `sign_in_qrcode()` — Authorize a user in Telegram with a QR code. *(users)*
- `terminate()` — Terminate the client by shutting down workers.

### bots (37)

- `add_bot_secondary_username(bot_id, username)` — Add a secondary username to a bot owned by the current user. *(users)*
- `answer_callback_query(callback_query_id; optional: text, show_alert, url, cache_time)` — Send answers to callback queries sent from inline keyboards. *(bots)*
- `answer_chat_join_request_query(query_id, result; optional: url)` — Answer a join request query received by a guard bot. *(bots)*
- `answer_guest_query(guest_query_id, result)` — Answer a guest query: post a message as a guest into the chat in which the bot was mentioned. *(bots)*
- `answer_inline_query(inline_query_id, results; optional: cache_time, is_gallery, is_personal, next_offset, switch_pm_text, switch_pm_parameter)` — Send answers to an inline query. *(bots)*
- `answer_web_app_query(web_app_query_id, result)` — Set the result of an interaction with a `Web App <https://core.telegram.org/bots/webapps>`_ and send a corresponding message on behalf of the user to the chat from which the query originated. *(bots)*
- `check_bot_username(username; optional: is_secondary)` — Check whether a username can be used for a new bot or as a secondary username of an existing bot. *(users)*
- `create_bot(manager_bot_id, name, username; optional: via_link)` — Create a bot managed by another bot. *(users)*
- `delete_bot_commands(optional: scope, language_code)` — Delete the list of the bot's commands for the given scope and user language. *(bots)*
- `delete_bot_secondary_username(bot_id)` — Remove the secondary username from a bot owned by the current user. *(users)*
- `get_bot_commands(optional: scope, language_code)` — Get the current list of the bot's commands for the given scope and user language. *(bots)*
- `get_bot_default_privileges(optional: for_channels)` — Get the current default privileges of the bot. *(bots)*
- `get_bot_info(lang_code; optional: bot)` — Get the bot info in given language. *(users, bots)*
- `get_chat_menu_button(optional: chat_id)` — Get the current value of the bot's menu button in a private chat, or the default menu button. *(bots)*
- `get_collectible_item_info(optional: username, phone_number)` — Returns information about a given collectible item that was purchased at https://fragment.com. *(users)*
- `get_game_high_scores(user_id, chat_id; optional: message_id)` — Get data for high score tables. *(bots)*
- `get_inline_bot_results(bot; optional: query, offset, latitude, longitude)` — Get bot results via inline queries. *(users)*
- `get_login_url(chat_id, message_id, button_id; optional: allow_write_access, allow_phone_number_access)` — Get an HTTP URL which automatically authorizes the user on a website after pressing an inline button of type `types.LoginUrl`. *(users)*
- `get_login_url_info(chat_id, message_id, button_id)` — Get information about an inline button of type `types.LoginUrl`. *(users)*
- `get_managed_bot_access_settings(bot_id)` — Get the access settings of a bot managed by the current bot. *(bots)*
- `get_managed_bot_token(bot_id; optional: revoke)` — Get the token of a bot managed by the current bot. *(bots)*
- `get_owned_bots()` — Returns the list of bots owned by the current user. *(users)*
- `get_prepared_keyboard_button(bot_id, prepared_button_id)` — Get a keyboard button prepared by a bot for the current user in a Mini App. *(users)*
- `get_similar_bots(bot)` — Get a list of bots similar to the target bot. *(users)*
- `request_callback_answer(chat_id, message_id, callback_data; optional: password, timeout)` — Request a callback answer from bots. *(users)*
- `save_prepared_keyboard_button(user_id, button)` — Prepare a keyboard button to be used by a user in a Mini App of the bot. *(bots)*
- `send_game(chat_id, game_short_name; optional: disable_notification, message_thread_id, business_connection_id, reply_to_message_id, protect_content, allow_paid_broadcast, message_effect_id, reply_markup)` — Send a game. *(bots)*
- `send_inline_bot_result(chat_id, query_id, result_id; optional: disable_notification, message_thread_id, reply_to_message_id, reply_to_chat_id, reply_to_story_id, quote_text, parse_mode, quote_entities, quote_offset, schedule_date)` — Send an inline bot result. *(users)*
- `set_bot_commands(commands; optional: scope, language_code)` — Set the list of the bot's commands. *(bots)*
- `set_bot_default_privileges(optional: privileges, for_channels)` — Change the default privileges requested by the bot when it's added as an administrator to groups or channels. *(bots)*
- `set_bot_info(lang_code; optional: bot, name, about, description)` — Get the bot info in given language. *(users, bots)*
- `set_chat_menu_button(optional: chat_id, menu_button)` — Change the bot's menu button in a private chat, or the default menu button. *(bots)*
- `set_game_score(user_id, score; optional: force, disable_edit_message, chat_id, message_id)` — Set the score of the specified user in a game. *(bots)*
- `set_managed_bot_access_settings(bot_id, is_restricted; optional: added_user_ids)` — Change the access settings of a bot managed by the current bot. *(bots)*
- `share_chat_with_bot(chat_id, button_id, shared_chat_id; optional: message_id, prepared_button_id)` — Share a chat with a bot after pressing a button that requests a chat. *(users)*
- `share_users_with_bot(chat_id, button_id, user_ids; optional: message_id, prepared_button_id)` — Share users with a bot after pressing a button that requests users, or a button that requests the creation of a managed bot (share the created bot). *(users)*
- `toggle_bot_username_is_active(bot_id, username, is_active)` — Activate or disable a username of a bot owned by the current user. *(users)*

### business (8)

- `answer_pre_checkout_query(pre_checkout_query_id; optional: success, error)` — Send answers to pre-checkout queries. *(bots)*
- `answer_shipping_query(shipping_query_id, ok; optional: shipping_options, error_message)` — If you sent an invoice requesting a shipping address and the parameter is_flexible was specified, the API sends the confirmation in the form of an `handlers.ShippingQueryHandler`. *(bots)*
- `confirm_business_connected_bot(bot_id)` — Confirm an unconfirmed connection of a business bot to the current user's account. *(users)*
- `delete_business_messages(business_connection_id, message_ids)` — Delete messages on behalf of a business account. *(bots)*
- `get_business_account_gifts(business_connection_id; optional: exclude_unsaved, exclude_saved, exclude_unlimited, exclude_limited, exclude_upgraded, sort_by_price, limit, offset, exclude_upgradable, exclude_unupgradable)` — Return the gifts received and owned by a managed business account. *(bots)*
- `get_business_account_star_balance(business_connection_id)` — Return the amount of Telegram Stars owned by a managed business account. *(bots)*
- `get_business_connection(business_connection_id)` — Use this method to get information about the connection of the bot with a business account. *(bots)*
- `transfer_business_account_stars(business_connection_id, star_count)` — Transfers Telegram Stars from the business account balance to the bot’s balance. *(users)*

### chats (54)

- `add_chat_members(chat_id, user_ids; optional: forward_limit)` — Add new chat members to a group, supergroup or channel. *(users)*
- `add_chat_welcome_message(chat_id; optional: text, media, rich_message, parse_mode, entities, invert_media)` — Add a message to the welcome messages of a chat, which are shown to its new members. *(users)*
- `archive_chats(chat_ids)` — Archive one or more chats. *(users)*
- `ban_chat_member(chat_id, user_id; optional: until_date, revoke_messages)` — Ban a user from a group, a supergroup or a channel. *(users, bots)*
- `create_channel(title; optional: description)` — Create a new broadcast channel. *(users)*
- `create_group(title, users)` — Create a new basic group. *(users)*
- `create_supergroup(title; optional: description)` — Create a new supergroup. *(users)*
- `delete_all_chat_welcome_messages(chat_id)` — Delete all the welcome messages of a chat. *(users)*
- `delete_channel(chat_id)` — Delete a channel. *(users)*
- `delete_chat_photo(chat_id)` — Delete a chat photo. *(users, bots)*
- `delete_chat_welcome_message(chat_id, welcome_message_id)` — Delete a welcome message of a chat. *(users)*
- `delete_folder(folder_id)` — Delete a user's folder. *(users)*
- `delete_supergroup(chat_id)` — Delete a supergroup. *(users)*
- `delete_user_history(chat_id, user_id)` — Delete all messages sent by a certain user in a supergroup. *(users)*
- `edit_chat_welcome_message(chat_id, welcome_message_id; optional: text, media, rich_message, parse_mode, entities, invert_media)` — Edit a welcome message of a chat. *(users)*
- `export_folder_link(folder_id)` — Export link to a user's folder. *(users)*
- `get_chat(chat_id)` — Get up to date information about a chat. *(users, bots)*
- `get_chat_event_log(chat_id; optional: query, offset_id, limit, filters, user_ids)` — Get the actions taken by chat members and administrators in the last 48h. *(users)*
- `get_chat_member(chat_id, user_id)` — Get information about one member of a chat. *(users, bots)*
- `get_chat_members(chat_id; optional: query, limit, filter)` — Get the members list of a chat. *(users, bots)*
- `get_chat_members_count(chat_id; optional: join_request)` — Get the number of members in a chat. *(users, bots)*
- `get_chat_online_count(chat_id)` — Get the number of members that are currently online in a chat. *(users)*
- `get_chat_owner_after_leaving(chat_id)` — Get the user who will become the owner of a chat if the current user leaves it. *(users)*
- `get_chat_welcome_messages(chat_id)` — Get the welcome messages of a chat. *(users)*
- `get_dialogs(optional: limit)` — Get a user's dialogs sequentially. *(users)*
- `get_dialogs_count(optional: pinned_only)` — Get the total count of your dialogs. *(users)*
- `get_folders(optional: folder_ids)` — Get one or more folders by using folder identifiers. *(users)*
- `get_guard_bot_web_app_url(query_id; optional: platform, theme_params)` — Get the URL of the Web App in which a guard bot decides about a join request of the current user. *(users)*
- `get_send_as_chats(chat_id)` — Get the list of "send_as" chats available. *(users)*
- `join_chat(chat_id)` — Join a group chat or channel. *(users)*
- `leave_chat(chat_id; optional: delete)` — Leave a group chat or channel. *(users, bots)*
- `mark_chat_unread(chat_id)` — Mark a chat as unread. *(users)*
- `pin_chat_message(chat_id, message_id; optional: disable_notification, both_sides, business_connection_id)` — Pin a message in a group, channel or your own chat. *(users, bots)*
- `process_chat_protected_content_disable_request(chat_id, request_message_id, approve)` — Approve or reject a request to disable the content protection of a chat. *(users)*
- `promote_chat_member(chat_id, user_id; optional: privileges, title)` — Promote or demote a user in a supergroup or a channel. *(users, bots)*
- `restrict_chat_member(chat_id, user_id, permissions; optional: until_date)` — Restrict a user in a supergroup. *(users, bots)*
- `set_administrator_title(chat_id, user_id, title)` — Set a custom title (rank) to an administrator of a supergroup. *(users, bots)*
- `set_chat_description(chat_id, description)` — Change the description of a supergroup or a channel. *(users, bots)*
- `set_chat_join_by_request(chat_id, enabled; optional: guard_bot_id, apply_to_invite_links)` — Toggle whether the users who join a supergroup directly must be approved first. *(users, bots)*
- `set_chat_member_tag(chat_id, user_id, tag)` — Change the tag of a chat member; for basic groups and supergroups only. *(users, bots)*
- `set_chat_permissions(chat_id, permissions)` — Set default chat permissions for all members. *(users, bots)*
- `set_chat_photo(chat_id; optional: photo, emoji, emoji_background, video, video_start_ts)` — Set a new chat photo or video (H.264/MPEG-4 AVC video, max 5 seconds). *(users, bots)*
- `set_chat_protected_content(chat_id, enabled)` — Set the chat protected content setting. *(users)*
- `set_chat_title(chat_id, title)` — Change the title of a chat. *(users, bots)*
- `set_chat_username(chat_id, username)` — Set a channel or a supergroup username. *(users)*
- `set_send_as_chat(chat_id, send_as_chat_id)` — Set the default "send_as" chat for a chat. *(users)*
- `set_slow_mode(chat_id, seconds)` — Set the slow mode interval for a chat. *(users)*
- `transfer_chat_ownership(chat_id, user_id, password)` — Transfer the owner of a basic group, supergroup or channel to another user. *(users)*
- `unarchive_chats(chat_ids)` — Unarchive one or more chats. *(users)*
- `unban_chat_member(chat_id, user_id)` — Unban a previously banned user in a supergroup or channel. *(users, bots)*
- `unpin_all_chat_messages(chat_id; optional: message_thread_id)` — Use this method to clear the list of pinned messages in a chat. *(users, bots)*
- `unpin_chat_message(chat_id; optional: message_id, business_connection_id)` — Unpin a message in a group, channel or your own chat. *(users, bots)*
- `update_color(chat_id, color; optional: background_emoji_id)` — Update color. *(users)*
- `update_folder(folder_id, title; optional: title_entities, included_chats, excluded_chats, pinned_chats, contacts, non_contacts, groups, channels, bots, exclude_muted, exclude_read, exclude_archived, color, emoji, … +1)` — Create or update a user's folder. *(users)*

### communities (13)

- `add_chat_to_community(community_id, chat_id; optional: is_hidden)` — Add a chat to a community. *(users)*
- `approve_all_community_chat_requests(community_id)` — Approve all pending requests to add chats to a community. *(users)*
- `approve_community_chat_request(community_id, chat_id)` — Approve a request to add a chat to a community. *(users)*
- `ban_community_member(community_id, user_id)` — Ban a user from a community. *(users)*
- `create_community(title, chat_id; optional: description, is_chat_hidden)` — Create a community: a group of supergroups, channels and chats with bots. *(users)*
- `decline_all_community_chat_requests(community_id)` — Decline all pending requests to add chats to a community. *(users)*
- `decline_community_chat_request(community_id, chat_id)` — Decline a request to add a chat to a community. *(users)*
- `get_community_chat_requests(community_id; optional: limit)` — Get the pending requests to add chats to a community. *(users)*
- `get_community_member_chats(community_id, user_id)` — Get the chats of a community that a member owns or joined. *(users)*
- `get_joined_communities()` — Get the communities you are a member of. *(users)*
- `remove_chat_from_community(community_id, chat_id)` — Remove a chat from a community. *(users)*
- `set_community_collapsed(community_id; optional: collapsed)` — Collapse the chats of a community into a single entry of the chat list, or expand them. *(users)*
- `unban_community_member(community_id, user_id)` — Unban a user previously banned from a community. *(users)*

### contacts (5)

- `add_contact(user_id, first_name; optional: last_name, phone_number, share_phone_number)` — Add an existing Telegram user as contact, even without a phone number. *(users)*
- `delete_contacts(user_ids)` — Delete contacts from your Telegram address book. *(users)*
- `get_contacts()` — Get contacts from your Telegram address book. *(users)*
- `get_contacts_count()` — Get the total count of contacts from your Telegram address book. *(users)*
- `import_contacts(contacts)` — Import contacts to your Telegram address book. *(users)*

### forums (13)

- `close_forum_topic(chat_id, topic_id)` — Close a forum topic. *(users, bots)*
- `close_general_topic(chat_id)` — Close a forum topic. *(users, bots)*
- `create_forum_topic(chat_id, title; optional: icon_color, icon_emoji_id)` — Create a new forum topic. *(users, bots)*
- `delete_forum_topic(chat_id, topic_id)` — Delete a forum topic. *(users, bots)*
- `edit_forum_topic(chat_id, topic_id; optional: title, icon_emoji_id)` — Edit a forum topic. *(users, bots)*
- `edit_general_topic(chat_id, title)` — Edit a general forum topic. *(users, bots)*
- `get_forum_topics(chat_id; optional: limit)` — Get forum topics from a chat. *(users)*
- `get_forum_topics_by_id(chat_id, topic_ids)` — Get one or more topic from a chat by using topic identifiers. *(users)*
- `get_forum_topics_count(chat_id)` — Get forum topics count from a chat. *(users)*
- `hide_general_topic(chat_id)` — hide a general forum topic. *(users, bots)*
- `reopen_forum_topic(chat_id, topic_id)` — Reopen a forum topic. *(users, bots)*
- `reopen_general_topic(chat_id)` — Reopen a general forum topic. *(users, bots)*
- `unhide_general_topic(chat_id)` — unhide a general forum topic. *(users, bots)*

### invite_links (17)

- `approve_all_chat_join_requests(chat_id; optional: invite_link)` — Approve all pending join requests in a chat. *(users)*
- `approve_chat_join_request(chat_id, user_id)` — Approve a chat join request. *(users, bots)*
- `create_chat_invite_link(chat_id; optional: name, expire_date, member_limit, creates_join_request, subscription_period, subscription_price)` — Create an additional invite link for a chat. *(users, bots)*
- `decline_all_chat_join_requests(chat_id; optional: invite_link)` — Decline all pending join requests in a chat. *(users)*
- `decline_chat_join_request(chat_id, user_id)` — Decline a chat join request. *(users, bots)*
- `delete_chat_admin_invite_links(chat_id, admin_id)` — Delete all revoked invite links of an administrator. *(users)*
- `delete_chat_invite_link(chat_id, invite_link)` — Delete an already revoked invite link. *(users)*
- `edit_chat_invite_link(chat_id, invite_link; optional: name, expire_date, member_limit, creates_join_request)` — Edit a non-primary invite link. *(users, bots)*
- `export_chat_invite_link(chat_id)` — Generate a new primary invite link for a chat; any previously generated primary link is revoked. *(users, bots)*
- `get_chat_admin_invite_links(chat_id, admin_id; optional: revoked, limit)` — Get the invite links created by an administrator in a chat. *(users)*
- `get_chat_admin_invite_links_count(chat_id, admin_id; optional: revoked)` — Get the count of the invite links created by an administrator in a chat. *(users)*
- `get_chat_admins_with_invite_links(chat_id)` — Get the list of the administrators that have exported invite links in a chat. *(users)*
- `get_chat_invite_link(chat_id, invite_link)` — Get detailed information about a chat invite link. *(users, bots)*
- `get_chat_invite_link_joiners(chat_id, invite_link; optional: limit)` — Get the members who joined the chat with the invite link. *(users)*
- `get_chat_invite_link_joiners_count(chat_id, invite_link)` — Get the count of the members who joined the chat with the invite link. *(users)*
- `get_chat_join_requests(chat_id; optional: limit, query)` — Get the pending join requests of a chat. *(users)*
- `revoke_chat_invite_link(chat_id, invite_link)` — Revoke a previously created invite link. *(users, bots)*

### messages (105)

- `add_poll_option(chat_id, message_id, text; optional: parse_mode, entities, media)` — Add an answer option to a poll that allows it. *(users)*
- `add_task_to_todo(chat_id, message_id, tasks; optional: parse_mode)` — Add tasks to a todo list.
- `add_text_composition_style(style)` — Add a custom text composition style to the styles of the current user. *(users)*
- `compose_rich_message_with_ai(rich_message; optional: translate_to_language_code, style, custom_prompt, add_emojis)` — Change a rich message using an AI model. *(users)*
- `compose_text_with_ai(text; optional: translate_to_language_code, style, add_emojis, parse_mode, entities)` — Change a text using an AI model: translate it, rewrite it in a style and/or add emojis to it. *(users)*
- `copy_media_group(chat_id, from_chat_id, message_id; optional: captions, has_spoilers, disable_notification, message_thread_id, send_as, reply_to_message_id, reply_to_chat_id, reply_to_story_id, quote_text, parse_mode, quote_entities, quote_offset, schedule_date, invert_media, … +3)` — Copy a media group by providing one of the message ids. *(users, bots)*
- `copy_message(chat_id, from_chat_id, message_id; optional: caption, parse_mode, caption_entities, has_spoiler, disable_notification, message_thread_id, reply_to_message_id, reply_to_chat_id, schedule_date, protect_content, allow_paid_broadcast, invert_media, reply_markup)` — Copy messages of any kind. *(users, bots)*
- `create_rich_message_with_ai(prompt; optional: language_code, add_emojis)` — Create a new rich message using an AI model. *(users)*
- `create_text_composition_style(title, custom_emoji_id, prompt; optional: show_creator)` — Create a custom text composition style. *(users)*
- `delete_all_recent_message_reactions_from_sender(chat_id, sender_id)` — Delete all recent reactions added by a sender in a chat. *(users, bots)*
- `delete_chat_history(chat_id; optional: max_id, revoke, just_clear, min_date, max_date)` — Delete the history of a chat. *(users)*
- `delete_ephemeral_message(chat_id, receiver_user_id, ephemeral_message_id)` — Delete an ephemeral message. *(bots)*
- `delete_message_reactions_from_sender(chat_id, message_id, sender_id)` — Delete all reactions added by a sender to a message. *(users, bots)*
- `delete_messages(chat_id, message_ids; optional: revoke)` — Delete messages, including service messages. *(users, bots)*
- `delete_poll_option(chat_id, message_id, option)` — Delete an answer option that was added to a poll after its creation. *(users)*
- `delete_scheduled_messages(chat_id, message_ids)` — Delete scheduled messages. *(users, bots)*
- `delete_text_composition_style(style)` — Permanently delete a custom text composition style created by the current user. *(users)*
- `download_media(message; optional: file_name, in_memory, block, progress, progress_args)` — Download the media from a message. *(users, bots)*
- `edit_ephemeral_message(chat_id, receiver_user_id, ephemeral_message_id; optional: text, media, rich_message, parse_mode, entities, reply_markup, invert_media)` — Edit the text, the media or the reply markup of an ephemeral message sent by the bot. *(bots)*
- `edit_ephemeral_message_caption(chat_id, receiver_user_id, ephemeral_message_id, caption; optional: parse_mode, caption_entities, reply_markup, invert_media)` — Edit the caption of a media ephemeral message sent by the bot. *(bots)*
- `edit_inline_caption(inline_message_id, caption; optional: parse_mode, reply_markup, invert_media)` — Edit the caption of inline media messages. *(bots)*
- `edit_inline_media(inline_message_id, media; optional: reply_markup)` — Edit inline animation, audio, document, photo or video messages, or replace inline text with inline animation, audio, document, photo or video messages. *(bots)*
- `edit_inline_reply_markup(inline_message_id; optional: reply_markup)` — Edit only the reply markup of inline messages sent via the bot (for inline bots). *(bots)*
- `edit_inline_rich_message(inline_message_id, rich_message; optional: reply_markup)` — Edit the text of an inline text or rich message sent via the bot, replacing it with a rich message. *(bots)*
- `edit_inline_text(inline_message_id, text; optional: parse_mode, disable_web_page_preview, reply_markup, invert_media)` — Edit the text of inline messages. *(bots)*
- `edit_message_caption(chat_id, message_id, caption; optional: parse_mode, caption_entities, invert_media, reply_markup, business_connection_id)` — Edit the caption of media messages. *(users, bots)*
- `edit_message_media(chat_id, message_id, media; optional: reply_markup, file_name, invert_media, business_connection_id)` — Edit animation, audio, document, photo or video messages, or replace text with animation, audio, document, photo or video messages. *(users, bots)*
- `edit_message_reply_markup(chat_id, message_id; optional: reply_markup, business_connection_id)` — Edit only the reply markup of messages sent by the bot. *(bots)*
- `edit_message_rich_message(chat_id, message_id, rich_message; optional: reply_markup, business_connection_id)` — Edit a text or rich message, replacing its content with a rich message. *(users, bots)*
- `edit_message_text(chat_id, message_id, text; optional: parse_mode, entities, disable_web_page_preview, invert_media, reply_markup, business_connection_id)` — Edit the text of messages. *(users, bots)*
- `edit_text_composition_style(style; optional: title, custom_emoji_id, prompt, show_creator)` — Edit a custom text composition style created by the current user. *(users)*
- `fix_rich_message_with_ai(rich_message)` — Fix the spelling and grammar of a rich message using an AI model. *(users)*
- `fix_text_with_ai(text; optional: parse_mode, entities)` — Fix mistakes in a text using an AI model. *(users)*
- `forward_media_group(chat_id, from_chat_id, message_id; optional: message_thread_id, disable_notification, schedule_date, hide_sender_name, hide_captions, protect_content)` — Forward a media group by providing one of the message ids. *(users, bots)*
- `forward_messages(chat_id, from_chat_id, message_ids; optional: message_thread_id, disable_notification, schedule_date, protect_content, allow_paid_broadcast, drop_author, remove_caption, new_video_start_timestamp, from_ephemeral)` — Forward messages of any kind. *(users, bots)*
- `get_available_effects()` — Get all available effects. *(users)*
- `get_chat_history(chat_id; optional: limit, offset, offset_id, offset_date, min_id, max_id, reverse)` — Get messages from a chat history. *(users)*
- `get_chat_history_count(chat_id)` — Get the total count of messages in a chat. *(users)*
- `get_custom_emoji_stickers(custom_emoji_ids)` — Get information about custom emoji stickers by their identifiers. *(users, bots)*
- `get_discussion_message(chat_id, message_id)` — Get the first discussion message of a channel post or a discussion thread in a group. *(users)*
- `get_discussion_replies(chat_id, message_id; optional: limit)` — Get the message replies of a discussion thread. *(users)*
- `get_discussion_replies_count(chat_id, message_id)` — Get the total count of replies in a discussion thread. *(users)*
- `get_full_rich_message(chat_id, message_id)` — Get the full version of a rich message received partially (see `types.RichMessage` `is_full`). *(users)*
- `get_media_group(chat_id, message_id)` — Get the media group a message belongs to. *(users, bots)*
- `get_message_read_participants(chat_id, message_id)` — Get the list of users who have read a message. *(users)*
- `get_messages(chat_id; optional: message_ids, reply_to_message_ids, replies)` — Get one or more messages from a chat by using message identifiers. *(users, bots)*
- `get_personal_chat_history(user_id; optional: limit)` — Get the last messages of the personal chat of a user. *(bots)*
- `get_poll_vote_statistics(chat_id, message_id; optional: is_dark)` — Get statistics about the votes of a poll. *(users)*
- `get_scheduled_messages(chat_id, message_ids)` — Get one or more scheduled messages from a chat by using message identifiers. *(users, bots)*
- `get_stake_dice_state()` — Get the current state of the stake dice of the current user. *(users)*
- `get_text_composition_style_example(style; optional: example_number)` — Get an example of how a text composition style changes a text. *(users)*
- `get_text_composition_styles()` — Get the text composition styles of the current user: the default styles provided by Telegram and the custom styles added by the user. *(users)*
- `get_unread_poll_votes(chat_id; optional: message_thread_id, limit)` — Get the poll messages of a chat that received votes the current user didn't see yet. *(users)*
- `listen_to_audio(audio_file_id, duration)` — Inform Telegram that the current user listened to an audio file (a song). *(users)*
- `read_all_chat_poll_votes(chat_id; optional: message_thread_id)` — Mark all the unseen poll votes of a chat as read. *(users)*
- `read_chat_history(chat_id; optional: max_id)` — Mark a chat's message history as read. *(users)*
- `remove_text_composition_style(style)` — Remove a custom text composition style from the styles of the current user. *(users)*
- `report_ephemeral_message(chat_id, ephemeral_message_id; optional: option_id, text)` — Report an ephemeral message to Telegram. *(users)*
- `request_ephemeral_callback_answer(chat_id, ephemeral_message_id, callback_data; optional: timeout)` — Press a callback button of an ephemeral message sent by a bot, and get the answer of the bot. *(users)*
- `retract_vote(chat_id, message_id)` — Retract your vote in a poll. *(users)*
- `search_global(optional: query, filter, channels_only, groups_only, users_only, limit, community_id)` — Search messages globally from all of your chats. *(users)*
- `search_global_count(optional: query, filter, channels_only, groups_only, users_only, community_id)` — Get the count of messages resulting from a global search. *(users)*
- `search_global_hashtag_messages(optional: hashtag, offset_id, offset_date, limit)` — Searches for public channel posts with the given hashtag. *(users)*
- `search_global_hashtag_messages_count(optional: hashtag)` — Get the count of messages with the provided hashtag. *(users)*
- `search_messages(chat_id; optional: query, offset, filter, limit, from_user, thread_id)` — Search for text and media messages inside a specific chat. *(users)*
- `search_messages_count(chat_id; optional: query, filter, from_user, thread_id)` — Get the count of messages resulting from a search inside a chat. *(users)*
- `search_text_composition_style(style)` — Find a text composition style by its name, for example the name in a https://t.me/addstyle/<name> link. *(users)*
- `send_animation(chat_id, animation; optional: caption, unsave, parse_mode, caption_entities, has_spoiler, duration, width, height, thumb, file_name, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, … +13)` — Send animation files (animation or H.264/MPEG-4 AVC video without sound). *(users, bots)*
- `send_audio(chat_id, audio; optional: caption, parse_mode, caption_entities, duration, performer, title, thumb, file_name, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, … +10)` — Send audio files. *(users, bots)*
- `send_cached_media(chat_id, file_id; optional: caption, parse_mode, caption_entities, has_spoiler, disable_notification, message_thread_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, schedule_date, protect_content, … +4)` — Send any media stored on the Telegram servers using a file_id. *(users, bots)*
- `send_chat_action(chat_id, action; optional: message_thread_id, business_connection_id, emoji, emoji_message_id, emoji_message_interaction)` — Tell the other party that something is happening on your side. *(users, bots)*
- `send_contact(chat_id, phone_number, first_name; optional: last_name, vcard, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, parse_mode, schedule_date, protect_content, allow_paid_broadcast, … +2)` — Send phone contacts. *(users, bots)*
- `send_dice(chat_id; optional: emoji, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, parse_mode, schedule_date, protect_content, allow_paid_broadcast, … +2)` — Send a dice with a random value from 1 to 6. *(users, bots)*
- `send_document(chat_id, document; optional: thumb, caption, parse_mode, caption_entities, file_name, force_document, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, … +8)` — Send generic files. *(users, bots)*
- `send_ephemeral_message(chat_id, receiver_user_id; optional: text, media, rich_message, parse_mode, entities, reply_markup, reply_to_message_id, reply_to_ephemeral_message_id, message_thread_id, callback_query_id, replace_callback_query_message, protect_content, invert_media)` — Send an ephemeral message: a message in a chat that only its receiver can see. *(users, bots)*
- `send_location(chat_id, latitude, longitude; optional: horizontal_accuracy, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, parse_mode, schedule_date, protect_content, allow_paid_broadcast, message_effect_id, … +1)` — Send points on the map. *(users, bots)*
- `send_media_group(chat_id, media; optional: disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, parse_mode, schedule_date, protect_content, allow_paid_broadcast, message_effect_id, … +3)` — Send a group of photos or videos as an album. *(users, bots)*
- `send_message(chat_id, text; optional: parse_mode, entities, disable_web_page_preview, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, schedule_date, protect_content, … +5)` — Send text messages. *(users, bots)*
- `send_message_view_metrics(chat_id, message_id, time_in_view_ms, active_time_in_view_ms, height_to_viewport_ratio_per_mille, seen_range_ratio_per_mille)` — Report details of a view of a message by the current user in a chat, a message thread or a forum topic. *(users)*
- `send_photo(chat_id, photo; optional: caption, parse_mode, caption_entities, has_spoiler, ttl_seconds, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, … +10)` — Send photos. *(users, bots)*
- `send_poll(chat_id, question, options; optional: question_entities, is_anonymous, type, allows_multiple_answers, correct_option_id, explanation, explanation_parse_mode, explanation_entities, open_period, close_date, is_closed, disable_notification, protect_content, allow_paid_broadcast, … +22)` — Send a new poll. *(users, bots)*
- `send_reaction(chat_id; optional: message_id, story_id, emoji, big, add_to_recent)` — Use this method to send reactions on a message/stories. *(users, bots)*
- `send_rich_message(chat_id, rich_message; optional: disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, parse_mode, schedule_date, protect_content, allow_paid_broadcast, message_effect_id, … +2)` — Send a rich message: a message with headings, lists, tables, quotes, media, formulas, buttons and more. *(users, bots)*
- `send_rich_message_draft(chat_id, draft_id, rich_message; optional: message_thread_id, can_stop, keep_on_stop)` — Send a draft of a rich message being generated, shown to the user as a pending message. *(bots)*
- `send_stake_dice(chat_id, state_hash, stake_gram_amount; optional: disable_notification, message_thread_id, reply_to_message_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, parse_mode, protect_content, message_effect_id)` — Send a stake dice: a "🎲" dice rolled for a stake in TON, whose prize depends on the rolled value. *(users)*
- `send_sticker(chat_id, sticker; optional: emoji, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, parse_mode, schedule_date, protect_content, allow_paid_broadcast, … +4)` — Send static .webp or animated .tgs stickers. *(users, bots)*
- `send_text_message_draft(chat_id, draft_id; optional: text, parse_mode, entities, message_thread_id, can_stop, keep_on_stop)` — Send a live draft of a message being generated (live response streaming). *(users, bots)*
- `send_todo(chat_id, title, tasks; optional: entities, can_append, can_complete, parse_mode)` — Send a todo list to a chat.
- `send_venue(chat_id, latitude, longitude, title, address; optional: foursquare_id, foursquare_type, google_place_id, google_place_type, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_chat_id, quote_text, quote_entities, parse_mode, schedule_date, protect_content, … +3)` — Send information about a venue. *(users, bots)*
- `send_video(chat_id, video; optional: caption, parse_mode, caption_entities, has_spoiler, ttl_seconds, duration, width, height, thumb, file_name, supports_streaming, disable_notification, message_thread_id, business_connection_id, … +17)` — Send video files. *(users, bots)*
- `send_video_note(chat_id, video_note; optional: duration, length, thumb, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, parse_mode, schedule_date, … +7)` — Send video messages. *(users, bots)*
- `send_voice(chat_id, voice; optional: caption, parse_mode, caption_entities, duration, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, quote_entities, schedule_date, … +6)` — Send audio files. *(users, bots)*
- `send_web_page(chat_id; optional: url, text, parse_mode, entities, large_media, invert_media, disable_notification, message_thread_id, business_connection_id, reply_to_message_id, reply_to_story_id, reply_to_chat_id, reply_to_monoforum_id, quote_text, … +6)` — Send Web Page Preview. *(users, bots)*
- `set_chat_draft_message(chat_id; optional: text, parse_mode, entities, rich_message, message_thread_id, reply_to_message_id, disable_web_page_preview, invert_media, message_effect_id)` — Change the message draft of a chat or of a forum topic: a text or a rich message. *(users)*
- `set_todo_tasks_completion(chat_id, message_id; optional: completed_ids, incompleted_ids)` — Set the completion status of one or more todo tasks.
- `start_bot(chat_id; optional: param)` — Start bot. *(users)*
- `stop_pending_message(chat_id, draft_id; optional: message_thread_id)` — Stop the generation of a pending message (a live draft) by a bot. *(users)*
- `stop_poll(chat_id, message_id; optional: reply_markup, business_connection_id)` — Stop a poll which was sent by you. *(users, bots)*
- `stream_media(message; optional: limit, offset)` — Stream the media from a message chunk by chunk. *(users, bots)*
- `summarize_message(chat_id, message_id; optional: translate_to_language_code, tone)` — Summarize the text of a message using an AI model. *(users)*
- `transcribe_audio(chat_id, message_id)` — Transcribes the audio of a voice message.
- `translate_message_rich_message(chat_id, message_id, to_language_code; optional: tone)` — Translate the rich message of a message to the given language. *(users)*
- `translate_rich_message(rich_message, to_language_code; optional: tone)` — Translate a rich message to the given language. *(users)*
- `translate_message_text(to_language_code; optional: chat_id, message_ids, text, parse_mode, entities, tone)` — Translates a text or message(s) to the given language. *(`translate_text.py`)*
- `vote_poll(chat_id, message_id, options)` — Vote a poll. *(users)*

### password (3)

- `change_cloud_password(current_password, new_password; optional: new_hint)` — Change your Two-Step Verification password (Cloud Password) with a new one. *(users)*
- `enable_cloud_password(password; optional: hint, email)` — Enable the Two-Step Verification security feature (Cloud Password) on your account. *(users)*
- `remove_cloud_password(password)` — Turn off the Two-Step Verification security feature (Cloud Password) on your account. *(users)*

### payments (29)

- `apply_gift_code(link)` — Apply a gift code. *(users)*
- `check_gift_code(link)` — Get information about a gift code. *(users; `check_giftcode.py`)*
- `convert_gift(message_id)` — Convert star gift to stars. *(users)*
- `craft_gift(owned_gift_ids)` — Craft a new gift from other upgraded gifts, which will be permanently lost. *(users)*
- `create_invoice_link(title, description, payload, currency, prices; optional: provider_token, start_parameter, provider_data, photo_url, photo_size, photo_width, photo_height, need_name, need_phone_number, need_email, need_shipping_address, send_phone_number_to_provider, send_email_to_provider, is_flexible)` — Use this method to create a link for an invoice. *(bots)*
- `get_available_gifts()` — Get all available star gifts to send. *(users)*
- `get_chat_gifts(chat_id; optional: exclude_unsaved, exclude_saved, exclude_unlimited, exclude_limited, exclude_upgraded, sort_by_value, limit, offset, exclude_upgradable, exclude_unupgradable)` — Get all gifts owned by specified chat. *(users)*
- `get_chat_gifts_count(chat_id; optional: exclude_unsaved, exclude_saved, exclude_unlimited, exclude_limited, exclude_upgraded, exclude_upgradable, exclude_unupgradable)` — Get the total count of owned gifts of specified chat. *(users)*
- `get_currency_exchange_rates()` — Get the exchange rates of currencies to USD. *(users)*
- `get_gifts_for_crafting(gift_id; optional: limit, offset)` — Get the upgraded gifts of the current user that can be used to craft another gift. *(users)*
- `get_payment_form(optional: chat_id, message_id, invoice_link)` — Get information about a invoice or paid media. *(users)*
- `get_stars_balance(optional: chat_id)` — Get the current Telegram Stars balance of the current account. *(users)*
- `get_stars_transactions(optional: chat_id, limit, offset, is_inbound, is_outbound, is_ascending)` — Get stars transactions. *(users, bots)*
- `get_stars_transactions_by_id(transaction_ids; optional: chat_id)` — Get stars transactions by transaction id. *(users, bots)*
- `get_upgraded_gift(link)` — Get information about upgraded gift. *(users)*
- `hide_gift(message_id)` — Hide the star gift from your profile. *(users)*
- `refund_star_payment(user_id, telegram_payment_charge_id)` — Refund the star to the user. *(`refund_stars_payment.py`)*
- `search_gifts_for_resale(gift_id; optional: order, attributes, limit, offset, for_crafting, for_stars)` — Get upgraded gifts that can be bought from other owners. *(users)*
- `send_gift(chat_id, gift_id; optional: text, parse_mode, entities, hide_my_name, pay_for_upgrade)` — Send star gift. *(users)*
- `send_invoice(chat_id, title, description, currency, prices; optional: provider, provider_data, payload, photo_url, photo_size, photo_mime_type, start_parameter, extended_media, reply_to_message_id, message_thread_id, quote_text, allow_paid_broadcast, message_effect_id, quote_entities, … +1)` — Use this method to send invoices. *(bots)*
- `send_paid_media(chat_id, stars_amount, media; optional: business_connection_id, caption, caption_entities, parse_mode, disable_notification, schedule_date, protect_content, invert_media, payload)` — Send paid media. *(users, bots)*
- `send_paid_reaction(chat_id, message_id, amount; optional: anonymous)` — Use this method to send paid reactions on a channel message. *(users)*
- `send_payment_form(optional: chat_id, message_id, invoice_link)` — Pay an invoice. *(users)*
- `send_resold_gift(gift_link, new_owner_chat_id; optional: text, parse_mode, entities, is_private)` — Send an upgraded gift that is available for resale to another user or channel chat. *(users)*
- `set_gift_resale_price(owned_gift_id, resale_star_count)` — Change resale price of a unique gift owned by the current user. *(users)*
- `set_pinned_gifts(owner_id, owned_gift_ids)` — Change the list of pinned gifts on the current user. *(users)*
- `show_gift(message_id)` — Display the star gift in your profile. *(users)*
- `transfer_gift(message_id, to_chat_id)` — Transfer star gift to another user. *(users)*
- `upgrade_gift(message_id; optional: keep_details)` — Upgrade star gift to unique. *(users)*

### phone (1)

- `get_call_members(chat_id; optional: limit)` — Get the members list of a chat call. *(users)*

### pyromod (12)

- `ask(chat_id, text, *args; optional: filters, listener_type, timeout, unallowed_click_alert, user_id, message_id, inline_message_id, **kwargs)` — Send a message then listen for a message, callback query, etc. *(users, bots)*
- `get_listener_matching_with_data(data, listener_type)` — Gets a listener that matches the given data. *(users, bots; not a coroutine)*
- `get_listener_matching_with_identifier_pattern(pattern, listener_type)` — Gets a listener that matches the given identifier pattern. *(users, bots; not a coroutine)*
- `get_many_listeners_matching_with_data(data, listener_type)` — Gets multiple listener that matches the given data. *(users, bots; not a coroutine)*
- `get_many_listeners_matching_with_identifier_pattern(pattern, listener_type)` — Gets multiple listener that matches the given identifier pattern. *(users, bots; not a coroutine)*
- `listen(optional: filters, listener_type, timeout, unallowed_click_alert, chat_id, user_id, message_id, inline_message_id)` — Listen for a message, callback query, etc. *(users, bots)*
- `register_next_step_handler(callback; optional: filters, listener_type, unallowed_click_alert, chat_id, user_id, message_id, inline_message_id)` — Registers a listener with a callback to be called when the listener is fulfilled. *(users, bots; not a coroutine)*
- `remove_listener(listener)` — Removes a listener from the `Client.listeners` dictionary. *(users, bots; not a coroutine; `remove_listerner.py`)*
- `stop_listener(listener)` — Stops a listener, calling stopped_handler if applicable or raising ListenerStopped if throw_exceptions is True. *(users, bots)*
- `stop_listening(optional: listener_type, chat_id, user_id, message_id, inline_message_id)` — Stops all listeners that match the given identifier pattern. *(users, bots)*
- `wait_for_callback_query(chat_id; optional: filters, timeout)` — Wait for callback query. *(bots)*
- `wait_for_message(chat_id; optional: filters, timeout)` — Wait for message. *(users, bots)*

### stickers (3)

- `add_sticker_to_set(set_short_name, sticker; optional: user_id, emoji)` — Add a sticker to stickerset. *(users, bots)*
- `create_sticker_set(title, short_name, sticker; optional: user_id, emoji, masks)` — Create a new stickerset. *(users, bots)*
- `get_sticker_set(set_short_name)` — Get info about a stickerset. *(users, bots)*

### users (30)

- `add_web_browser_settings_exception(url, open_external_browser)` — Add a special handling for opening a website. *(users)*
- `block_user(user_id)` — Block a user. *(users)*
- `change_web_browser_settings(open_external_browser, display_close_button)` — Change the settings of the web browser used to open links. *(users)*
- `delete_profile_photos(photo_ids)` — Delete your own profile photos. *(users)*
- `delete_stories(story_ids; optional: chat_id)` — Delete one or more story by using story identifiers. *(users)*
- `edit_story(story_id; optional: chat_id, privacy, allowed_users, denied_users, animation, photo, video, caption, parse_mode, caption_entities, media_areas, music)` — Edit story. *(users)*
- `export_story_link(chat_id, story_id)` — Get one story link from an user by using story identifiers. *(users)*
- `forward_story(from_chat_id, from_story_id; optional: chat_id, privacy, allowed_users, denied_users, pinned, protect_content, caption, parse_mode, caption_entities, period)` — Forward a story. *(users)*
- `get_all_stories()` — Get all active stories. *(users)*
- `get_chat_photos(chat_id; optional: limit)` — Get a chat or a user profile photos sequentially. *(users, bots)*
- `get_chat_photos_count(chat_id)` — Get the total count of photos for a chat. *(users, bots)*
- `get_common_chats(user_id)` — Get the common chats you have with a user. *(users)*
- `get_default_emoji_statuses()` — Get the default emoji statuses. *(users, bots)*
- `get_me()` — Get your own user identity. *(users, bots)*
- `get_peer_stories(chat_id)` — Get all active stories from an user/channel by using user identifiers. *(users)*
- `get_stories(chat_id, story_ids)` — Get one or more story from an user by using story identifiers. *(users)*
- `get_stories_history(optional: chat_id, limit, offset_id)` — Get stories history. *(users)*
- `get_story_album_stories(chat_id, story_album_id; optional: limit, offset)` — Get the stories added to a story album. *(users)*
- `get_users(user_ids)` — Get information about a user. *(users, bots)*
- `get_web_browser_settings()` — Get the settings of the web browser used to open links. *(users)*
- `remove_all_web_browser_settings_exceptions()` — Remove the special handling for opening of all websites. *(users)*
- `remove_web_browser_settings_exception(url)` — Remove the special handling for opening a website. *(users)*
- `send_story(optional: chat_id, privacy, allowed_users, denied_users, photo, video, file_name, pinned, protect_content, caption, parse_mode, caption_entities, period, forward_from_chat_id, … +3)` — Send new story. *(users)*
- `set_emoji_status(optional: emoji_status)` — Set the emoji status. *(users)*
- `set_profile_photo(optional: photo, emoji, emoji_background, video)` — Set a new profile photo or video (H.264/MPEG-4 AVC video, max 5 seconds). *(users, bots)*
- `set_username(username)` — Set your own username. *(users)*
- `unblock_user(user_id)` — Unblock a user. *(users)*
- `update_birthday(day, month; optional: year)` — Update your birthday details. *(users)*
- `update_personal_chat(chat_id)` — Update your birthday details. *(users)*
- `update_profile(optional: first_name, last_name, bio)` — Update your profile details such as first name, last name and bio. *(users)*

### utilities (11)

- `add_handler(handler; optional: group)` — Register an update handler. *(not a coroutine)*
- `export_session_string()` — Export the current authorized session as a serialized string.
- `ping()` — Measure the round-trip time (RTT) to the Telegram server.
- `remove_error_handler(optional: exception)` — Remove a previously registered error handler using exception classes. *(not a coroutine)*
- `remove_handler(handler; optional: group)` — Remove a previously-registered update handler. *(not a coroutine)*
- `restart(optional: block)` — Restart the Client.
- `run(optional: coroutine)` — Start the client, idle the main script and finally stop the client. *(not a coroutine)*
- `run_sync(func, *args; optional: **kwargs)` — Runs the given sync function (optionally with arguments) on a separate thread.
- `start()` — Start the client.
- `stop(optional: block)` — Stop the Client.
- `stop_transmission()` — Stop downloading or uploading a file. *(not a coroutine)*

### wallet (38)

- `answer_ton_connect_request(session_id, message_id, body; optional: trace_id)` — Send the answer to a request received through TON Connect. *(users)*
- `check_wallet_bot_balance()` — Check the balance of the current user in the bot https://t.me/walt. *(users)*
- `claim_ton_connect_request(session_id, message_id, dapp_request_id; optional: is_rejected, challenge_answer)` — Claim a request received through TON Connect for processing on this device, or reject it. *(users)*
- `create_on_ramp_payment_session(provider_id, cryptocurrency, address; optional: payment_method_name, base_currency, cryptocurrency_amount, base_currency_amount, memo, theme, success_return_url, fail_return_url)` — Create a session for an on-ramp payment, to buy cryptocurrency for a fiat currency. *(users)*
- `create_ton_connect_session(dapp_client_id, manifest_url)` — Create a TON Connect session between the TON wallet of the current user and a dApp. *(users)*
- `create_user_ton_wallet(user_id)` — Create a TON wallet for a regular user. *(users)*
- `delete_ton_wallet(optional: password)` — Delete the current TON wallet of the current user and create a new one. *(users)*
- `disable_ton_wallet_backup(optional: password)` — Disable the backup of the key of the TON wallet of the current user. *(users)*
- `disable_ton_wallet_backup_with_proof(proof)` — Disable the backup of the key of the TON wallet of the current user, proving the ownership of the wallet instead of using the 2-step verification password. *(users)*
- `disconnect_ton_connect_session(session_id; optional: body)` — Close a TON Connect session. *(users)*
- `enable_ton_wallet_backup(encrypted_parts; optional: proof)` — Enable the backup of the key of the TON wallet of the current user. *(users)*
- `export_ton_wallet_secret_phrase(optional: password)` — Start the export of the secret phrase of the TON wallet of the current user. *(users)*
- `get_address_ton_wallet(address)` — Get information about the TON wallet with the given address. *(users)*
- `get_on_ramp_payment_availability(provider_id, cryptocurrency; optional: base_currency)` — Get the on-ramp payment methods available to the current user, based on the user's location. *(users)*
- `get_on_ramp_payment_limits(provider_id, cryptocurrency, base_currency; optional: payment_method_name)` — Get the limits of an on-ramp payment. *(users)*
- `get_on_ramp_payment_quote(provider_id, cryptocurrency, base_currency; optional: cryptocurrency_amount, base_currency_amount, payment_method_name)` — Get a quote for an on-ramp payment. *(users)*
- `get_on_ramp_provider_base_currencies(provider_id, cryptocurrency)` — Get the fiat currencies supported by an on-ramp provider to buy a cryptocurrency. *(users)*
- `get_on_ramp_providers(optional: cryptocurrency)` — Get the available on-ramp providers, which sell cryptocurrency for fiat currencies. *(users)*
- `get_ton_center_streaming_api_url()` — Get an HTTP URL of the TON Center Streaming API. *(users)*
- `get_ton_connect_dapp_pending_requests(dapp_client_id)` — Get the pending requests received from a dApp through TON Connect. *(users)*
- `get_ton_connect_session_next_event_id(session_id)` — Get the identifier of the next event of a TON Connect session. *(users)*
- `get_ton_connect_session_pending_requests(session_id)` — Get the pending requests of a TON Connect session. *(users)*
- `get_ton_connect_sessions()` — Get the active TON Connect sessions of the TON wallet of the current user. *(users)*
- `get_ton_wallet_backup_holder_dcs()` — Get the data centers which hold the parts of the backup of the secret phrase of the TON wallet. *(users)*
- `get_ton_wallet_encrypted_secret_phrase_part(token, public_key, dc_id)` — Fetch an encrypted part of the secret phrase of the TON wallet of the current user from a data center. *(users)*
- `get_ton_wallet_gasless_transfers_info()` — Get the state of the gasless transfers of the current user, whose fee is paid by Telegram. *(users)*
- `get_ton_wallet_nfts(optional: limit, offset)` — Get the NFTs stored on the TON wallet of the current user. *(users)*
- `get_ton_wallet_ownership_proof_challenge()` — Get a challenge which can be used to prove the ownership of a TON wallet. *(users)*
- `get_ton_wallet_state()` — Get the state of the TON wallet of the current user. *(users)*
- `get_ton_wallet_transaction(transaction_id)` — Get transactions of the TON wallet of the current user by their identifiers. *(users)*
- `get_ton_wallet_transaction_by_msg_hash(msg_hash)` — Get transactions of the TON wallet of the current user by the hashes of their messages. *(users)*
- `get_ton_wallet_transactions(optional: limit, offset, is_inbound, is_outbound)` — Get the transactions of the TON wallet of the current user. *(users)*
- `get_user_ton_wallet_addresses(user_ids)` — Get the addresses of the TON wallets of users. *(users)*
- `replace_ton_wallet(proof; optional: anchor_public_key, password)` — Replace the current TON wallet of the current user with another existing v6r2 wallet, or change the private key of the current TON wallet. *(users)*
- `send_ton_center_api_request(endpoint; optional: query, payload)` — Send a request to the TON Center API through Telegram. *(users)*
- `send_ton_connect_session_connect_result(session_id, challenge_answer, body; optional: is_error, trace_id)` — Accept or reject a pending TON Connect session. *(users)*
- `send_ton_wallet_transfer(regular_transfer_data; optional: gasless_transfer_data, user_id)` — Send cryptocurrency from the TON wallet of the current user. *(users)*
- `set_ton_connect_session_wallet_client_id(session_id, wallet_client_id)` — Set the key of the application of the current user for a pending TON Connect session. *(users)*


## Appendix B: decorators, filters, bound methods, enums, types, errors

### Decorators and handler classes

| Decorator | Handler class | File |
|---|---|---|
| `@app.on_bot_business_connect(filters=None, group=0)` | `BotBusinessConnectHandler` | `pyrogram/methods/decorators/on_bot_business_connect.py` |
| `@app.on_bot_business_message(filters=None, group=0)` | `BotBusinessMessageHandler` | `pyrogram/methods/decorators/on_bot_business_message.py` |
| `@app.on_business_connected_bot(filters=None, group=0)` | `BusinessConnectedBotHandler` | `pyrogram/methods/decorators/on_business_connected_bot.py` |
| `@app.on_callback_query(filters=None, group=0)` | `CallbackQueryHandler` | `pyrogram/methods/decorators/on_callback_query.py` |
| `@app.on_chat_join_request(filters=None, group=0)` | `ChatJoinRequestHandler` | `pyrogram/methods/decorators/on_chat_join_request.py` |
| `@app.on_chat_join_result(filters=None, group=0)` | `ChatJoinResultHandler` | `pyrogram/methods/decorators/on_chat_join_result.py` |
| `@app.on_chat_member_updated(filters=None, group=0)` | `ChatMemberUpdatedHandler` | `pyrogram/methods/decorators/on_chat_member_updated.py` |
| `@app.on_chosen_inline_result(filters=None, group=0)` | `ChosenInlineResultHandler` | `pyrogram/methods/decorators/on_chosen_inline_result.py` |
| `@app.on_deleted_bot_business_messages(filters=None, group=0)` | `DeletedBotBusinessMessagesHandler` | `pyrogram/methods/decorators/on_deleted_bot_business_messages.py` |
| `@app.on_deleted_ephemeral_messages(filters=None, group=0)` | `DeletedEphemeralMessagesHandler` | `pyrogram/methods/decorators/on_deleted_ephemeral_messages.py` |
| `@app.on_deleted_messages(filters=None, group=0)` | `DeletedMessagesHandler` | `pyrogram/methods/decorators/on_deleted_messages.py` |
| `@app.on_disconnect()` | `DisconnectHandler` | `pyrogram/methods/decorators/on_disconnect.py` |
| `@app.on_edited_bot_business_message(filters=None, group=0)` | `EditedBotBusinessMessageHandler` | `pyrogram/methods/decorators/on_edited_bot_business_message.py` |
| `@app.on_edited_ephemeral_message(filters=None, group=0)` | `EditedEphemeralMessageHandler` | `pyrogram/methods/decorators/on_edited_ephemeral_message.py` |
| `@app.on_edited_message(filters=None, group=0)` | `EditedMessageHandler` | `pyrogram/methods/decorators/on_edited_message.py` |
| `@app.on_ephemeral_message(filters=None, group=0)` | `EphemeralMessageHandler` | `pyrogram/methods/decorators/on_ephemeral_message.py` |
| `@app.on_error(errors=None, group=0)` | `ErrorHandler` | `pyrogram/methods/decorators/on_error.py` |
| `@app.on_guest_query(filters=None, group=0)` | `GuestQueryHandler` | `pyrogram/methods/decorators/on_guest_query.py` |
| `@app.on_inline_query(filters=None, group=0)` | `InlineQueryHandler` | `pyrogram/methods/decorators/on_inline_query.py` |
| `@app.on_managed_bot_updated(filters=None, group=0)` | `ManagedBotUpdatedHandler` | `pyrogram/methods/decorators/on_managed_bot_updated.py` |
| `@app.on_message(filters=None, group=0)` | `MessageHandler` | `pyrogram/methods/decorators/on_message.py` |
| `@app.on_message_draft_stopped(filters=None, group=0)` | `MessageDraftStoppedHandler` | `pyrogram/methods/decorators/on_message_draft_stopped.py` |
| `@app.on_message_reaction_count_updated(filters=None, group=0)` | `MessageReactionCountUpdatedHandler` | `pyrogram/methods/decorators/on_message_reaction_count_updated.py` |
| `@app.on_message_reaction_updated(filters=None, group=0)` | `MessageReactionUpdatedHandler` | `pyrogram/methods/decorators/on_message_reaction_updated.py` |
| `@app.on_pending_message(filters=None, group=0)` | `PendingMessageHandler` | `pyrogram/methods/decorators/on_pending_message.py` |
| `@app.on_poll(filters=None, group=0)` | `PollHandler` | `pyrogram/methods/decorators/on_poll.py` |
| `@app.on_pre_checkout_query(filters=None, group=0)` | `PreCheckoutQueryHandler` | `pyrogram/methods/decorators/on_pre_checkout_query.py` |
| `@app.on_purchased_paid_media(filters=None, group=0)` | `PurchasedPaidMediaHandler` | `pyrogram/methods/decorators/on_purchased_paid_media.py` |
| `@app.on_raw_update(filters=None, group=0)` | `RawUpdateHandler` | `pyrogram/methods/decorators/on_raw_update.py` |
| `@app.on_shipping_query(filters=None, group=0)` | `ShippingQueryHandler` | `pyrogram/methods/decorators/on_shipping_query.py` |
| `@app.on_story(filters=None, group=0)` | `StoryHandler` | `pyrogram/methods/decorators/on_story.py` |
| `@app.on_ton_connect_pending_disconnect(filters=None, group=0)` | `TonConnectPendingDisconnectHandler` | `pyrogram/methods/decorators/on_ton_connect_pending_disconnect.py` |
| `@app.on_ton_connect_session(filters=None, group=0)` | `TonConnectSessionHandler` | `pyrogram/methods/decorators/on_ton_connect_session.py` |
| `@app.on_ton_wallet_gasless_transfers_info(filters=None, group=0)` | `TonWalletGaslessTransfersInfoHandler` | `pyrogram/methods/decorators/on_ton_wallet_gasless_transfers_info.py` |
| `@app.on_ton_wallet_state(filters=None, group=0)` | `TonWalletStateHandler` | `pyrogram/methods/decorators/on_ton_wallet_state.py` |
| `@app.on_user_status(filters=None, group=0)` | `UserStatusHandler` | `pyrogram/methods/decorators/on_user_status.py` |
| `@app.on_user_subscription_updated(filters=None, group=0)` | `UserSubscriptionUpdatedHandler` | `pyrogram/methods/decorators/on_user_subscription_updated.py` |

Handler classes: `BotBusinessConnectHandler`, `BotBusinessMessageHandler`, `BusinessConnectedBotHandler`, `CallbackQueryHandler`, `ChatJoinRequestHandler`, `ChatJoinResultHandler`, `ChatMemberUpdatedHandler`, `ChosenInlineResultHandler`, `ConversationHandler`, `DeletedBotBusinessMessagesHandler`, `DeletedEphemeralMessagesHandler`, `DeletedMessagesHandler`, `DisconnectHandler`, `EditedBotBusinessMessageHandler`, `EditedEphemeralMessageHandler`, `EditedMessageHandler`, `EphemeralMessageHandler`, `ErrorHandler`, `GuestQueryHandler`, `InlineQueryHandler`, `ManagedBotUpdatedHandler`, `MessageDraftStoppedHandler`, `MessageHandler`, `MessageReactionCountUpdatedHandler`, `MessageReactionUpdatedHandler`, `PendingMessageHandler`, `PollHandler`, `PreCheckoutQueryHandler`, `PurchasedPaidMediaHandler`, `RawUpdateHandler`, `ShippingQueryHandler`, `StoryHandler`, `TonConnectPendingDisconnectHandler`, `TonConnectSessionHandler`, `TonWalletGaslessTransfersInfoHandler`, `TonWalletStateHandler`, `UserStatusHandler`, `UserSubscriptionUpdatedHandler`

### Filters (`pyrogram.filters`)

- `filters.all` — Filter all messages.
- `filters.me` — Filter messages generated by you yourself.
- `filters.bot` — Filter messages coming from bots.
- `filters.incoming` — Filter incoming messages.
- `filters.outgoing` — Filter outgoing messages.
- `filters.text` — Filter text messages.
- `filters.reply` — Filter messages that are replies to other messages.
- `filters.react` — Filter reactions.
- `filters.forwarded` — Filter messages that are forwarded.
- `filters.caption` — Filter media messages that contain captions.
- `filters.audio` — Filter messages that contain `types.Audio` objects.
- `filters.document` — Filter messages that contain `types.Document` objects.
- `filters.photo` — Filter messages that contain `types.Photo` objects.
- `filters.sticker` — Filter messages that contain `types.Sticker` objects.
- `filters.animation` — Filter messages that contain `types.Animation` objects.
- `filters.game` — Filter messages that contain `types.Game` objects.
- `filters.giveaway` — Filter messages that contain `types.Giveaway` objects.
- `filters.giveaway_result` — Filter messages that contain `types.GiveawayWinners` or `types.GiveawayCompleted` objects.
- `filters.gift_code` — Filter messages that contain `types.GiftCode` objects.
- `filters.star_gift` — Filter messages that contain `types.Gift` objects.
- `filters.video` — Filter messages that contain `types.Video` objects.
- `filters.media_group` — Filter messages containing photos or videos being part of an album.
- `filters.voice` — Filter messages that contain `types.Voice` note objects.
- `filters.video_note` — Filter messages that contain `types.VideoNote` objects.
- `filters.contact` — Filter messages that contain `types.Contact` objects.
- `filters.location` — Filter messages that contain `types.Location` objects.
- `filters.venue` — Filter messages that contain `types.Venue` objects.
- `filters.web_page` — Filter messages sent with a webpage preview.
- `filters.poll` — Filter messages that contain `types.Poll` objects.
- `filters.dice` — Filter messages that contain `types.Dice` objects.
- `filters.media_spoiler` — Filter media messages that contain a spoiler.
- `filters.private` — Filter messages sent in private chats.
- `filters.group` — Filter messages sent in group or supergroup chats.
- `filters.channel` — Filter messages sent in channels.
- `filters.new_chat_members` — Filter service messages for new chat members.
- `filters.left_chat_member` — Filter service messages for members that left the chat.
- `filters.new_chat_title` — Filter service messages for new chat titles.
- `filters.new_chat_photo` — Filter service messages for new chat photos.
- `filters.delete_chat_photo` — Filter service messages for deleted photos.
- `filters.group_chat_created` — Filter service messages for group chat creations.
- `filters.supergroup_chat_created` — Filter service messages for supergroup chat creations.
- `filters.channel_chat_created` — Filter service messages for channel chat creations.
- `filters.migrate_to_chat_id` — Filter service messages that contain migrate_to_chat_id.
- `filters.migrate_from_chat_id` — Filter service messages that contain migrate_from_chat_id.
- `filters.pinned_message` — Filter service messages for pinned messages.
- `filters.game_high_score` — Filter service messages for game high scores.
- `filters.reply_keyboard` — Filter messages containing reply keyboard markups.
- `filters.inline_keyboard` — Filter messages containing inline keyboard markups.
- `filters.mentioned` — Filter messages containing mentions.
- `filters.via_bot` — Filter messages sent via inline bots.
- `filters.video_chat_started` — Filter messages for started video chats.
- `filters.video_chat_ended` — Filter messages for ended video chats.
- `filters.video_chat_members_invited` — Filter messages for voice chat invited members.
- `filters.successful_payment` — Filter messages for successful payments.
- `filters.service` — Filter service messages.
- `filters.media` — Filter media messages.
- `filters.scheduled` — Filter messages that have been scheduled (not yet sent).
- `filters.from_scheduled` — Filter new automatically sent messages that were previously scheduled.
- `filters.linked_channel` — Filter messages that are automatically forwarded from the linked channel to the group chat.
- `filters.forum_topic_closed` — Filter service message for closed forum topics.
- `filters.forum_topic_created` — Filter service message for created forum topics.
- `filters.forum_topic_edited` — Filter service message for edited forum topics.
- `filters.forum_topic_reopened` — Filter service message for reopened forum topics.
- `filters.general_forum_topic_hidden` — Filter service message for hidden general forum topics.
- `filters.general_forum_topic_unhidden` — Filter service message for unhidden general forum topics.
- `filters.command(commands, prefixes, case_sensitive)` *(factory)* — Filter commands, i.e.: text messages starting with "/" or any other custom prefix.
- `filters.regex(pattern, flags)` *(factory)* — Filter updates that match a given regular expression pattern.
- `filters.user(users)` *(factory)* — Filter messages coming from one or more users.
- `filters.chat(chats)` *(factory)* — Filter messages coming from one or more chats.
- `filters.topic(topics)` *(factory)* — Filter messages coming from one or more topics.

### Bound methods of common types

- `Message` (`pyrogram/types/messages_and_media/message.py`): `wait_for_click`, `get_media_group`, `reply_text`, `reply_animation`, `reply_audio`, `reply_cached_media`, `reply_chat_action`, `reply_contact`, `reply_document`, `reply_game`, `reply_inline_bot_result`, `reply_location`, `reply_media_group`, `reply_photo`, `reply_poll`, `reply_sticker`, `reply_venue`, `reply_video`, `reply_video_note`, `reply_voice`, `reply_web_page`, `edit_text`, `edit_caption`, `edit_media`, `edit_reply_markup`, `forward`, `copy`, `delete`, `click`, `react`, `retract_vote`, `download`, `vote`, `pin`, `unpin`, `ask`, `transcribe`, `translate`, `summarize`
- `CallbackQuery` (`pyrogram/types/bots_and_keyboards/callback_query.py`): `answer`, `edit_message_text`, `edit_message_caption`, `edit_message_media`, `edit_message_reply_markup`, `reply_with_ephemeral_message`
- `InlineQuery` (`pyrogram/types/inline_mode/inline_query.py`): `answer`
- `Chat` (`pyrogram/types/user_and_chats/chat.py`): `archive`, `unarchive`, `set_title`, `set_description`, `set_photo`, `ban_member`, `unban_member`, `restrict_member`, `promote_member`, `join`, `leave`, `export_invite_link`, `get_member`, `add_members`, `mark_unread`, `set_protected_content`, `unpin_all_messages`
- `User` (`pyrogram/types/user_and_chats/user.py`): `archive`, `unarchive`
- `ChatJoinRequest` (`pyrogram/types/user_and_chats/chat_join_request.py`): `approve`, `decline`
- `Story` (`pyrogram/types/messages_and_media/story.py`): `reply_text`, `reply_animation`, `reply_audio`, `reply_cached_media`, `reply_media_group`, `reply_photo`, `reply_sticker`, `reply_video`, `reply_video_note`, `reply_voice`, `delete`, `edit_animation`, `edit`, `edit_caption`, `edit_photo`, `edit_privacy`, `edit_video`, `export_link`, `forward`, `download`

Aliases: `Message.reply` = `reply_text`, `Message.edit` = `edit_text`, `Story.reply` = `reply_text`.

### Enums (`pyrogram.enums`)

- `enums.BusinessSchedule`: ALWAYS, OUTSIDE_WORK_HOURS, CUSTOM
- `enums.ButtonStyle`: DEFAULT, PRIMARY, DANGER, SUCCESS, LINK
- `enums.ChatAction`: TYPING, UPLOAD_PHOTO, RECORD_VIDEO, UPLOAD_VIDEO, RECORD_AUDIO, UPLOAD_AUDIO, UPLOAD_DOCUMENT, FIND_LOCATION, RECORD_VIDEO_NOTE, UPLOAD_VIDEO_NOTE, PLAYING, CHOOSE_CONTACT, SPEAKING, IMPORT_HISTORY, CHOOSE_STICKER, CANCEL, TRIGGER_EMOJI_ANIMATION, WATCH_EMOJI_ANIMATION
- `enums.ChatEventAction`: DESCRIPTION_CHANGED, HISTORY_TTL_CHANGED, LINKED_CHAT_CHANGED, PHOTO_CHANGED, TITLE_CHANGED, USERNAME_CHANGED, CHAT_PERMISSIONS_CHANGED, MESSAGE_DELETED, MESSAGE_EDITED, INVITE_LINK_EDITED, INVITE_LINK_REVOKED, INVITE_LINK_DELETED, MEMBER_INVITED, MEMBER_JOINED, MEMBER_LEFT, ADMINISTRATOR_PRIVILEGES_CHANGED, MEMBER_PERMISSIONS_CHANGED, POLL_STOPPED, INVITES_ENABLED, HISTORY_HIDDEN, SIGNATURES_ENABLED, SLOW_MODE_CHANGED, MESSAGE_PINNED, MESSAGE_UNPINNED, CREATED_FORUM_TOPIC, EDITED_FORUM_TOPIC, DELETED_FORUM_TOPIC, MEMBER_TAG_CHANGED, UNKNOWN
- `enums.ChatJoinResultType`: APPROVED, DECLINED, QUEUED, WEB_APP
- `enums.ChatJoinType`: BY_ADD, BY_LINK, BY_REQUEST, BY_COMMUNITY
- `enums.ChatMemberStatus`: OWNER, ADMINISTRATOR, MEMBER, RESTRICTED, LEFT, BANNED
- `enums.ChatMembersFilter`: SEARCH, BANNED, RESTRICTED, BOTS, RECENT, ADMINISTRATORS
- `enums.ChatType`: PRIVATE, BOT, GROUP, SUPERGROUP, CHANNEL, FORUM, MONOFORUM, COMMUNITY
- `enums.ClientPlatform`: ANDROID, IOS, WP, BB, DESKTOP, WEB, UBP, OTHER
- `enums.CraftGiftResultType`: SUCCESS, TOO_EARLY, INVALID_GIFT, FAIL
- `enums.FolderColor`: NO_COLOR, RED, ORANGE, VIOLET, GREEN, CYAN, BLUE, PINK
- `enums.GiftAttributeRarityType`: PER_MILLE, UNCOMMON, RARE, EPIC, LEGENDARY
- `enums.GiftAttributeType`: MODEL, SYMBOL, BACKDROP, ORIGINAL_DETAILS
- `enums.GiftForResaleOrder`: PRICE, CHANGE_DATE, NUMBER
- `enums.ListenerTypes`: MESSAGE, CALLBACK_QUERY
- `enums.MessageEntityType`: MENTION, HASHTAG, CASHTAG, BOT_COMMAND, URL, EMAIL, PHONE_NUMBER, BOLD, ITALIC, UNDERLINE, STRIKETHROUGH, SPOILER, CODE, PRE, BLOCKQUOTE, EXPANDABLE_BLOCKQUOTE, TEXT_LINK, TEXT_MENTION, BANK_CARD, CUSTOM_EMOJI, UNKNOWN, FORMATTED_DATE, TON_ADDRESS, DIFF_INSERT, DIFF_REPLACE, DIFF_DELETE
- `enums.MessageMediaType`: AUDIO, DOCUMENT, PHOTO, STICKER, VIDEO, ANIMATION, VOICE, VIDEO_NOTE, CONTACT, LOCATION, VENUE, POLL, WEB_PAGE_PREVIEW, DICE, GAME, GIVEAWAY, GIVEAWAY_RESULT, STORY, INVOICE, PAID_MEDIA, TODO
- `enums.MessageOriginType`: CHANNEL, CHAT, HIDDEN_USER, IMPORT, USER
- `enums.MessageServiceType`: NEW_CHAT_MEMBERS, LEFT_CHAT_MEMBERS, NEW_CHAT_TITLE, NEW_CHAT_PHOTO, DELETE_CHAT_PHOTO, GROUP_CHAT_CREATED, CHANNEL_CHAT_CREATED, MIGRATE_TO_CHAT_ID, MIGRATE_FROM_CHAT_ID, PINNED_MESSAGE, GAME_HIGH_SCORE, CHAT_SHARED, FORUM_TOPIC_CREATED, FORUM_TOPIC_CLOSED, FORUM_TOPIC_REOPENED, FORUM_TOPIC_EDITED, GENERAL_TOPIC_HIDDEN, GENERAL_TOPIC_UNHIDDEN, VIDEO_CHAT_STARTED, VIDEO_CHAT_ENDED, VIDEO_CHAT_SCHEDULED, VIDEO_CHAT_MEMBERS_INVITED, WEB_APP_DATA, GIFTED_PREMIUM, GIVEAWAY_LAUNCHED, GIVEAWAY_RESULT, BOOST_APPLY, SUCCESSFUL_PAYMENT, PAYMENT_REFUNDED, BOT_ALLOWED, CHAT_THEME_UPDATED, CHAT_WALLPAPER_UPDATED, CONTACT_REGISTERED, GIFT_CODE, GIFT, SCREENSHOT_TAKEN, PAID_MESSAGE_PRICE_CHANGED, TODO_TASKS_ADDED, TODO_TASKS_COMPLETION, POLL_OPTION_ADDED, … +10
- `enums.MessagesFilter`: EMPTY, PHOTO, VIDEO, PHOTO_VIDEO, DOCUMENT, URL, ANIMATION, VOICE_NOTE, VIDEO_NOTE, AUDIO_VIDEO_NOTE, AUDIO, CHAT_PHOTO, PHONE_CALL, MENTION, LOCATION, CONTACT, PINNED, POLL
- `enums.NextCodeType`: CALL, FLASH_CALL, MISSED_CALL, SMS, FRAGMENT_SMS
- `enums.PageBlockHorizontalAlignment`: LEFT, CENTER, RIGHT
- `enums.PageBlockVerticalAlignment`: TOP, MIDDLE, BOTTOM
- `enums.ParseMode`: DEFAULT, MARKDOWN, HTML, DISABLED
- `enums.PollType`: QUIZ, REGULAR
- `enums.ProfileColor`: RED, ORANGE, VIOLET, GREEN, CYAN, BLUE, PINK, GRAY, RED_LIGHT_RED, ORANGE_LIGHT_ORANGE, VIOLET_LIGHT_VIOLET, GREEN_LIGHT_GREEN, CYAN_LIGHT_CYAN, BLUE_LIGHT_BLUE, PINK_LIGHT_PINK, GRAY_LIGHT_GRAY
- `enums.ReactionType`: EMOJI, CUSTOM_EMOJI, PAID
- `enums.ReplyColor`: RED, ORANGE, VIOLET, GREEN, CYAN, BLUE, PINK, RED_DARK_RED, ORANGE_DARK_ORANGE, VIOLET_DARK_VIOLET, GREEN_DARK_GREEN, CYAN_DARK_CYAN, BLUE_DARK_BLUE, PINK_DARK_PINK, BLUE_WHITE_RED, ORANGE_WHITE_GREEN, GREEN_WHITE_RED, CYAN_WHITE_GREEN, CYAN_YELLOW_PINK, VIOLET_YELLOW_ORANGE, BLUE_WHITE_ORANGE, DYNAMIC
- `enums.ReportResultType`: REPORTED, OPTION_REQUIRED, TEXT_REQUIRED
- `enums.SentCodeType`: APP, CALL, FLASH_CALL, MISSED_CALL, SMS, FRAGMENT_SMS, EMAIL_CODE, FIREBASE_SMS, SMS_WORD, SMS_PHRASE, SETUP_EMAIL_REQUIRED
- `enums.StoriesPrivacyRules`: PUBLIC, CLOSE_FRIENDS, CONTACTS, PRIVATE, NO_CONTACTS
- `enums.StoryPrivacy`: PUBLIC, CLOSE_FRIENDS, CONTACTS, PRIVATE, NO_CONTACTS
- `enums.TonConnectSessionState`: PENDING, READY, CLOSING, CLOSED
- `enums.TonWalletTransactionType`: TRANSFER, NFT_TRANSFER, ON_RAMP_DEPOSIT, KEY_CHANGE
- `enums.UserStatus`: ONLINE, OFFLINE, RECENTLY, LAST_WEEK, LAST_MONTH, LONG_AGO

### Types (`pyrogram.types`) by category

- **authorization** (7): `ActiveSession`, `ActiveSessions`, `LoginToken`, `SentCode`, `TermsOfService`, `OauthLinkInfo`, `FirebasePnvIntent`
- **bots_and_keyboards** (47): `BotAllowed`, `BotApp`, `BotBusinessConnection`, `CallbackGame`, `CallbackQuery`, `CollectibleItemInfo`, `ForceReply`, `GameHighScore`, `InlineKeyboardButton`, `InlineKeyboardButtonBuy`, `InlineKeyboardMarkup`, `KeyboardButton`, `ReplyKeyboardMarkup`, `ReplyKeyboardRemove`, `RequestPeerTypeChannel`, `RequestPeerTypeChat`, `RequestPeerTypeUser`, `RequestedChats`, `RequestedChat`, `RequestedUser`, `LoginUrl`, `BotCommand`, `BotCommandScope`, `BotCommandScopeAllChatAdministrators`, `BotCommandScopeAllGroupChats`, `BotCommandScopeAllPrivateChats`, `BotCommandScopeChat`, `BotCommandScopeChatAdministrators`, `BotCommandScopeChatMember`, `BotCommandScopeDefault`, `BotInfo`, `WebAppInfo`, `MenuButton`, `MenuButtonCommands`, `MenuButtonWebApp`, `MenuButtonDefault`, `SentWebAppMessage`, `KeyboardButtonPollType`, `RequestPeerTypeManagedBot`, `BotAccessSettings`, `GuestQuery`, `ManagedBotUpdated`, `ManagedBotCreated`, `LoginUrlInfo`, `LoginUrlInfoOpen`, `LoginUrlInfoRequestConfirmation`, `UserSubscriptionUpdated`
- **business** (5): `PreCheckoutQuery`, `ShippingAddress`, `ShippingOption`, `ShippingQuery`, `BusinessConnectedBot`
- **inline_mode** (20): `InlineQuery`, `InlineQueryResult`, `InlineQueryResultArticle`, `InlineQueryResultPhoto`, `InlineQueryResultAnimation`, `InlineQueryResultAudio`, `InlineQueryResultVideo`, `ChosenInlineResult`, `InlineQueryResultContact`, `InlineQueryResultDocument`, `InlineQueryResultVoice`, `InlineQueryResultLocation`, `InlineQueryResultVenue`, `InlineQueryResultCachedPhoto`, `InlineQueryResultCachedAnimation`, `InlineQueryResultCachedSticker`, `InlineQueryResultCachedDocument`, `InlineQueryResultCachedVideo`, `InlineQueryResultCachedVoice`, `InlineQueryResultCachedAudio`
- **input_media** (9): `InputMedia`, `InputMediaAnimation`, `InputMediaAudio`, `InputMediaDocument`, `InputMediaPhoto`, `InputMediaVideo`, `InputPhoneContact`, `InputMediaArea`, `InputMediaAreaChannelPost`
- **input_message_content** (11): `InputMessageContent`, `InputReplyToMessage`, `InputReplyToMonoforum`, `InputReplyToStory`, `InputTextMessageContent`, `InputLocationMessageContent`, `InputVenueMessageContent`, `InputContactMessageContent`, `InputInvoiceMessageContent`, `InputTodoTask`, `InputRichMessageContent`
- **messages_and_media** (84): `AlternativeVideo`, `Animation`, `Audio`, `AvailableEffect`, `ChatTheme`, `ChatWallpaper`, `Contact`, `ContactRegistered`, `Document`, `ExternalReplyInfo`, `Game`, `Giveaway`, `GiveawayLaunched`, `GiveawayResult`, `Location`, `MediaArea`, `MediaAreaChannelPost`, `MediaAreaCoordinates`, `Message`, `MessageEntity`, `MessageOrigin`, `MessageOriginChannel`, `MessageOriginChat`, `MessageOriginHiddenUser`, `MessageOriginImport`, `MessageOriginUser`, `Photo`, `Thumbnail`, `StrippedThumbnail`, `Poll`, `PollOption`, `Sticker`, `StickerSet`, `Venue`, `Video`, `VideoNote`, `Voice`, `WebPage`, `WebPageEmpty`, `WebPagePreview`, `Dice`, `Reaction`, `WebAppData`, `MessageReactions`, `MessageReactionUpdated`, `MessageReactionCountUpdated`, `MessageReactor`, `MessageStory`, `ReadParticipant`, `ScreenshotTaken`, `Story`, `StoryDeleted`, `StorySkipped`, `StoryViews`, `StoryForwardHeader`, `StoriesPrivacyRules`, `ExportedStoryLink`, `Wallpaper`, `WallpaperSettings`, `TranscribedAudio`, `TranslatedText`, `TextQuote`, `TodoList`, `TodoTask`, `TodoTasksAdded`, `TodoTasksCompleted`, `TodoTasksIncompleted`, `StakeDiceState`, `FormattedText`, `TextCompositionStyle`, `TextCompositionStyleExample`, `FixedText`, `StatisticalGraph`, `StatisticalGraphData`, `StatisticalGraphAsync`, `StatisticalGraphError`, `PollVoteStatistics`, `PendingMessage`, `MessageDraftStopped`, `DraftMessage`, `EphemeralMessage`, `WelcomeMessage`, `ReportOption`, `ReportResult`
- **payments** (20): `ExtendedMediaPreview`, `CheckedGiftCode`, `Gift`, `GiftAttribute`, `GiftCode`, `GiftedPremium`, `InputStarsTransaction`, `Invoice`, `LabeledPrice`, `PaidMedia`, `PaidMessagePriceChanged`, `PaymentForm`, `PaymentInfo`, `PaymentRefunded`, `PurchasedPaidMedia`, `StarsStatus`, `StarsTransaction`, `SuccessfulPayment`, `CraftGiftResult`, `CurrencyExchangeRate`
- **pyromod** (2): `Identifier`, `Listener`
- **rich_messages** (107): `RichMessage`, `InlineButton`, `RichText`, `RichTextPlain`, `RichTextBold`, `RichTextItalic`, `RichTextUnderline`, `RichTextStrikethrough`, `RichTextSpoiler`, `RichTextSubscript`, `RichTextSuperscript`, `RichTextMarked`, `RichTextDateTime`, `RichTextMention`, `RichTextHashtag`, `RichTextCashtag`, `RichTextBankCardNumber`, `RichTextTonAddress`, `RichTextBotCommand`, `RichTextFixed`, `RichTextMentionName`, `RichTextUrl`, `RichTextEmailAddress`, `RichTextPhoneNumber`, `RichTextCustomEmoji`, `RichTextIcon`, `RichTextMathematicalExpression`, `RichTextButton`, `RichTextDiff`, `RichTextReference`, `RichTextReferenceLink`, `RichTextAnchor`, `RichTextAnchorLink`, `RichTexts`, `PageBlock`, `PageBlockTitle`, `PageBlockSubtitle`, `PageBlockAuthorDate`, `PageBlockHeader`, `PageBlockSubheader`, `PageBlockSectionHeading`, `PageBlockKicker`, `PageBlockParagraph`, `PageBlockPreformatted`, `PageBlockFooter`, `PageBlockThinking`, `PageBlockDivider`, `PageBlockMathematicalExpression`, `PageBlockAnchor`, `PageBlockList`, `PageBlockBlockQuote`, `PageBlockExpandableBlockQuote`, `PageBlockPullQuote`, `PageBlockAnimation`, `PageBlockAudio`, `PageBlockDocument`, `PageBlockPhoto`, `PageBlockVideo`, `PageBlockVoiceNote`, `PageBlockCover`, `PageBlockEmbedded`, `PageBlockEmbeddedPost`, `PageBlockCollage`, `PageBlockSlideshow`, `PageBlockChatLink`, `PageBlockTable`, `PageBlockDetails`, `PageBlockRelatedArticles`, `PageBlockMap`, `PageBlockButtonRow`, `PageBlockUnsupported`, `PageBlockCaption`, `PageBlockListItem`, `PageBlockTableCell`, `PageBlockRelatedArticle`, `InputRichMessage`, `RichMessageSource`, `RichMessageSourceBlocks`, `RichMessageSourceMarkdown`, `RichMessageSourceHtml`, `InputRichMessageMedia`, `InputPageBlock`, `InputPageBlockSectionHeading`, `InputPageBlockParagraph`, `InputPageBlockPreformatted`, `InputPageBlockFooter`, `InputPageBlockThinking`, `InputPageBlockDivider`, `InputPageBlockMathematicalExpression`, `InputPageBlockAnchor`, `InputPageBlockList`, `InputPageBlockListItem`, `InputPageBlockBlockQuote`, `InputPageBlockExpandableBlockQuote`, `InputPageBlockPullQuote`, `InputPageBlockAnimation`, `InputPageBlockAudio`, `InputPageBlockDocument`, `InputPageBlockPhoto`, `InputPageBlockVideo`, `InputPageBlockVoiceNote`, `InputPageBlockCollage`, `InputPageBlockSlideshow`, `InputPageBlockTable`, `InputPageBlockDetails`, `InputPageBlockMap`, `InputPageBlockButtonRow`
- **user_and_chats** (56): `Birthday`, `BotVerification`, `BusinessInfo`, `BusinessMessage`, `BusinessRecipients`, `BusinessWeeklyOpen`, `BusinessWorkingHours`, `Chat`, `ChatMember`, `ChatPermissions`, `ChatPhoto`, `ChatPreview`, `Dialog`, `Folder`, `User`, `Username`, `Restriction`, `ChatEvent`, `ChatEventFilter`, `ChatInviteLink`, `InviteLinkImporter`, `ChatAdminWithInviteLinks`, `ChatColor`, `ForumTopic`, `ForumTopicCreated`, `ForumTopicClosed`, `ForumTopicDeleted`, `ForumTopicReopened`, `ForumTopicEdited`, `GeneralTopicHidden`, `GeneralTopicUnhidden`, `PeerChannel`, `PeerUser`, `VideoChatStarted`, `VideoChatEnded`, `VideoChatMembersInvited`, `ChatMemberUpdated`, `VideoChatScheduled`, `ChatJoinRequest`, `ChatJoinedByRequest`, `ChatPrivileges`, `ChatJoiner`, `EmojiStatus`, `ExportedFolderLink`, `GroupCallMember`, `ChatReactions`, `ChatJoinResult`, `CommunityChat`, `CommunityChatRequest`, `CommunityMemberChats`, `WebDomainException`, `WebBrowserSettings`, `ChatOwnerLeft`, `ChatOwnerChanged`, `ChatHasProtectedContentToggled`, `ChatHasProtectedContentDisableRequested`
- **wallet** (27): `WebDocument`, `TonNftAttribute`, `TonNft`, `TonWalletState`, `TonWalletTransaction`, `TonWalletGaslessTransfersInfo`, `WalletBotBalance`, `UserTonWalletAddress`, `TonWalletTransferResult`, `TonWalletOwnershipProof`, `TonWalletOwnershipProofChallenge`, `TonWalletBackupHolderDc`, `TonWalletSecretPhraseParts`, `TonConnectManifest`, `TonConnectSession`, `TonConnectRequest`, `TonConnectRequests`, `TonConnectChallenge`, `OnRampProvider`, `OnRampPaymentMethod`, `OnRampPaymentAvailability`, `OnRampPaymentLimits`, `OnRampPaymentQuote`, `OnRampPaymentSession`, `TonCenterStreamingApiUrl`, `TonWalletTransfer`, `TonConnectPendingDisconnect`
