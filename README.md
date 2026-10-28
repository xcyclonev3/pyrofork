<h1 align="center">Pyrogram</h1>

<p align="center">
    <b>Elegant, modern and asynchronous Telegram MTProto API framework in Python for users and bots</b>
</p>

<p align="center">
    <img alt="Telegram API Layer 230" src="https://img.shields.io/badge/Telegram%20API-Layer%20230-26A5E4?logo=telegram&logoColor=white">
    <img alt="Python 3.10+" src="https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white">
    <a href="COPYING.lesser"><img alt="License: LGPL-3.0-or-later" src="https://img.shields.io/badge/License-LGPL--3.0--or--later-blue"></a>
</p>

<p align="center">
    <a href="#installation">Installation</a>
    •
    <a href="#quick-start">Quick start</a>
    •
    <a href="#whats-new-in-layer-230">What's new</a>
    •
    <a href="https://xcyclonev3.github.io/pyrofork/">Documentation</a>
    •
    <a href="#credits-and-license">Credits</a>
</p>

```python
from pyrogram import Client, filters

app = Client("my_account")


@app.on_message(filters.private)
async def hello(client, message):
    await message.reply("Hello from Pyrogram!")


app.run()
```

**Pyrogram** lets you use the whole Telegram API from Python, either through a user account (a custom client) or as a
bot (an alternative to the Bot API).

This repository keeps Pyrogram up to date with **Telegram API Layer 230**: its schema is identical to the one of the
official apps, and every function the new layers added is available as a high-level method.

## Features

- **Ready**: install it with pip and start building your applications right away.
- **Easy**: makes the Telegram API simple and intuitive, while still allowing advanced usages.
- **Elegant**: low-level details are abstracted and re-presented in a more convenient way.
- **Fast**: boosted by [TgCrypto](https://github.com/pyrogram/tgcrypto), an optional cryptography library written in C.
- **Type-hinted**: methods and types are all type-hinted, enabling excellent editor support.
- **Async**: fully asynchronous, and also usable synchronously if you prefer.
- **Powerful**: full access to the Telegram API, down to every raw function of Layer 230.

## What's new in Layer 230

Compared to release 2.3.69 (Layer 220), this version adds 139 methods, 177 types and 14 update handlers:

| Area | Highlights |
|---|---|
| Communities | `create_community`, `get_joined_communities`, `add_chat_to_community`, join requests and bans |
| Ephemeral and welcome messages | `send_ephemeral_message`, shown to one member only; `add_chat_welcome_message` for newcomers |
| Polls | descriptions, media, revoting, options added by users, shuffled options, hidden results, several correct answers, `get_poll_vote_statistics` |
| Rich messages | `send_rich_message` with page blocks, HTML or Markdown; drafts and translations |
| AI | `compose_text_with_ai`, `fix_text_with_ai`, `summarize_message`, text composition styles |
| Bots | `create_bot` for managed bots, `answer_guest_query`, secondary usernames, prepared and styled keyboard buttons |
| Groups | `set_chat_member_tag`, guard bots for join requests, new administrator rights and member permissions |
| Gifts and games | `craft_gift`, resale filters, `get_currency_exchange_rates`, `send_stake_dice` |
| Media | live photos (`Photo.live_video`), music in stories, `listen_to_audio` |
| TON wallet | `get_ton_wallet_state`, transactions, NFTs, transfers, backups and TON Connect |
| Text | formatted dates (`<tg-time>` and `![...](tg://time?...)`) and TON addresses |

### Upgrading from 2.3.69

No public method, parameter, attribute or enum was removed or renamed. A few behaviours changed:

- `join_chat()` can raise `errors.GuardBotApprovalRequired` for groups protected by a guard bot.
- `GiftedPremium.month_count` holds months again; the new `day_count` holds the exact duration.
- `ChatPermissions()`, a complete restriction, also prevents reactions and tag changes, as in the official apps.
- `send_poll()` ignores the quiz-only arguments for regular polls.
- Raw API: objects that fill only some of the fields sharing one flag are rejected when written, instead of being sent
  corrupted.

## Installation

Pyrogram needs Python 3.10 or newer. Install this version from GitHub:

```bash
pip install -U "git+https://github.com/xcyclonev3/pyrofork.git"
```

The installation generates the raw API from the TL schema, so it takes a little while.

### Optional speedups

```bash
pip install -U tgcrypto-pyrofork uvloop
```

- [TgCrypto](https://github.com/pyrogram/tgcrypto) implements in C the encryption Telegram requires. Pyrogram uses it
  automatically when it is installed, and falls back to a much slower pure-Python implementation otherwise. To build it
  from source instead: `pip install "tgcrypto-pyrofork @ git+https://github.com/xcyclonev3/tgcrypto-pyrofork.git"`.
- [uvloop](https://github.com/MagicStack/uvloop) is a faster event loop: call `uvloop.install()` before creating your
  clients. It is not available on Windows.

## Quick start

Get your `api_id` and `api_hash` at [my.telegram.org](https://my.telegram.org). The first run asks for your phone
number and the login code, then keeps the session in `my_account.session`.

```python
import asyncio

from pyrogram import Client

api_id = 12345
api_hash = "0123456789abcdef0123456789abcdef"


async def main():
    async with Client("my_account", api_id=api_id, api_hash=api_hash) as app:
        me = await app.get_me()
        await app.send_message("me", f"Hello, **{me.first_name}**! This message comes from Pyrogram.")


asyncio.run(main())
```

Bots use a token from [@BotFather](https://t.me/BotFather):

```python
from pyrogram import Client, filters

app = Client("my_bot", api_id=12345, api_hash="0123456789abcdef0123456789abcdef", bot_token="123456:ABC-DEF")


@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply("Hi! I'm running on Pyrogram.")


app.run()
```

A few of the Layer 230 features:

```python
from pyrogram import Client, enums, types

app = Client("my_account")


async def showcase(chat_id):
    # A quiz with a description and shuffled options
    await app.send_poll(
        chat_id, "Which layer does this Pyrogram speak?",
        [types.PollOption("220"), types.PollOption("230")],
        type=enums.PollType.QUIZ, correct_option_id=1,
        description="Only one answer is right", shuffle_options=True,
    )

    # A formatted date
    await app.send_message(
        chat_id, 'The meeting starts <tg-time unix="1767225600" format="wDT">on January 1st</tg-time>.',
        parse_mode=enums.ParseMode.HTML,
    )

    # A rich message built from HTML
    await app.send_rich_message(
        chat_id, types.InputRichMessage(types.RichMessageSourceHtml("<h1>Pyrogram</h1><p>Now on Layer 230.</p>"))
    )
```

## Performance tips

- Install [TgCrypto](#optional-speedups): without it, encryption runs in pure Python.
- Running many accounts in one process? Lower `max_message_cache_size` (10,000 messages per client by default) and
  `workers`, and pass `no_updates=True` to the clients that don't handle updates.
- A Python process uses about one CPU core: spread busy clients over several processes.

## Documentation

Start with the [documentation index](docs/README.md). It links to the installation and quick-start guides, practical
topics, examples, FAQ, release notes, and the complete high-level and raw API references. Every documentation page is a
GitHub-rendered Markdown file; no documentation build is needed to read it.

For architecture and implementation notes, see [AI_GUIDE.md](AI_GUIDE.md).

## Development

```bash
git clone https://github.com/xcyclonev3/pyrofork.git pyrogram
cd pyrogram
make venv     # creates venv/ and installs Pyrogram in editable mode
make api      # generates the raw API and the error classes
venv/bin/pip install -e ".[dev]"
venv/bin/python -m pytest tests
```

The TL schema lives in [`compiler/api/source/main_api.tl`](compiler/api/source/main_api.tl) and the RPC errors in
[`compiler/errors/source`](compiler/errors/source): run `make api` again after changing them.

## Credits and license

- [Pyrogram](https://github.com/pyrogram/pyrogram) by Dan ([@delivrance](https://github.com/delivrance)), the original
  framework, and everyone who has contributed to it since: the copyright holders are named in [NOTICE](NOTICE) and in
  the header of every file.
- [TgCrypto](https://github.com/pyrogram/tgcrypto), the optional cryptography library.

Pyrogram is free software, released under the [GNU Lesser General Public License v3.0](COPYING.lesser) or later. See
also [COPYING](COPYING) and [NOTICE](NOTICE).
