# Quick start

This guide sends a message with a user account, then shows the small changes needed to run a bot. Install Pyrofork and
create an API ID and hash before continuing.

## User account

Get an API ID and hash from [my.telegram.org/apps](https://my.telegram.org/apps). Save the following as `hello.py`,
replacing the example credentials with your own:

```python
import asyncio

from pyrogram import Client

api_id = 12345
api_hash = "0123456789abcdef0123456789abcdef"


async def main():
    async with Client("my_account", api_id=api_id, api_hash=api_hash) as app:
        await app.send_message("me", "Hello from Pyrofork!")


asyncio.run(main())
```

Run it with `python hello.py`. On the first run, Pyrofork asks for your phone number, login code, and, when enabled,
your two-step verification password. It saves the authorized session in `my_account.session` so later runs can reuse it.
Treat that file like a password: anyone with it can use your Telegram account.

## Bot

Create a bot with [@BotFather](https://t.me/BotFather), then pass its token as `bot_token`. Bots still need the API ID
and hash from [my.telegram.org/apps](https://my.telegram.org/apps).

```python
from pyrogram import Client, filters

api_id = 12345
api_hash = "0123456789abcdef0123456789abcdef"
bot_token = "123456:replace-this-with-your-token"

app = Client("my_bot", api_id=api_id, api_hash=api_hash, bot_token=bot_token)


@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply("Hello! The bot is running.")


app.run()
```

For the full authorization flow, see [Authorization](authorization.md). Continue with [project setup](setup.md) or
[calling methods](calling-methods.md).

---

[← Previous](installation.md) · [Next →](setup.md)
