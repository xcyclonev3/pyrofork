# Project setup

## Create a Telegram API application

1. Sign in at [my.telegram.org/apps](https://my.telegram.org/apps).
2. Create an application and copy its `api_id` and `api_hash`.
3. Keep both values private. They identify your Telegram application and are required for user and bot clients.

The API ID is an integer; the API hash is a string. Pass them to `Client` when creating a session:

```python
from pyrogram import Client

app = Client(
    "my_account",
    api_id=12345,
    api_hash="0123456789abcdef0123456789abcdef",
)
```

For a bot, create a token with [@BotFather](https://t.me/BotFather) and also pass `bot_token`. The bot token does not
replace `api_id` or `api_hash`.

## Keep credentials out of source control

Do not commit API credentials, bot tokens, session files, or exported session strings. Read credentials from environment
variables or a secret manager in deployed applications. For local development, a `.env` file can help if it is excluded
from Git.

## Sessions

The first successful authorization creates a session file named after the client, such as `my_account.session`.
Pyrofork reuses that session on later runs, so you normally do not need to keep passing the API credentials after
authorization. Store the session securely and avoid running two processes with the same session file at the same time.

See [Authorization](authorization.md) for the user and bot login flows, or the [storage guide](../guides/storage-engines.md)
for alternative session storage.

---

[← Previous](quick-start.md) · [Next →](authorization.md)
