# Error handling

Telegram request failures are raised as exceptions from `pyrogram.errors`. Catch the narrowest exception that lets your
application recover correctly.

## Handle a flood wait

Telegram can ask a client to wait before retrying. `FloodWait.value` is the number of seconds to wait:

```python
import asyncio

from pyrogram.errors import FloodWait


async def send_with_retry(app, chat_id, text):
    try:
        await app.send_message(chat_id, text)
    except FloodWait as error:
        await asyncio.sleep(error.value)
        await app.send_message(chat_id, text)
```

Avoid tight retry loops. For other failures, log enough context to diagnose the request and decide whether to retry.

## Error hierarchy

`RPCError` is the base for Telegram RPC failures. Telegram assigns errors a numeric code and a named condition; Pyrofork
exposes code-specific base classes as well as individual error classes. Browse the [RPC error reference](../api/errors/README.md)
for the complete list.

```python
from pyrogram.errors import BadRequest, FloodWait, RPCError

try:
    await app.send_message(chat_id, text)
except FloodWait as error:
    print(f"Retry after {error.value} seconds")
except BadRequest as error:
    print(f"Telegram rejected the request: {error}")
except RPCError as error:
    print(f"Telegram request failed: {error}")
```

The package also raises library-specific exceptions such as `GuardBotApprovalRequired`; see the
[Pyrogram exception reference](../api/errors/library.md).

---

[← Previous](updates.md) · [Examples →](../examples/README.md)
