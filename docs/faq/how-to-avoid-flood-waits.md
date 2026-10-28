# How to avoid flood waits

Telegram applies request limits that can change based on the method, account, and usage pattern. Reduce unnecessary
requests and handle `FloodWait` by waiting for the number of seconds Telegram provides.

```python
import asyncio

from pyrogram.errors import FloodWait


async def send_with_flood_wait(app, chat_id, text):
    while True:
        try:
            return await app.send_message(chat_id, text)
        except FloodWait as error:
            await asyncio.sleep(error.value)
```

Do not repeatedly retry before the wait expires. See [Error handling](../getting-started/error-handling.md) for other
exception patterns.
