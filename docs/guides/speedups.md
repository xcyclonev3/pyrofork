# Speedups

## Contents

- [TgCrypto](#tgcrypto)
  - [Installation](#installation)
  - [Usage](#usage)
- [uvloop](#uvloop)
  - [Installation](#installation-1)
  - [Usage](#usage-1)



Pyrogram's speed can be boosted up by using TgCrypto and uvloop.

-----

## TgCrypto


[TgCrypto](https://github.com/pyrogram/tgcrypto) is a high-performance, easy-to-install cryptography library specifically written in C for Pyrogram as a Python
extension. It is a replacement for a slower Python-only alternative and implements the cryptographic algorithms Telegram
requires, namely: AES-256-IGE, AES-256-CTR and AES-256-CBC.

### Installation


TgCrypto is optional: install it next to Pyrogram.

```bash
$ pip3 install -U tgcrypto-pyrofork
```

Prebuilt packages exist for Linux and Windows; on other systems, such as macOS or Termux, pip compiles it, which
requires a C compiler. To build it from source instead, for example from a fork:

```bash
$ pip3 install -U "tgcrypto-pyrofork @ git+https://github.com/xcyclonev3/tgcrypto-pyrofork.git"
```

### Usage


Pyrogram automatically makes use of TgCrypto when it is installed. Otherwise, it falls back to a slower Python-only
implementation and logs a warning.

## uvloop


[uvloop](https://github.com/MagicStack/uvloop) is a fast, drop-in replacement of the built-in asyncio event loop. uvloop is implemented in Cython and uses
libuv under the hood. It makes asyncio 2-4x faster.

### Installation


```bash
$ pip3 install -U uvloop
```

### Usage


Call `uvloop.install()` before calling `asyncio.run()` or `app.run()`.

```python
import asyncio
import uvloop

from pyrogram import Client


async def main():
    app = Client("my_account")

    async with app:
        print(await app.get_me())


uvloop.install()
asyncio.run(main())
```

The `uvloop.install()` call also needs to be placed before creating a Client instance.

```python
import uvloop
from pyrogram import Client

uvloop.install()

app = Client("my_account")


@app.on_message()
async def hello(client, message):
    print(await client.get_me())


app.run()
```