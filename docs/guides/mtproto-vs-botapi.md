# MTProto vs. Bot API


Pyrogram is a framework written from the ground up that acts as a fully-fledged Telegram client based on the MTProto
API. This means that Pyrogram is able to execute any official client and bot API action and more. This page will
therefore show you why Pyrogram might be a better choice for your project by comparing the two APIs, but first, let's
make it clear what actually is the MTProto and the Bot API.

-----

## What is the MTProto API?


[MTProto](https://core.telegram.org/mtproto), took alone, is the name of the custom-made, open and encrypted communication protocol created by Telegram
itself --- it's the only protocol used to exchange information between a client and the actual Telegram servers.

The MTProto API on the other hand, is what people for convenience call the main Telegram API in order to distinguish it
from the Bot API. The main Telegram API is able to authorize both users and bots and is built on top of the MTProto
encryption protocol by means of [binary data serialized](https://core.telegram.org/mtproto/serialize) in a specific way, as described by the [TL language](https://core.telegram.org/mtproto/TL), and
delivered using UDP, TCP or even HTTP as transport-layer protocol. Clients that make use of Telegram's main API, such as
Pyrogram, implement all these details.


## What is the Bot API?


The [Bot API](https://core.telegram.org/bots/api) is an HTTP(S) interface for building normal bots using a sub-set of the main Telegram API. Bots are
special accounts that are authorized via tokens instead of phone numbers. The Bot API is built yet again on top of the
main Telegram API, but runs on an intermediate server application that in turn communicates with the actual Telegram
servers using MTProto.



## Advantages of the MTProto API


Here is a non-exhaustive list of all the advantages in using MTProto-based libraries -- such as Pyrogram -- instead of
the official HTTP Bot API. Using Pyrogram you can:

- `+` **Authorize both user and bot identities**
- `--` The Bot API only allows bot accounts

- `+` **Upload & download any file, up to 2000 MiB each (~2 GB)**
- `--` The Bot API allows uploads and downloads of files only up to 50 MB / 20 MB in size (respectively).

- `+` **Has less overhead due to direct connections to Telegram**
- `--` The Bot API uses an intermediate server to handle HTTP requests before they are sent to the actual
  Telegram servers.

- `+` **Run multiple sessions at once (for both user and bot identities)**
- `--` The Bot API intermediate server will terminate any other session in case you try to use the same
  bot again in a parallel connection.

- `+` **Has much more detailed types and powerful methods**
- `--` The Bot API types often miss some useful information about Telegram entities and some of the
  methods are limited as well.

- `+` **Obtain information about any message existing in a chat using their ids**
- `--` The Bot API simply doesn't support this

- `+` **Retrieve the whole chat members list of either public or private chats**
- `--` The Bot API simply doesn't support this

- `+` **Receive extra updates, such as the one about a user name change**
- `--` The Bot API simply doesn't support this

- `+` **Has more meaningful errors in case something went wrong**
- `--` The Bot API reports less detailed errors

- `+` **Get API version updates, and thus new features, sooner**
- `--` The Bot API is simply slower in implementing new features
