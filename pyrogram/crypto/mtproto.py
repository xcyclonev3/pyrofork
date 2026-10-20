#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

from hashlib import sha256
from io import BytesIO
from os import urandom
from typing import Optional

from pyrogram.errors import SecurityCheckMismatch
from pyrogram.raw.core import Message, Long, Int
from . import aes


def kdf(auth_key: bytes, msg_key: bytes, outgoing: bool) -> tuple:
    # https://core.telegram.org/mtproto/description#defining-aes-key-and-initialization-vector
    x = 0 if outgoing else 8

    sha256_a = sha256(msg_key + auth_key[x: x + 36]).digest()
    sha256_b = sha256(auth_key[x + 40:x + 76] + msg_key).digest()  # 76 = 40 + 36

    aes_key = sha256_a[:8] + sha256_b[8:24] + sha256_a[24:32]
    aes_iv = sha256_b[:8] + sha256_a[8:24] + sha256_b[24:32]

    return aes_key, aes_iv


def pack(
    message: Message,
    salt: int,
    session_id: bytes,
    auth_key: bytes,
    auth_key_id: bytes,
    body: Optional[bytes] = None
) -> bytes:
    """Encrypt a message. *body* is message.body already serialized (see MsgFactory.new), if available."""
    if body is None:
        message_data = (message.write(),)
    else:
        # The same bytes as message.write(), without serializing the body again
        message_data = (Long(message.msg_id), Int(message.seq_no), Int(message.length), body)

    padding = urandom(-(16 + sum(map(len, message_data)) + 12) % 16 + 12)
    # Put together once: the hash and the encryption read the same bytes, no copy each
    data = b"".join((Long(salt), session_id, *message_data, padding))

    # 88 = 88 + 0 (outgoing message)
    msg_key_large = sha256(auth_key[88: 88 + 32])
    msg_key_large.update(data)
    msg_key = msg_key_large.digest()[8:24]
    aes_key, aes_iv = kdf(auth_key, msg_key, True)

    return auth_key_id + msg_key + aes.ige256_encrypt(data, aes_key, aes_iv)


def unpack(
    b: BytesIO,
    session_id: bytes,
    auth_key: bytes,
    auth_key_id: bytes,
    result_vectors: dict = None
) -> Message:
    SecurityCheckMismatch.check(b.read(8) == auth_key_id, "b.read(8) == auth_key_id")

    msg_key = b.read(16)
    aes_key, aes_iv = kdf(auth_key, msg_key, False)
    data = BytesIO(aes.ige256_decrypt(b.read(), aes_key, aes_iv))
    data.read(8)  # Salt

    # https://core.telegram.org/mtproto/security_guidelines#checking-session-id
    SecurityCheckMismatch.check(data.read(8) == session_id, "data.read(8) == session_id")

    try:
        # The element types of the bare vectors that requests return, see Session.send
        message = Message.read(data, result_vectors) if result_vectors else Message.read(data)
    except KeyError as e:
        if e.args[0] == 0:
            raise ConnectionError("Received empty data. Check your internet connection.")

        left = data.read().hex()

        left = [left[i:i + 64] for i in range(0, len(left), 64)]
        left = [[left[i:i + 8] for i in range(0, len(left), 8)] for left in left]
        left = "\n".join(" ".join(x for x in left) for left in left)

        raise ValueError(f"The server sent an unknown constructor: {hex(e.args[0])}\n{left}")

    # https://core.telegram.org/mtproto/security_guidelines#checking-sha256-hash-value-of-msg-key
    # 96 = 88 + 8 (incoming message)
    decrypted = data.getvalue()  # The decrypted bytes themselves, not a copy
    msg_key_large = sha256(auth_key[96:96 + 32])
    msg_key_large.update(decrypted)
    SecurityCheckMismatch.check(
        msg_key == msg_key_large.digest()[8:24],
        "msg_key == sha256(auth_key[96:96 + 32] + data.getvalue()).digest()[8:24]"
    )

    # https://core.telegram.org/mtproto/security_guidelines#checking-message-length
    # The payload follows salt (8) + session_id (8) + msg_id (8) + seq_no (4) + length (4). The lengths of the
    # payload and of payload[message.length:] are computed without copying them
    payload_length = max(len(decrypted) - 32, 0)
    padding_length = len(range(payload_length)[message.length:])
    SecurityCheckMismatch.check(12 <= padding_length <= 1024, "12 <= len(padding) <= 1024")
    SecurityCheckMismatch.check(payload_length % 4 == 0, "len(payload) % 4 == 0")

    # https://core.telegram.org/mtproto/security_guidelines#checking-msg-id
    SecurityCheckMismatch.check(message.msg_id % 2 != 0, "message.msg_id % 2 != 0")

    return message
