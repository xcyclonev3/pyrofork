#  Pyrogram - Telegram MTProto API Client Library for Python
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

"""Vectors are read exactly as (and as far as) before, without copying the data that follows them."""

import random
from io import BytesIO

import pytest

from pyrogram import raw
from pyrogram.raw.core import TLObject
from pyrogram.raw.core.list import List
from pyrogram.raw.core.primitives import Int, Long, Vector


def old_vector_read(data, t=None):
    count = Int.read(data)
    left = len(data.read())
    size = (left / count) if count else 0
    data.seek(-left, 1)
    return List(t.read(data) if t else Vector.read_bare(data, size) for _ in range(count))


def outcome(read, data, *args):
    stream = BytesIO(data)
    try:
        return read(stream, *args), stream.tell()
    except Exception as e:
        return type(e), e.args, stream.tell()


def vectors(rng):
    n = rng.randrange(0, 20)
    yield Vector([rng.getrandbits(31) for _ in range(n)], Int), Int
    yield Vector([rng.getrandbits(63) for _ in range(n)], Long), Long
    yield Vector([rng.getrandbits(31) for _ in range(n)], Int), None  # bare ints, told apart by size
    yield Vector([rng.getrandbits(63) for _ in range(n)], Long), None
    yield Vector([rng.choice([True, False]) for _ in range(n)], raw.core.primitives.Bool), None
    yield Vector([raw.types.Pong(msg_id=i, ping_id=i) for i in range(n)]), None
    yield Vector([raw.types.ChatBannedRights(until_date=i) for i in range(n)]), None


@pytest.mark.parametrize("seed", range(30))
def test_vector_read_is_unchanged(seed):
    rng = random.Random(seed)
    for vector, t in vectors(rng):
        data = vector[4:]  # after the Vector constructor id, as TLObject.read hands it over
        for tail in (b"", rng.randbytes(rng.randrange(1, 40))):  # data after the vector
            args = (t,) if t else ()
            assert outcome(Vector.read, data + tail, *args) == outcome(old_vector_read, data + tail, *args)


def test_big_answers_read_the_same():
    from pyrogram.raw.core.primitives.string import String

    messages = [raw.types.Message(
        id=i, peer_id=raw.types.PeerUser(user_id=7), date=i, message=f"text {i}" * 10,
        entities=[raw.types.MessageEntityBold(offset=0, length=4)],
        restriction_reason=[], reactions=raw.types.MessageReactions(results=[], recent_reactions=[])
    ) for i in range(300)]
    answer = raw.types.messages.Messages(messages=messages, topics=[], chats=[], users=[])
    data = answer.write()
    assert TLObject.read(BytesIO(data)).write() == data
    assert String.read(BytesIO(String("héllo"))) == "héllo"
