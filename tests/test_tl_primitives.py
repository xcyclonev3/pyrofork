#  Pyrogram - Telegram MTProto API Client Library for Python
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

import pickle
import random
from io import BytesIO

import pytest

from pyrogram import raw
from pyrogram.raw.core import TLObject
from pyrogram.raw.core.list import List
from pyrogram.raw.core.primitives import Bytes, Int, Long, String, Vector


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


@pytest.mark.parametrize("read, data, expected", [
    (Int.read, b"", 0),
    (Int.read, b"\x01", 1),
    (Int.read, b"\xff\xff\xff\xff", -1),
    (Long.read, b"\xff" * 8, -1),
    (Long.read, b"\x01", 1),
    (Bytes.read, b"", b""),
    (Bytes.read, b"\x02ab\x00", b"ab"),
    (Bytes.read, b"\x02a", b"a"),
    (Bytes.read, b"\xfe\x01\x00\x00a\x00\x00\x00", b"a"),
    (String.read, b"", ""),
    (String.read, b"\x02\xff\xfe\x00", "\ufffd\ufffd"),
    (Vector.read, b"", []),
    (Vector.read, b"\x00\x00\x00\x00", []),
], ids=repr)
def test_short_or_empty_data_is_read_as_before(read, data, expected):
    """A packet cut short ends in a constructor 0, which tells the session that the connection went wrong."""
    assert read(BytesIO(data)) == expected


def test_a_packet_cut_short_ends_in_constructor_0():
    with pytest.raises(KeyError) as error:
        TLObject.read(BytesIO(b""))

    assert error.value.args[0] == 0


def test_int_read_keeps_its_arguments():
    assert Int.read(BytesIO(b"\xff\xff\xff\xff"), False) == 2 ** 32 - 1
    assert Int.read(BytesIO(b"\xff\xff\xff\xff"), signed=False) == 2 ** 32 - 1
    assert Long.read(BytesIO(b"\xff" * 8), False) == 2 ** 64 - 1


@pytest.mark.parametrize("t", [None, Int, Long], ids=["bare", "int", "long"])
def test_an_empty_vector_stops_right_after_its_count(t):
    data = BytesIO(b"\x00\x00\x00\x00rest")

    assert Vector.read(data, t) == [] and type(Vector.read(BytesIO(b"\x00" * 4), t)) is List
    assert data.read() == b"rest"


def test_objects_read_have_all_their_fields():
    written = raw.types.UserProfilePhoto(photo_id=1, dc_id=2, personal=True, stripped_thumb=b"t").write()

    photo = TLObject.read(BytesIO(written))

    assert type(photo) is raw.types.UserProfilePhoto
    assert (photo.photo_id, photo.dc_id, photo.personal, photo.has_video, photo.stripped_thumb) == (1, 2, True, False, b"t")
    assert photo == raw.types.UserProfilePhoto(photo_id=1, dc_id=2, personal=True, has_video=False, stripped_thumb=b"t")
    assert photo.write() == written
    assert pickle.loads(pickle.dumps(photo)) == photo


def test_objects_without_fields_can_be_read():
    empty = TLObject.read(BytesIO(raw.types.InputPeerEmpty().write()))

    assert type(empty) is raw.types.InputPeerEmpty
