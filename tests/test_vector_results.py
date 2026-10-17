#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrofork.
#
#  Pyrofork is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrofork is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrofork.  If not, see <http://www.gnu.org/licenses/>.

"""Results that are bare vectors of primitive values (e.g. Vector<string>) are read with their declared type."""

import asyncio
import gzip
import os
from io import BytesIO

import pytest

from pyrogram import raw
from pyrogram.raw.core import GzipPacked, Message, MsgContainer
from pyrogram.raw.core.primitives import Bytes, Int, Long, String, Vector
from pyrogram.session import Session

CURRENCIES = raw.functions.payments.GetOnrampBaseCurrencies(provider="p", crypto_currency="TON")


def message(body: bytes, msg_id: int = 1) -> bytes:
    return Long(msg_id) + Int(1) + Int(len(body)) + body


def rpc_result(req_msg_id: int, result: bytes) -> bytes:
    return Int(raw.types.RpcResult.ID, False) + Long(req_msg_id) + result


@pytest.mark.parametrize("values", [["USD", "EUR", "GBP"], ["USD", "EURO", "Yen"], ["USDT"], []])
def test_string_vector_result(values):
    data = message(rpc_result(42, Vector(values, String)))

    parsed = Message.read(BytesIO(data), {42: String})
    assert isinstance(parsed.body, raw.types.RpcResult) and parsed.body.req_msg_id == 42
    assert list(parsed.body.result) == values


def test_string_vector_result_needs_its_type():
    # Without the declared type, three-letter strings are taken for ints: what the declaration avoids
    data = message(rpc_result(42, Vector(["USD", "EUR"], String)))
    assert all(isinstance(i, int) for i in Message.read(BytesIO(data)).body.result)


def test_gzip_packed_string_vector_result():
    packed = Int(GzipPacked.ID, False) + Bytes(gzip.compress(Vector(["USD", "EURO"], String)))
    parsed = Message.read(BytesIO(message(rpc_result(42, packed))), {42: String})

    assert list(parsed.body.result) == ["USD", "EURO"]


def test_vector_results_in_a_container():
    strings = message(rpc_result(42, Vector(["USD", "EURO"], String)), msg_id=3)
    longs = message(rpc_result(43, Vector([5, 6], Long)), msg_id=5)
    container = Int(MsgContainer.ID, False) + Int(2) + strings + longs

    parsed = Message.read(BytesIO(message(container)), {42: String})
    first, second = parsed.body.messages
    assert list(first.body.result) == ["USD", "EURO"]
    assert list(second.body.result) == [5, 6]  # not declared: read as before


def test_errors_of_requests_returning_vectors():
    error = raw.types.RpcError(error_code=400, error_message="PROVIDER_INVALID").write()
    parsed = Message.read(BytesIO(message(rpc_result(42, error))), {42: String})

    assert isinstance(parsed.body.result, raw.types.RpcError)
    assert parsed.body.result.error_message == "PROVIDER_INVALID"


def test_functions_declare_their_vector_results():
    assert CURRENCIES.RESULT_VECTOR is String
    assert raw.functions.contacts.GetContactIDs.RESULT_VECTOR.__name__ == "Int"
    assert raw.functions.photos.DeletePhotos.RESULT_VECTOR.__name__ == "Long"
    assert not hasattr(raw.functions.users.GetUsers, "RESULT_VECTOR")  # Vector<User>: boxed objects


class FakeConnection:
    def __init__(self, session):
        self.session = session
        self.declared = []

    async def send(self, payload):
        self.declared.append(dict(self.session.result_vectors))


@pytest.mark.parametrize("query, expected", [
    (CURRENCIES, String),
    (raw.functions.InvokeWithoutUpdates(query=CURRENCIES), String),
    (raw.functions.InvokeWithLayer(layer=230, query=raw.functions.InitConnection(
        api_id=1, device_model="d", system_version="s", app_version="a", system_lang_code="en", lang_pack="",
        lang_code="en", query=CURRENCIES)), String),
    (raw.functions.users.GetUsers(id=[]), None),
])
def test_session_declares_vector_results(query, expected):
    async def main():
        session = Session(None, 2, os.urandom(256), False)
        session.connection = FakeConnection(session)

        with pytest.raises(TimeoutError):
            await session.send(query, timeout=0.01)

        return session

    session = asyncio.run(main())
    [declared] = session.connection.declared
    assert list(declared.values()) == ([expected] if expected else [])
    assert session.result_vectors == {}  # forgotten once the request is done
