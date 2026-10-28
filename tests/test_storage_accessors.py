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

"""The session values (dc_id, api_id, ...) are read and written under the name of their accessor, cheaply."""

import asyncio
import inspect

import pytest

from pyrogram.storage import FileStorage, MemoryStorage

VALUES = {
    "dc_id": 4,
    "api_id": 12345,
    "test_mode": True,
    "auth_key": bytes(range(256)),
    "date": 1700000000,
    "user_id": 777,
    "is_bot": False,
}


@pytest.fixture(autouse=True)
def no_stack_inspection(monkeypatch):
    def stack(*args, **kwargs):
        # It reads the source file of every frame: ~10 ms per accessor call in a big application, and every client
        # start and session restart reads some, so hundreds of clients in one process kept the event loop busy
        raise AssertionError("inspect.stack() called")

    monkeypatch.setattr(inspect, "stack", stack)


@pytest.mark.parametrize("kind", ["memory", "file"])
def test_values_are_kept_under_their_accessor(kind, tmp_path):
    async def main():
        storage = MemoryStorage("accessors") if kind == "memory" else FileStorage("accessors", tmp_path)
        await storage.open()

        for name, value in VALUES.items():
            await getattr(storage, name)(value)

        read = {name: await getattr(storage, name)() for name in VALUES}
        await storage.close()

        if kind == "file":  # Committed right away, under the same columns
            storage = FileStorage("accessors", tmp_path)
            await storage.open()
            assert {name: await getattr(storage, name)() for name in VALUES} == VALUES
            await storage.close()

        return read

    assert asyncio.run(main()) == VALUES


class Collection:
    """The two calls of an async Mongo collection the accessors make."""

    def __init__(self):
        self.document = {"_id": 0}

    async def find_one(self, query, projection):
        assert query == {"_id": 0}
        return {key: self.document[key] for key in projection if key in self.document}

    async def update_one(self, query, update, upsert=False):
        assert query == {"_id": 0} and upsert
        self.document.update(update["$set"])


def test_mongo_values_are_kept_under_their_accessor():
    mongo_storage = pytest.importorskip("pyrogram.storage.mongo_storage")

    async def main():
        storage = mongo_storage.MongoStorage.__new__(mongo_storage.MongoStorage)  # Without a database connection
        storage._session = Collection()

        for name, value in VALUES.items():
            await getattr(storage, name)(value)

        return storage._session.document, {name: await getattr(storage, name)() for name in VALUES}

    document, read = asyncio.run(main())
    assert document == {"_id": 0, **VALUES}
    assert read == VALUES
