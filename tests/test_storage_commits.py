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

"""Session files commit the peers and the update states together, not on every update."""

import asyncio
import sqlite3

import pytest

from pyrogram.storage import FileStorage, MemoryStorage
from pyrogram.storage.sqlite_storage import SQLiteStorage

PEERS = [(7, 1, "user", "someone", None), (-1000000000123, 2, "supergroup", "group", None)]


@pytest.fixture(autouse=True)
def short_delay(monkeypatch):
    monkeypatch.setattr(SQLiteStorage, "COMMIT_DELAY", 0.1)


def committed(path, query):
    """What another connection sees: only committed changes."""
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(query).fetchall()
    finally:
        conn.close()


async def write_updates(storage):
    for i in range(50):
        await storage.update_peers(PEERS)
        await storage.update_usernames([(7, "someone"), (7, "another")])
        await storage.update_state((0, i, None, 1, None))
    await storage.update_state((-1000000000123, 5, None, 1, None))
    await storage.remove_state(-1000000000123)


def test_changes_are_committed_together_after_the_delay(tmp_path):
    async def main():
        storage = FileStorage("commits", tmp_path)
        await storage.open()
        await write_updates(storage)

        # Not on every update: nothing is on the disk yet
        assert committed(storage.database, "SELECT count(*) FROM peers") == [(0,)]
        assert committed(storage.database, "SELECT count(*) FROM update_state") == [(0,)]
        # The storage itself reads its changes
        assert len(await storage.update_state()) == 1
        assert (await storage.get_peer_by_id(7)).user_id == 7

        await asyncio.sleep(0.25)
        assert committed(storage.database, "SELECT count(*) FROM peers") == [(2,)]
        assert committed(storage.database, "SELECT pts FROM update_state") == [(49,)]
        assert sorted(committed(storage.database, "SELECT id FROM usernames")) == [("another",), ("someone",)]
        assert storage.commit_handle is None
        await storage.close()

    asyncio.run(main())


def test_save_and_close_commit_right_away(tmp_path):
    async def main():
        storage = FileStorage("save", tmp_path)
        await storage.open()
        await write_updates(storage)
        await storage.save()
        assert committed(storage.database, "SELECT count(*) FROM peers") == [(2,)]
        assert storage.commit_handle is None

        await storage.update_state((0, 100, None, 1, None))
        await storage.close()  # before the delay is over

        reopened = FileStorage("save", tmp_path)
        await reopened.open()
        states = await reopened.update_state()
        await reopened.close()
        return states

    assert asyncio.run(main()) == [(0, 100, None, 1, None)]


def test_sessions_without_the_usernames_table(tmp_path):
    async def main():
        storage = FileStorage("old", tmp_path)
        await storage.open()
        await storage.close()

        conn = sqlite3.connect(str(storage.database))  # A session file made before the table existed
        conn.execute("DROP TABLE usernames")
        conn.commit()
        conn.close()

        storage = FileStorage("old", tmp_path)
        await storage.open()
        await storage.update_peers(PEERS)
        await storage.update_usernames([(7, "another")])
        peer = await storage.get_peer_by_username("another")
        await storage.close()
        return peer

    assert asyncio.run(main()).user_id == 7


def test_memory_storage():
    async def main():
        storage = MemoryStorage("memory")
        await storage.open()
        await write_updates(storage)
        peer = await storage.get_peer_by_username("someone")
        await asyncio.sleep(0.25)
        await storage.close()
        return peer

    assert asyncio.run(main()).user_id == 7


def test_commit_planned_in_a_closed_loop(tmp_path):
    storage = FileStorage("loops", tmp_path)

    async def first():
        await storage.open()
        await storage.update_peers(PEERS[:1])  # Its event loop is closed before the commit

    async def second():
        await storage.update_peers(PEERS[1:])
        await asyncio.sleep(0.25)

    asyncio.run(first())
    asyncio.run(second())
    assert committed(storage.database, "SELECT count(*) FROM peers") == [(2,)]
    asyncio.run(storage.close())


def test_nothing_is_written_for_no_peers(tmp_path):
    async def main():
        storage = FileStorage("empty", tmp_path)
        await storage.open()

        await storage.update_peers([])
        await storage.update_usernames([])
        planned = storage.commit_handle

        await storage.close()
        return planned

    assert asyncio.run(main()) is None


def test_no_usernames_still_make_the_table_of_old_sessions(tmp_path):
    """Looking for a username reads that table, which older session files don't have until the first update."""
    async def main():
        storage = FileStorage("old", tmp_path)
        await storage.open()
        storage.conn.execute("DROP TABLE usernames")
        storage.has_usernames_table = False

        await storage.update_usernames([])

        with pytest.raises(KeyError):
            await storage.get_peer_by_username("nobody")

        await storage.close()

    asyncio.run(main())
