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

"""Opening a session file compacts it only when much of it is free pages."""

import asyncio
import sqlite3

from pyrogram.storage import FileStorage

PEERS = [(i, i, "user", f"user{i}", None) for i in range(1, 5001)]


def pages(path):
    """(free pages, all pages) of a database file."""
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("PRAGMA freelist_count").fetchone()[0], conn.execute("PRAGMA page_count").fetchone()[0]
    finally:
        conn.close()


def open_and_close(tmp_path, peers=()):
    async def main():
        storage = FileStorage("vacuum", tmp_path)
        await storage.open()
        if peers:
            await storage.update_peers(peers)
        await storage.close()

    asyncio.run(main())
    return tmp_path / "vacuum.session"


def with_free_pages(tmp_path, deleted):
    """A session file of 5000 peers, the first `deleted` of them removed: their pages are free."""
    path = open_and_close(tmp_path, PEERS)
    conn = sqlite3.connect(str(path))
    conn.execute("DELETE FROM peers WHERE id <= ?", (deleted,))
    conn.commit()
    conn.close()
    return path


def test_a_few_free_pages_are_left_alone(tmp_path):
    path = with_free_pages(tmp_path, 500)
    before = pages(path)
    assert 0 < before[0] <= before[1] * FileStorage.VACUUM_FREE_RATIO
    open_and_close(tmp_path)
    # open() used to VACUUM every time: the whole file rewritten, in the event loop
    assert pages(path) == before


def test_many_free_pages_are_compacted(tmp_path):
    path = with_free_pages(tmp_path, 4000)
    before = pages(path)
    size = path.stat().st_size
    assert before[0] > before[1] * FileStorage.VACUUM_FREE_RATIO
    open_and_close(tmp_path)
    assert pages(path)[0] == 0 and path.stat().st_size < size


def test_new_session_file(tmp_path):
    path = open_and_close(tmp_path)
    assert pages(path)[0] == 0
