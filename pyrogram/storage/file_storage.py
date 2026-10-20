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

import logging
import os
import sqlite3
from pathlib import Path

from .sqlite_storage import SQLiteStorage

log = logging.getLogger(__name__)


UPDATE_STATE_SCHEMA = """
CREATE TABLE update_state
(
    id   INTEGER PRIMARY KEY,
    pts  INTEGER,
    qts  INTEGER,
    date INTEGER,
    seq  INTEGER
);
"""


class FileStorage(SQLiteStorage):
    FILE_EXTENSION = ".session"
    # Share of free pages above which open() compacts the session file
    VACUUM_FREE_RATIO = 0.25

    def __init__(self, name: str, workdir: Path):
        super().__init__(name)

        self.database = workdir / (self.name + self.FILE_EXTENSION)

    def update(self):
        version = self.version()

        if version == 1:
            with self.conn:
                self.conn.execute("DELETE FROM peers")

            version += 1

        if version == 2:
            with self.conn:
                self.conn.execute("ALTER TABLE sessions ADD api_id INTEGER")

            version += 1

        if version == 3:
            with self.conn:
                self.conn.executescript(UPDATE_STATE_SCHEMA)

            version += 1

        self.version(version)

    async def open(self):
        path = self.database
        file_exists = path.is_file()

        self.conn = sqlite3.connect(str(path), timeout=1, check_same_thread=False)

        if not file_exists:
            self.create()
        else:
            self.update()

        # VACUUM rewrites the whole file, in the event loop (about 100 ms for 100 000 peers): only worth it when a good
        # part of the file is free pages, which SQLite reuses anyway
        page_count = self.conn.execute("PRAGMA page_count").fetchone()[0]
        free_pages = self.conn.execute("PRAGMA freelist_count").fetchone()[0]

        if free_pages > page_count * self.VACUUM_FREE_RATIO:
            with self.conn:
                self.conn.execute("VACUUM")

    async def delete(self):
        os.remove(self.database)
