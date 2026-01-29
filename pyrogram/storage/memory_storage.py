#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
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

import base64
import logging
import sqlite3
import struct
from typing import Optional

from .sqlite_storage import SQLiteStorage

log = logging.getLogger(__name__)


class MemoryStorage(SQLiteStorage):
    def __init__(self, name: str, session_string: str = None):
        super().__init__(name)
        self.session_string = session_string
        self.conn: Optional[sqlite3.Connection] = None

    async def open(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)

        try:
            self.conn.execute("PRAGMA journal_mode=OFF")
            self.conn.execute("PRAGMA synchronous=OFF")
            self.conn.execute("PRAGMA temp_store=MEMORY")
        except sqlite3.Error as e:
            log.error(f"Failed to set PRAGMA optimizations: {e}")

        self.create()

        if not self.session_string:
            return

        session_bytes = base64.urlsafe_b64decode(
            self.session_string + "=" * (-len(self.session_string) % 4)
        )
        data_len = len(session_bytes)

        current_struct_size = struct.calcsize(self.SESSION_STRING_FORMAT)
        old_struct_size = struct.calcsize(self.OLD_SESSION_STRING_FORMAT)
        old_struct_size_64 = struct.calcsize(self.OLD_SESSION_STRING_FORMAT_64)

        if data_len == current_struct_size:
            dc_id, api_id, test_mode, auth_key, user_id, is_bot = struct.unpack(
                self.SESSION_STRING_FORMAT, session_bytes
            )
            await self.api_id(api_id)

        elif data_len in (old_struct_size, old_struct_size_64):
            fmt = self.OLD_SESSION_STRING_FORMAT if data_len == old_struct_size else self.OLD_SESSION_STRING_FORMAT_64
            dc_id, test_mode, auth_key, user_id, is_bot = struct.unpack(fmt, session_bytes)
            log.warning("You are using an old session string format. Use export_session_string to update")
            return

        else:
            log.error(f"Invalid session string size: {data_len} bytes. Expected {current_struct_size} bytes.")
            return

        await self.dc_id(dc_id)
        await self.test_mode(test_mode)
        await self.auth_key(auth_key)
        await self.user_id(user_id)
        await self.is_bot(is_bot)
        await self.date(0)

    async def delete(self):
        if self.conn:
            try:
                self.conn.interrupt()
                self.conn.close()
            except sqlite3.Error as e:
                log.debug(f"Error closing SQLite memory connection: {e}")
            finally:
                self.conn = None

    def __del__(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            finally:
                self.conn = None