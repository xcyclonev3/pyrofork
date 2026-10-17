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

import pyrogram
from pyrogram import raw
from pyrogram.errors import AuthBytesInvalid
from pyrogram.session import Session
from pyrogram.session.auth import Auth


async def get_session(client: "pyrogram.Client", dc_id: int, is_media: bool = True):
    """A session authorized on another data center, or the client itself on its own data center.

    Media sessions are meant for files; other requests sent to another data center (e.g. TON Center requests) use
    regular sessions, like TDLib does. A session is kept for later only once it is started and authorized.
    """
    if dc_id == await client.storage.dc_id():
        return client

    sessions = client.media_sessions if is_media else client.dc_sessions

    async with client.media_sessions_lock:
        if sessions.get(dc_id):
            return sessions[dc_id]

        session = Session(
            client, dc_id,
            await Auth(client, dc_id, await client.storage.test_mode()).create(),
            await client.storage.test_mode(), is_media=is_media
        )

        await session.start()

        try:
            for _ in range(3):
                exported_auth = await client.invoke(
                    raw.functions.auth.ExportAuthorization(
                        dc_id=dc_id
                    )
                )

                try:
                    await session.invoke(
                        raw.functions.auth.ImportAuthorization(
                            id=exported_auth.id,
                            bytes=exported_auth.bytes
                        )
                    )
                except AuthBytesInvalid:
                    continue
                else:
                    break
            else:
                raise AuthBytesInvalid
        except BaseException:
            await session.stop()
            raise

        sessions[dc_id] = session

        return session
