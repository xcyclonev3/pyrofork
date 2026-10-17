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

"""Restarts of a session asked for at the same time."""

import asyncio

import pyrogram
from pyrogram.connection.transport import TCPAbridged
from pyrogram.session import Session

AUTH_KEY = bytes(range(256))


def slow_transport(started):
    class SlowTCP(TCPAbridged):
        """Connects after a delay that grows with every connection, then receives nothing until closed."""

        async def connect(self, address):
            started.append(self)
            await asyncio.sleep(0.02 * len(started))
            self.reader = asyncio.StreamReader()

        async def close(self):
            if self.reader is not None:
                self.reader.feed_eof()

    return SlowTCP


def test_restarts_asked_at_once_restart_once():
    async def main():
        loop = asyncio.get_running_loop()
        started = []
        errors = []
        client = pyrogram.Client("restart", api_id=1, api_hash="x", in_memory=True)
        client.protocol_factory = slow_transport(started)
        await client.storage.open()
        session = Session(client, 2, AUTH_KEY, False)

        async def send(*args, **kwargs):  # No server: Ping and InitConnection succeed at once
            return None

        session.send = send
        await session.start()
        started.clear()

        create_task = loop.create_task

        def create_watched_task(coro, **kwargs):
            def done(task):
                if not task.cancelled() and task.exception() is not None:
                    errors.append(task.exception())

            task = create_task(coro, **kwargs)
            task.add_done_callback(done)
            return task

        loop.create_task = create_watched_task
        try:
            # A broken connection asks for a restart from the receiving and the ping loops, for example
            await asyncio.gather(session.restart(), session.restart())
            await asyncio.sleep(0.2)  # The receiving loop of the new connection runs
        finally:
            del loop.create_task

        await session.stop()
        await client.storage.close()
        return started, errors

    started, errors = asyncio.run(main())
    # Each restart used to start a connection, and the receiving loop of the first one read the second one before it
    # was connected: AttributeError: 'NoneType' object has no attribute 'read'
    assert len(started) == 1
    assert errors == []


def test_an_error_while_receiving_restarts_the_session():
    async def main():
        started = []
        client = pyrogram.Client("receive-error", api_id=1, api_hash="x", in_memory=True)
        client.protocol_factory = slow_transport(started)
        await client.storage.open()
        session = Session(client, 2, AUTH_KEY, False)

        async def send(*args, **kwargs):  # No server: Ping and InitConnection succeed at once
            return None

        session.send = send
        await session.start()
        receiving = session.recv_task

        # Not an OSError, which only means a closed connection: e.g. two loops reading the same connection
        started[0].reader.set_exception(RuntimeError("read() called while another coroutine is already waiting"))
        await asyncio.sleep(0.2)  # The session restarts on a new connection

        restarted = session.is_started.is_set()
        await session.stop()  # Used to raise the error of the receiving loop again, so restarts could never stop it
        await client.storage.close()
        return started, receiving, restarted

    started, receiving, restarted = asyncio.run(main())
    assert len(started) == 2
    assert restarted
    assert receiving.done() and receiving.exception() is None
