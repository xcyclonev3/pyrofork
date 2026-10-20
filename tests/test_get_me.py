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

"""get_me asks Telegram for the own account only, offline."""

import asyncio
from io import BytesIO

import pyrogram
from pyrogram import raw
from pyrogram.raw.core import TLObject


def test_get_me_asks_for_the_account_only():
    async def main():
        client = pyrogram.Client("me", api_id=1, api_hash="x", in_memory=True)
        await client.storage.open()
        queries = []

        async def invoke(query, *args, **kwargs):
            queries.append(query)
            user = raw.types.User(id=7, is_self=True, access_hash=11, first_name="Me", username="me7")
            return raw.core.List([TLObject.read(BytesIO(user.write()))])  # As read from the network

        client.invoke = invoke
        me = await client.get_me()
        peer = await client.storage.get_peer_by_id(7)
        await client.storage.close()
        return queries, me, peer

    queries, me, peer = asyncio.run(main())
    # users.getFullUser sent the whole profile, of which only the User was used
    assert len(queries) == 1 and isinstance(queries[0], raw.functions.users.GetUsers)
    assert [type(i) for i in queries[0].id] == [raw.types.InputUserSelf]
    assert (me.id, me.is_self, me.first_name, me.username) == (7, True, "Me", "me7")
    # Cached as before, when invoke cached the users of the users.getFullUser result
    assert (peer.user_id, peer.access_hash) == (7, 11)
