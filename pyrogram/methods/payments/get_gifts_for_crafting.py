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

from typing import AsyncGenerator, Optional

import pyrogram
from pyrogram import raw, types


class GetGiftsForCrafting:
    async def get_gifts_for_crafting(
        self: "pyrogram.Client",
        gift_id: int,
        limit: int = 0,
        offset: str = ""
    ) -> Optional[AsyncGenerator["types.Gift", None]]:
        """Get the upgraded gifts of the current user that can be used to craft another gift.

        Use :meth:`~pyrogram.Client.craft_gift` to craft a gift.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            gift_id (``int``):
                Identifier of the regular gift that will be used for crafting:
                the *regular_gift_id* of the first upgraded gift chosen for crafting.

            limit (``int``, *optional*):
                Maximum amount of gifts to be returned.
                By default, all gifts are returned.

            offset (``str``, *optional*):
                Offset of the results to be returned.

        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.Gift` objects.

        Example:
            .. code-block:: python

                async for gift in app.get_gifts_for_crafting(first_gift.regular_gift_id):
                    print(gift.name, gift.craft_probability_per_mille, gift.can_craft_at)
        """
        current = 0
        total = abs(limit) or (1 << 31) - 1
        limit = min(100, total)

        while True:
            r = await self.invoke(
                raw.functions.payments.GetCraftStarGifts(
                    gift_id=gift_id,
                    offset=offset,
                    limit=limit
                ),
                sleep_threshold=60
            )

            users = {i.id: i for i in r.users}
            chats = {i.id: i for i in r.chats}

            gifts = [
                await types.Gift._parse_saved(self, gift, users, chats)
                for gift in r.gifts
            ]

            if not gifts:
                return

            for gift in gifts:
                yield gift

                current += 1

                if current >= total:
                    return

            offset = r.next_offset

            if not offset:
                return
