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

import re
from typing import List, Optional, Union

import pyrogram
from pyrogram import errors, raw, types


class CraftGift:
    async def craft_gift(
        self: "pyrogram.Client",
        owned_gift_ids: List[Union[int, str]]
    ) -> Optional["types.CraftGiftResult"]:
        """Craft a new gift from other upgraded gifts, which will be permanently lost.

        All the gifts must be upgraded from the same regular gift, see
        :meth:`~pyrogram.Client.get_gifts_for_crafting`.
        The more gifts are used, the higher the chance of success: the *craft_probability_per_mille* of the gifts
        are summed up.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            owned_gift_ids (List of ``int`` | ``str``):
                Identifiers of the 1 to 4 gifts to use for crafting.
                For a user gift, you can use the message ID (int) of the gift message.
                For a channel gift, you can use the packed format `chatID_savedID` (str).
                For a upgraded gift, you can use the gift link.
                In case of success, the crafted gift gets the number of the first gift, so the first gift must not
                have been exported to the TON blockchain.

        Returns:
            :obj:`~pyrogram.types.CraftGiftResult`: On success, the result of the crafting is returned.
            None is returned if the answer of the server contains neither the crafted gift nor the failure; check
            the gifts of the account before trying again.

        Example:
            .. code-block:: python

                from pyrogram import enums

                result = await app.craft_gift([123, 456])

                if result.type == enums.CraftGiftResultType.SUCCESS:
                    print(result.gift.link)
                elif result.type == enums.CraftGiftResultType.TOO_EARLY:
                    print(f"Retry in {result.retry_after} seconds")
        """
        stargifts = [await _get_input_saved_star_gift(self, i) for i in owned_gift_ids]

        try:
            r = await self.invoke(
                raw.functions.payments.CraftStarGift(
                    stargift=stargifts
                )
            )
        except errors.RPCError as e:
            result = types.CraftGiftResult._parse_error(self, e)

            if result is None:
                raise

            return result

        return await types.CraftGiftResult._parse(self, r)


async def _get_input_saved_star_gift(
    client: "pyrogram.Client",
    owned_gift_id: Union[int, str]
) -> "raw.base.InputSavedStarGift":
    if isinstance(owned_gift_id, int):
        return raw.types.InputSavedStarGiftUser(msg_id=owned_gift_id)

    saved_gift_match = re.match(r"^(-?\d+)_(\d+)$", owned_gift_id)
    slug_match = client.UPGRADED_GIFT_RE.match(owned_gift_id)

    if saved_gift_match:
        return raw.types.InputSavedStarGiftChat(
            peer=await client.resolve_peer(int(saved_gift_match.group(1))),
            saved_id=int(saved_gift_match.group(2))
        )

    if slug_match:
        return raw.types.InputSavedStarGiftSlug(slug=slug_match.group(1))

    return raw.types.InputSavedStarGiftUser(msg_id=int(owned_gift_id))
