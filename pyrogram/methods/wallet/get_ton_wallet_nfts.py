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


class GetTonWalletNfts:
    async def get_ton_wallet_nfts(
        self: "pyrogram.Client",
        limit: int = 0,
        offset: str = ""
    ) -> Optional[AsyncGenerator["types.TonNft", None]]:
        """Get the NFTs stored on the TON wallet of the current user.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            limit (``int``, *optional*):
                Limits the number of NFTs to be retrieved.
                By default, no limit is applied and all NFTs are returned.

            offset (``str``, *optional*):
                Offset of the first NFT to be returned, as received from a previous request.
                By default, NFTs are returned starting from the first one.

        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.TonNft` objects.

        Example:
            .. code-block:: python

                async for nft in app.get_ton_wallet_nfts():
                    print(nft.name, nft.address)
        """
        current = 0
        total = abs(limit) or (1 << 31) - 1
        # TDLib documents that at most 20 NFTs can be requested at once
        limit = min(20, total)

        while True:
            r = await self.invoke(
                raw.functions.wallet.GetNfts(
                    offset=offset,
                    limit=limit
                )
            )

            nfts = [types.TonNft._parse(self, item) for item in r.items]

            if not nfts:
                return

            for nft in nfts:
                yield nft

                current += 1

                if current >= total:
                    return

            offset = r.next_offset

            if not offset:
                return
