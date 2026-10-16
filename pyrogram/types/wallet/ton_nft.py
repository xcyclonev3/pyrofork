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

import json
from typing import List, Optional

import pyrogram
from pyrogram import raw, types
from ..object import Object


class TonNft(Object):
    """An NFT in the TON blockchain.

    Parameters:
        address (``str``):
            Address of the NFT.

        owner_address (``str``):
            Address of the owner of the NFT.

        index (``str``):
            Index of the NFT among all NFTs from the collection, as a decimal number.

        collection_address (``str``, *optional*):
            Address of the collection to which the NFT belongs, if known.

        name (``str``, *optional*):
            Name of the NFT.

        description (``str``, *optional*):
            Description of the NFT.

        image (:obj:`~pyrogram.types.WebDocument`, *optional*):
            Image of the NFT.

        thumbnail (:obj:`~pyrogram.types.WebDocument`, *optional*):
            Thumbnail (smaller image) of the NFT.

        content (:obj:`~pyrogram.types.WebDocument`, *optional*):
            Content of the NFT.

        sticker (:obj:`~pyrogram.types.WebDocument`, *optional*):
            Animated sticker (Lottie animation) of the NFT.

        attributes (List of :obj:`~pyrogram.types.TonNftAttribute`, *optional*):
            Attributes of the NFT.

        extra (``dict``, *optional*):
            Additional data about the NFT, decoded from JSON.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        address: str,
        owner_address: str,
        index: str,
        collection_address: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        image: Optional["types.WebDocument"] = None,
        thumbnail: Optional["types.WebDocument"] = None,
        content: Optional["types.WebDocument"] = None,
        sticker: Optional["types.WebDocument"] = None,
        attributes: Optional[List["types.TonNftAttribute"]] = None,
        extra: Optional[dict] = None
    ):
        super().__init__(client)

        self.address = address
        self.owner_address = owner_address
        self.index = index
        self.collection_address = collection_address
        self.name = name
        self.description = description
        self.image = image
        self.thumbnail = thumbnail
        self.content = content
        self.sticker = sticker
        self.attributes = attributes
        self.extra = extra

    @staticmethod
    def _parse(client, nft: "raw.types.wallet.NftItem") -> Optional["TonNft"]:
        if nft is None:
            return None

        extra = None

        if nft.extra is not None:
            try:
                extra = json.loads(nft.extra.data)
            except ValueError:  # Like TDLib, drop extra data that isn't valid JSON
                pass

        return TonNft(
            address=nft.address,
            owner_address=nft.owner_address,
            index=nft.index,
            collection_address=nft.collection_address,
            name=nft.name,
            description=nft.description,
            image=types.WebDocument._parse(client, nft.image),
            thumbnail=types.WebDocument._parse(client, nft.image_small),
            content=types.WebDocument._parse(client, nft.content_url),
            sticker=types.WebDocument._parse(client, nft.lottie),
            attributes=[types.TonNftAttribute._parse(client, a) for a in nft.attributes] if nft.attributes else None,
            extra=extra,
            client=client
        )
