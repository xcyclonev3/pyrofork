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

"""Offline tests of the TON wallet, TON Connect, on-ramp and TON Center API (Layer 230)."""
import asyncio

from pyrogram import raw, types
from tests.test_layer_roundtrip import offline_client, roundtrip, PEERS


def collect(make_gen):
    """Run a Client generator method inside a running loop and collect what it yields."""
    async def main():
        return [item async for item in make_gen()]
    return asyncio.run(main())


def web_document(url, mime_type="image/png", w=100, h=100):
    return raw.types.WebDocument(
        url=url, access_hash=7, size=1024, mime_type=mime_type,
        attributes=[raw.types.DocumentAttributeImageSize(w=w, h=h)],
    )


def nft_item(address="EQnft", **kwargs):
    return raw.types.wallet.NftItem(address=address, owner_address="UQowner", index="42", **kwargs)


# NFTs

def test_web_document_parses_proxied_and_direct_files():
    proxied = roundtrip(raw.types.WebDocument(
        url="https://example.org/a.png", access_hash=77, size=1024, mime_type="image/png",
        attributes=[
            raw.types.DocumentAttributeImageSize(w=640, h=480),
            raw.types.DocumentAttributeFilename(file_name="a.png"),
        ],
    ))
    doc = types.WebDocument._parse(None, proxied)
    assert (doc.url, doc.access_hash, doc.size, doc.mime_type) == ("https://example.org/a.png", 77, 1024, "image/png")
    assert (doc.width, doc.height, doc.file_name) == (640, 480, "a.png")

    direct = roundtrip(raw.types.WebDocumentNoProxy(
        url="https://example.org/b.mp4", size=2048, mime_type="video/mp4",
        attributes=[raw.types.DocumentAttributeVideo(duration=1.5, w=320, h=240)],
    ))
    doc = types.WebDocument._parse(None, direct)
    assert doc.access_hash is None and doc.file_name is None
    assert (doc.url, doc.size, doc.mime_type, doc.width, doc.height) == ("https://example.org/b.mp4", 2048, "video/mp4", 320, 240)

    assert types.WebDocument._parse(None, None) is None


def test_ton_nft_attribute():
    attribute = types.TonNftAttribute._parse(None, roundtrip(raw.types.wallet.NftAttribute(trait_type="Backdrop", value="Black")))
    assert (attribute.trait_type, attribute.value) == ("Backdrop", "Black")


def test_ton_nft_with_every_field():
    nft = types.TonNft._parse(None, roundtrip(nft_item(
        collection_address="EQcollection", name="Plush Pepe", description="A frog",
        image=web_document("https://example.org/image.png", w=512, h=512),
        image_small=web_document("https://example.org/small.png", w=64, h=64),
        content_url=web_document("https://example.org/content.mp4", mime_type="video/mp4"),
        lottie=web_document("https://example.org/sticker.tgs", mime_type="application/x-tgsticker"),
        attributes=[raw.types.wallet.NftAttribute(trait_type="Model", value="Frog")],
        extra=raw.types.DataJSON(data='{"rarity": 5, "tags": ["a"]}'),
    )))
    assert (nft.address, nft.owner_address, nft.index) == ("EQnft", "UQowner", "42")
    assert (nft.collection_address, nft.name, nft.description) == ("EQcollection", "Plush Pepe", "A frog")
    assert nft.image.url == "https://example.org/image.png" and nft.image.width == 512
    assert nft.thumbnail.url == "https://example.org/small.png" and nft.thumbnail.height == 64
    assert nft.content.mime_type == "video/mp4"
    assert nft.sticker.mime_type == "application/x-tgsticker"
    assert [(a.trait_type, a.value) for a in nft.attributes] == [("Model", "Frog")]
    assert nft.extra == {"rarity": 5, "tags": ["a"]}


def test_ton_nft_with_required_fields_only():
    nft = types.TonNft._parse(None, roundtrip(nft_item()))
    assert nft.address == "EQnft"
    assert nft.collection_address is nft.name is nft.description is None
    assert nft.image is nft.thumbnail is nft.content is nft.sticker is None
    assert nft.attributes is None and nft.extra is None

    broken_extra = types.TonNft._parse(None, roundtrip(nft_item(extra=raw.types.DataJSON(data="{broken"))))
    assert broken_extra.extra is None
    assert types.TonNft._parse(None, None) is None


def test_get_ton_wallet_nfts_follows_next_offset():
    pages = {
        "": raw.types.wallet.NftItems(items=[nft_item("EQ1"), nft_item("EQ2")], next_offset="page2"),
        "page2": raw.types.wallet.NftItems(items=[nft_item("EQ3")]),
    }
    client = offline_client(PEERS, lambda q: pages[q.offset])

    nfts = collect(lambda: client.get_ton_wallet_nfts())

    assert [nft.address for nft in nfts] == ["EQ1", "EQ2", "EQ3"]
    assert all(isinstance(nft, types.TonNft) for nft in nfts)
    assert all(isinstance(q, raw.functions.wallet.GetNfts) for q in client.sent)
    assert [(q.offset, q.limit) for q in client.sent] == [("", 20), ("page2", 20)]


def test_get_ton_wallet_nfts_respects_limit_and_offset():
    client = offline_client(PEERS, lambda q: raw.types.wallet.NftItems(
        items=[nft_item("EQ1"), nft_item("EQ2")], next_offset="more"))

    nfts = collect(lambda: client.get_ton_wallet_nfts(limit=1, offset="start"))

    assert [nft.address for nft in nfts] == ["EQ1"]
    assert [(q.offset, q.limit) for q in client.sent] == [("start", 1)]
