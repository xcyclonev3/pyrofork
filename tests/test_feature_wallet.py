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
import importlib
from datetime import datetime

import pytest

import pyrogram
from pyrogram import enums, raw, types
from tests.test_layer_roundtrip import offline_client, call, roundtrip, empty_updates, PEERS

TWT = enums.TonWalletTransactionType
RAW_PEER_USER = raw.types.User(id=7, access_hash=2, first_name="Peer")


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


# Wallet state and transactions

def wallet_state(**kwargs):
    return raw.types.WalletState(address="UQme", public_key=b"\x01" * 32, balance=5_000_000_000, **kwargs)


def wallet_transaction(id="tx1", peer=None, **kwargs):
    return raw.types.WalletTransaction(
        id=id, amount=1_500_000_000, fee=4_000_000, date=1_700_000_000,
        peer=peer or raw.types.WalletTransactionPeerAddress(address="UQpeer"), **kwargs,
    )


def wallet_transactions(*transactions, next_offset=None, users=()):
    return raw.types.wallet.Transactions(
        balance=5_000_000_000, transactions=list(transactions), next_offset=next_offset, chats=[], users=list(users),
    )


def test_ton_wallet_state_parses_wallet_and_empty_state():
    state = types.TonWalletState._parse(None, roundtrip(wallet_state(backup_enabled=True, can_export_phrase=True)))
    assert (state.address, state.public_key, state.balance) == ("UQme", b"\x01" * 32, 5_000_000_000)
    assert state.is_backup_enabled and state.can_export_phrase
    assert not state.can_enable_backup and not state.is_being_created

    empty = types.TonWalletState._parse(None, roundtrip(raw.types.WalletStateEmpty(creating=True)))
    assert empty.is_being_created
    assert empty.address is empty.public_key is empty.balance is None
    assert not (empty.is_backup_enabled or empty.can_enable_backup or empty.can_export_phrase)


def test_get_ton_wallet_state():
    client = offline_client(PEERS, lambda q: wallet_state(can_enable_backup=True))
    state = call(lambda: client.get_ton_wallet_state())

    assert isinstance(client.sent[-1], raw.functions.wallet.GetState)
    assert isinstance(state, types.TonWalletState) and state.address == "UQme" and state.can_enable_backup


def test_ton_wallet_transaction_incoming_transfer_from_user():
    transaction = types.TonWalletTransaction._parse(None, roundtrip(wallet_transaction(
        peer=raw.types.WalletTransactionPeerUser(user_id=7, address="UQpeer", domain="peer.ton"),
        incoming=True, gasless=True, comment="thanks", tx_hash="hash1",
    )), {7: roundtrip(RAW_PEER_USER)})

    assert (transaction.id, transaction.type, transaction.amount) == ("tx1", TWT.TRANSFER, 1_500_000_000)
    assert (transaction.fee_amount, transaction.date) == (4_000_000, datetime.fromtimestamp(1_700_000_000))
    assert not transaction.is_outgoing and transaction.is_gasless and not transaction.is_failed
    assert (transaction.peer_address, transaction.peer_domain) == ("UQpeer", "peer.ton")
    assert isinstance(transaction.peer_user, types.User) and transaction.peer_user.id == 7
    assert (transaction.comment, transaction.tx_hash) == ("thanks", "hash1")
    assert not transaction.is_comment_encrypted and transaction.nft is None
    assert transaction.on_ramp_provider_name is None


@pytest.mark.parametrize("peer, flags, transaction_type, amount", [
    (raw.types.WalletTransactionPeerAddress(address="UQpeer"), {}, TWT.TRANSFER, -1_500_000_000),
    (raw.types.WalletTransactionPeerOnramp(address="UQramp", provider_name="Ramp", domain="ramp.ton"),
     {"incoming": True}, TWT.ON_RAMP_DEPOSIT, 1_500_000_000),
    (raw.types.WalletTransactionPeerUnsupported(), {"key_change": True}, TWT.KEY_CHANGE, -1_500_000_000),
    (raw.types.WalletTransactionPeerAddress(address="UQpeer"),
     {"nft": nft_item("EQgift"), "comment_encrypted": True, "comment": "c2VjcmV0"}, TWT.NFT_TRANSFER, -1_500_000_000),
])
def test_ton_wallet_transaction_types(peer, flags, transaction_type, amount):
    transaction = types.TonWalletTransaction._parse(None, roundtrip(wallet_transaction(peer=peer, failed=True, **flags)))

    assert transaction.type == transaction_type and transaction.amount == amount
    assert transaction.is_outgoing is not flags.get("incoming", False) and transaction.is_failed
    assert transaction.peer_user is None and transaction.peer_address == getattr(peer, "address", None)

    if transaction_type == TWT.ON_RAMP_DEPOSIT:
        assert transaction.on_ramp_provider_name == "Ramp" and transaction.peer_domain == "ramp.ton"
    if transaction_type == TWT.NFT_TRANSFER:
        assert transaction.nft.address == "EQgift" and transaction.is_comment_encrypted
        assert transaction.comment == "c2VjcmV0"


def test_get_ton_wallet_transactions_follows_next_offset():
    pages = {
        "": wallet_transactions(
            wallet_transaction("tx1", peer=raw.types.WalletTransactionPeerUser(user_id=7, address="UQpeer")),
            wallet_transaction("tx2"), next_offset="page2", users=[RAW_PEER_USER],
        ),
        "page2": wallet_transactions(wallet_transaction("tx3")),
    }
    client = offline_client(PEERS, lambda q: pages[q.offset])

    transactions = collect(lambda: client.get_ton_wallet_transactions(is_inbound=True))

    assert [t.id for t in transactions] == ["tx1", "tx2", "tx3"]
    assert transactions[0].peer_user.first_name == "Peer"
    assert all(isinstance(q, raw.functions.wallet.GetTransactions) for q in client.sent)
    assert [(q.offset, q.limit, q.inbound, q.outbound) for q in client.sent] == [
        ("", 100, True, False), ("page2", 100, True, False)
    ]


def test_get_ton_wallet_transactions_respects_limit():
    client = offline_client(PEERS, lambda q: wallet_transactions(
        wallet_transaction("tx1"), wallet_transaction("tx2"), next_offset="more"))

    transactions = collect(lambda: client.get_ton_wallet_transactions(limit=1, offset="o", is_outbound=True))

    assert [t.id for t in transactions] == ["tx1"]
    assert [(q.offset, q.limit, q.inbound, q.outbound) for q in client.sent] == [("o", 1, False, True)]


@pytest.mark.parametrize("method, request_type, field", [
    ("get_ton_wallet_transaction", raw.functions.wallet.GetTransactionsByIDs, "id"),
    ("get_ton_wallet_transaction_by_msg_hash", raw.functions.wallet.GetTransactionsByMsgHash, "msg_hash"),
])
def test_get_ton_wallet_transaction_by_id_or_msg_hash(method, request_type, field):
    def respond(query):
        return wallet_transactions(*[wallet_transaction(i) for i in getattr(query, field) if i != "missing"])

    client = offline_client(PEERS, respond)

    single = call(lambda: getattr(client, method)("tx1"))
    assert isinstance(single, types.TonWalletTransaction) and single.id == "tx1"
    assert isinstance(client.sent[-1], request_type) and getattr(client.sent[-1], field) == ["tx1"]

    assert call(lambda: getattr(client, method)("missing")) is None

    several = call(lambda: getattr(client, method)(["tx1", "missing", "tx2"]))
    assert isinstance(several, list) and [t.id for t in several] == ["tx1", "tx2"]
    assert getattr(client.sent[-1], field) == ["tx1", "missing", "tx2"]


def gasless_info_update(**kwargs):
    return raw.types.UpdateWalletGaslessInfo(
        left=3, reset_at=1_700_000_000, min_amount=10_000_000, relayer_address="UQrelayer", **kwargs)


def test_ton_wallet_gasless_transfers_info():
    info = types.TonWalletGaslessTransfersInfo._parse(None, roundtrip(gasless_info_update(available=True)))
    assert info.is_available and (info.left_count, info.min_amount) == (3, 10_000_000)
    assert info.relayer_address == "UQrelayer" and info.reset_date == datetime.fromtimestamp(1_700_000_000)

    never = types.TonWalletGaslessTransfersInfo._parse(
        None, roundtrip(raw.types.UpdateWalletGaslessInfo(left=0, reset_at=0, min_amount=0, relayer_address="")))
    assert not never.is_available and never.reset_date is None


@pytest.mark.parametrize("result", [
    empty_updates([gasless_info_update(available=True)]),
    raw.types.UpdateShort(update=gasless_info_update(available=True), date=0),
])
def test_get_ton_wallet_gasless_transfers_info(result):
    client = offline_client(PEERS, lambda q: result)
    info = call(lambda: client.get_ton_wallet_gasless_transfers_info())

    assert isinstance(client.sent[-1], raw.functions.wallet.GetGaslessInfo)
    assert isinstance(info, types.TonWalletGaslessTransfersInfo) and info.is_available and info.left_count == 3


def test_get_ton_wallet_gasless_transfers_info_without_update():
    client = offline_client(PEERS, lambda q: empty_updates())
    assert call(lambda: client.get_ton_wallet_gasless_transfers_info()) is None


def test_wallet_bot_balance():
    balance = types.WalletBotBalance._parse(
        None, roundtrip(raw.types.wallet.ExistingBalance(has_balance=True, url="https://t.me/walt?start=x")))
    assert balance.has_balance and balance.url == "https://t.me/walt?start=x"

    empty = types.WalletBotBalance._parse(None, roundtrip(raw.types.wallet.ExistingBalance(url="")))
    assert not empty.has_balance and empty.url is None


def test_check_wallet_bot_balance():
    client = offline_client(PEERS, lambda q: raw.types.wallet.ExistingBalance(has_balance=True, url="https://t.me/walt"))
    balance = call(lambda: client.check_wallet_bot_balance())

    assert isinstance(client.sent[-1], raw.functions.wallet.GetExistingWaltBalance)
    assert isinstance(balance, types.WalletBotBalance) and balance.has_balance and balance.url == "https://t.me/walt"


# Wallet addresses and transfers

USER = PEERS[7]


def user_addresses(*addresses, users=(RAW_PEER_USER,)):
    return raw.types.wallet.UserAddresses(addresses=list(addresses), users=list(users))


def test_user_ton_wallet_address():
    users = {7: roundtrip(RAW_PEER_USER)}
    known = types.UserTonWalletAddress._parse(
        None, roundtrip(raw.types.WalletUserAddress(user_id=7, address="UQpeer", public_key=b"\x02" * 32)), users)
    assert (known.wallet_address, known.public_key, known.user.id) == ("UQpeer", b"\x02" * 32, 7)

    no_owner = roundtrip(raw.types.WalletUserAddress(address="UQx", public_key=b"\x03" * 32))
    assert types.UserTonWalletAddress._parse(None, no_owner, users).user is None
    assert types.UserTonWalletAddress._parse(None, no_owner, users, 7).user.id == 7


def test_get_user_ton_wallet_addresses():
    client = offline_client(PEERS, lambda q: user_addresses(
        raw.types.WalletUserAddress(user_id=7, address="UQpeer", public_key=b"\x02" * 32)))

    address = call(lambda: client.get_user_ton_wallet_addresses(7))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.GetUserAddresses)
    assert [p.write() for p in request.id] == [USER.write()] and request.addresses == [] and not request.force
    assert isinstance(address, types.UserTonWalletAddress) and address.user.first_name == "Peer"

    addresses = call(lambda: client.get_user_ton_wallet_addresses([7, "me"]))
    assert [p.write() for p in client.sent[-1].id] == [USER.write(), PEERS["me"].write()]
    assert isinstance(addresses, list) and [a.wallet_address for a in addresses] == ["UQpeer"]


def test_get_user_ton_wallet_addresses_skips_users_without_wallet():
    client = offline_client(PEERS, lambda q: user_addresses())
    assert call(lambda: client.get_user_ton_wallet_addresses(7)) is None
    assert call(lambda: client.get_user_ton_wallet_addresses([7])) == []


def test_create_user_ton_wallet():
    client = offline_client(PEERS, lambda q: user_addresses(
        raw.types.WalletUserAddress(address="UQnew", public_key=b"\x04" * 32)))

    address = call(lambda: client.create_user_ton_wallet(7))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.GetUserAddresses) and request.force
    assert [p.write() for p in request.id] == [USER.write()] and request.addresses == []
    # The owner is the requested user even when the result doesn't repeat its id, as in TDLib
    assert address.wallet_address == "UQnew" and address.user.id == 7

    empty = offline_client(PEERS, lambda q: user_addresses())
    assert call(lambda: empty.create_user_ton_wallet(7)) is None


def test_get_address_ton_wallet():
    client = offline_client(PEERS, lambda q: user_addresses(
        raw.types.WalletUserAddress(address="UQx", public_key=b"\x05" * 32), users=()))

    wallet = call(lambda: client.get_address_ton_wallet("UQx"))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.GetUserAddresses)
    assert request.id == [] and request.addresses == ["UQx"] and not request.force
    assert (wallet.wallet_address, wallet.public_key, wallet.user) == ("UQx", b"\x05" * 32, None)

    missing = offline_client(PEERS, lambda q: user_addresses(users=()))
    assert call(lambda: missing.get_address_ton_wallet("UQmissing")) is None


def test_ton_wallet_transfer_result():
    sent = types.TonWalletTransferResult._parse(None, roundtrip(raw.types.UpdateSentWalletTransaction(
        msg_hash="hash", gasless=True, transaction=wallet_transaction("tx9"))))
    assert (sent.msg_hash, sent.is_gasless, sent.transaction.id) == ("hash", True, "tx9")
    assert sent.transaction.amount == -1_500_000_000

    pending = types.TonWalletTransferResult._parse(None, roundtrip(raw.types.UpdateSentWalletTransaction(msg_hash="h2")))
    assert (pending.msg_hash, pending.is_gasless, pending.transaction) == ("h2", False, None)


def test_send_ton_wallet_transfer_to_user():
    update = raw.types.UpdateSentWalletTransaction(msg_hash="hash", gasless=True, transaction=wallet_transaction(
        "tx9", peer=raw.types.WalletTransactionPeerUser(user_id=7, address="UQpeer")))
    client = offline_client(PEERS, lambda q: empty_updates([update], users=[RAW_PEER_USER]))

    result = call(lambda: client.send_ton_wallet_transfer(b"regular", b"gasless", user_id=7))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.SendTransfer)
    assert (request.data_normal, request.data_gasless) == (b"regular", b"gasless")
    assert request.user_id.write() == USER.write() and isinstance(request.random_id, int)
    assert isinstance(result, types.TonWalletTransferResult) and result.is_gasless and result.msg_hash == "hash"
    assert result.transaction.peer_user.first_name == "Peer"


def test_send_ton_wallet_transfer_without_receiver():
    client = offline_client(PEERS, lambda q: raw.types.UpdateShort(
        update=raw.types.UpdateSentWalletTransaction(msg_hash="hash"), date=0))

    result = call(lambda: client.send_ton_wallet_transfer(b"regular"))
    request = client.sent[-1]
    assert request.data_gasless is None and isinstance(request.user_id, raw.types.InputUserEmpty)
    assert result.msg_hash == "hash" and not result.is_gasless and result.transaction is None

    nothing = offline_client(PEERS, lambda q: empty_updates())
    assert call(lambda: nothing.send_ton_wallet_transfer(b"regular")) is None


# Ownership proofs, backup, wallet deletion and replacement

PASSWORD_CHECK = raw.types.InputCheckPasswordSRP(srp_id=1, A=b"a", M1=b"m")
PROOF = types.TonWalletOwnershipProof(public_key=b"\x09" * 32, timestamp=1_700_000_000, signature=b"\x08" * 64)


def password_client(monkeypatch, result):
    """An offline client whose 2FA password check is replaced by PASSWORD_CHECK."""
    monkeypatch.setattr(pyrogram.utils, "compute_password_check", lambda r, password: PASSWORD_CHECK)

    def respond(query):
        if isinstance(query, raw.functions.account.GetPassword):
            return raw.types.account.Password(
                new_algo=raw.types.PasswordKdfAlgoUnknown(),
                new_secure_algo=raw.types.SecurePasswordKdfAlgoUnknown(),
                secure_random=b"",
            )
        return result

    return offline_client(PEERS, respond)


def assert_proof(raw_proof):
    assert isinstance(raw_proof, raw.types.WalletOwnershipProof)
    assert (raw_proof.timestamp, raw_proof.signature) == (1_700_000_000, b"\x08" * 64)


def test_ton_wallet_ownership_proof_writes_raw_proof():
    assert_proof(roundtrip(PROOF.write()))

    with pytest.raises(ValueError):
        types.TonWalletOwnershipProof(public_key=b"short", timestamp=1, signature=b"\x08" * 64).write()
    with pytest.raises(ValueError):
        types.TonWalletOwnershipProof(public_key=b"\x09" * 32, timestamp=1, signature=b"short").write()


def test_ton_wallet_ownership_proof_challenge():
    client = offline_client(PEERS, lambda q: raw.types.wallet.ProofChallenge(
        payload="payload", expires=1_700_000_000, domain="wallet.tg"))
    challenge = call(lambda: client.get_ton_wallet_ownership_proof_challenge())

    assert isinstance(client.sent[-1], raw.functions.wallet.GetProofChallenge)
    assert isinstance(challenge, types.TonWalletOwnershipProofChallenge)
    assert (challenge.payload, challenge.domain) == ("payload", "wallet.tg")
    assert challenge.expiration_date == datetime.fromtimestamp(1_700_000_000)


def test_ton_wallet_backup_holder_dcs():
    holder_dc = types.TonWalletBackupHolderDc._parse(None, roundtrip(raw.types.wallet.HolderDc(dc=2, public_key=b"k2")))
    assert (holder_dc.dc_id, holder_dc.public_key) == (2, b"k2")

    client = offline_client(PEERS, lambda q: [
        roundtrip(raw.types.wallet.HolderDc(dc=dc, public_key=b"k%d" % dc)) for dc in (1, 3, 5)
    ])
    dcs = call(lambda: client.get_ton_wallet_backup_holder_dcs())

    assert isinstance(client.sent[-1], raw.functions.wallet.GetBackupHolderDcs)
    assert [(dc.dc_id, dc.public_key) for dc in dcs] == [(1, b"k1"), (3, b"k3"), (5, b"k5")]


def test_enable_ton_wallet_backup():
    client = offline_client(PEERS, lambda q: wallet_state(backup_enabled=True))

    state = call(lambda: client.enable_ton_wallet_backup([b"part1", b"part2", b"part3"], PROOF))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.EnableBackup)
    assert request.parts == [b"part1", b"part2", b"part3"] and request.new_public_key == b"\x09" * 32
    assert_proof(request.proof)
    assert isinstance(state, types.TonWalletState) and state.is_backup_enabled

    call(lambda: client.enable_ton_wallet_backup([b"part1", b"part2", b"part3"]))
    assert client.sent[-1].new_public_key is None and client.sent[-1].proof is None


def test_disable_ton_wallet_backup(monkeypatch):
    client = password_client(monkeypatch, wallet_state())

    state = call(lambda: client.disable_ton_wallet_backup("password"))
    assert isinstance(client.sent[0], raw.functions.account.GetPassword)
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.DisableBackup)
    assert request.password.write() == PASSWORD_CHECK.write()
    assert request.new_public_key is None and request.proof is None
    assert isinstance(state, types.TonWalletState) and not state.is_backup_enabled

    passwordless = offline_client(PEERS, lambda q: wallet_state())
    call(lambda: passwordless.disable_ton_wallet_backup())
    assert len(passwordless.sent) == 1 and passwordless.sent[0].password is None


def test_disable_ton_wallet_backup_with_proof():
    client = offline_client(PEERS, lambda q: wallet_state())

    state = call(lambda: client.disable_ton_wallet_backup_with_proof(PROOF))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.DisableBackup) and request.password is None
    assert request.new_public_key == b"\x09" * 32
    assert_proof(request.proof)
    assert isinstance(state, types.TonWalletState)


def test_delete_ton_wallet(monkeypatch):
    client = password_client(monkeypatch, raw.types.WalletStateEmpty(creating=True))

    state = call(lambda: client.delete_ton_wallet("password"))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.ReplaceWallet)
    assert isinstance(request.wallet, raw.types.InputWalletNew)
    assert request.password.write() == PASSWORD_CHECK.write()
    assert state.is_being_created and state.address is None

    passwordless = offline_client(PEERS, lambda q: wallet_state())
    call(lambda: passwordless.delete_ton_wallet())
    assert len(passwordless.sent) == 1 and passwordless.sent[0].password is None


def test_replace_ton_wallet(monkeypatch):
    client = password_client(monkeypatch, wallet_state())

    state = call(lambda: client.replace_ton_wallet(PROOF, anchor_public_key=b"\x07" * 32, password="password"))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.ReplaceWallet)
    assert isinstance(request.wallet, raw.types.InputWalletImported)
    assert request.wallet.public_key == b"\x09" * 32 and request.wallet.anchor_public_key == b"\x07" * 32
    assert_proof(request.wallet.proof)
    assert request.password.write() == PASSWORD_CHECK.write()
    assert isinstance(state, types.TonWalletState) and state.address == "UQme"

    passwordless = offline_client(PEERS, lambda q: wallet_state())
    call(lambda: passwordless.replace_ton_wallet(PROOF))
    request = passwordless.sent[-1]
    assert request.password is None and request.wallet.anchor_public_key is None


# Secret phrase export

def fake_get_session(monkeypatch, module_name):
    """Replace get_session() of a method module: record the requested DC and use the offline client itself."""
    used_dcs = []

    async def get_session(client, dc_id):
        used_dcs.append(dc_id)
        return client

    monkeypatch.setattr(importlib.import_module(f"pyrogram.methods.wallet.{module_name}"), "get_session", get_session)
    return used_dcs


def test_export_ton_wallet_secret_phrase(monkeypatch):
    client = password_client(monkeypatch, raw.types.wallet.SecretPhraseParts(token="token", dcs=[1, 3, 5]))

    parts = call(lambda: client.export_ton_wallet_secret_phrase("password"))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.ExportSecretPhrase)
    assert request.password.write() == PASSWORD_CHECK.write()
    assert isinstance(parts, types.TonWalletSecretPhraseParts)
    assert (parts.token, parts.dc_ids) == ("token", [1, 3, 5])

    passwordless = offline_client(PEERS, lambda q: raw.types.wallet.SecretPhraseParts(token="t", dcs=[]))
    assert call(lambda: passwordless.export_ton_wallet_secret_phrase()).dc_ids == []
    assert len(passwordless.sent) == 1 and passwordless.sent[0].password is None


def test_get_ton_wallet_encrypted_secret_phrase_part_is_sent_to_the_holder_dc(monkeypatch):
    used_dcs = fake_get_session(monkeypatch, "get_ton_wallet_encrypted_secret_phrase_part")
    client = offline_client(PEERS, lambda q: raw.types.wallet.EncryptedSecretPhrasePart(data=b"\x01" * 288))

    data = call(lambda: client.get_ton_wallet_encrypted_secret_phrase_part("token", b"\x02" * 32, 3))

    assert used_dcs == [3]
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.FetchEncryptedSecretPhrasePart)
    assert (request.token, request.public_key) == ("token", b"\x02" * 32)
    assert data == b"\x01" * 288


# TON Connect

TCS = enums.TonConnectSessionState
DAPP = "ab" * 32


def ton_connect_session(id=11, **kwargs):
    return raw.types.TonConnectSession(id=id, dapp_client_id=DAPP, nonce=b"\x06" * 24, date=1_700_000_000, **kwargs)


def ton_connect_request(msg_id=21, **kwargs):
    return raw.types.TonConnectRequest(session_id=11, msg_id=msg_id, body=b"body", expires=1_700_000_600, **kwargs)


def test_ton_connect_manifest():
    manifest = types.TonConnectManifest._parse(None, roundtrip(raw.types.TonConnectManifest(
        url="https://dapp.example", name="DApp", icon=web_document("https://dapp.example/icon.png"))))
    assert (manifest.url, manifest.name, manifest.icon.url) == ("https://dapp.example", "DApp", "https://dapp.example/icon.png")

    no_icon = types.TonConnectManifest._parse(None, roundtrip(raw.types.TonConnectManifest(url="u", name="n")))
    assert no_icon.icon is None
    assert types.TonConnectManifest._parse(None, None) is None


@pytest.mark.parametrize("flags, state", [
    ({}, TCS.READY),
    ({"pending": True}, TCS.PENDING),
    ({"closing": True}, TCS.CLOSING),
    ({"closed": True}, TCS.CLOSED),
    ({"pending": True, "closed": True}, TCS.PENDING),
])
def test_ton_connect_session_state(flags, state):
    assert types.TonConnectSession._parse(None, roundtrip(ton_connect_session(**flags))).state == state


def test_ton_connect_session_fields():
    session = types.TonConnectSession._parse(None, roundtrip(ton_connect_session(
        client_id="cd" * 32, manifest=raw.types.TonConnectManifest(url="https://dapp.example", name="DApp"))))
    assert (session.id, session.dapp_client_id, session.wallet_client_id) == (11, DAPP, "cd" * 32)
    assert session.creation_date == datetime.fromtimestamp(1_700_000_000) and session.nonce == b"\x06" * 24
    assert session.manifest.name == "DApp" and session.manifest_error is None

    failed = types.TonConnectSession._parse(None, roundtrip(ton_connect_session(manifest_error=3)))
    assert failed.manifest is None and failed.manifest_error == 3 and failed.wallet_client_id is None


def test_ton_connect_request_from_pending_requests():
    request = types.TonConnectRequest._parse(None, roundtrip(ton_connect_request(topic="sendTransaction", trace_id="t1")))
    assert (request.session_id, request.message_id, request.body) == (11, 21, b"body")
    assert request.expiration_date == datetime.fromtimestamp(1_700_000_600)
    assert (request.topic, request.trace_id) == ("sendTransaction", "t1")
    assert request.dapp_name is request.is_accepted is request.is_rejected is None

    never = types.TonConnectRequest._parse(None, roundtrip(raw.types.TonConnectRequest(
        session_id=11, msg_id=22, body=b"", expires=0)))
    assert never.expiration_date is None and never.topic is None and never.trace_id is None


@pytest.mark.parametrize("flags, accepted, rejected", [
    ({}, False, False),
    ({"accepted": True}, True, False),
    ({"declined": True}, False, True),
])
def test_ton_connect_request_from_service_message(flags, accepted, rejected):
    action = roundtrip(raw.types.MessageActionWalletTonConnectRequest(
        session_id=11, expires=1_700_000_600, topic="signData", trace_id="t2", dapp_name="DApp", **flags))
    request = types.TonConnectRequest._parse_action(None, action, 21)

    assert (request.session_id, request.message_id, request.body) == (11, 21, None)
    assert request.expiration_date == datetime.fromtimestamp(1_700_000_600)
    assert (request.topic, request.trace_id, request.dapp_name) == ("signData", "t2", "DApp")
    assert (request.is_accepted, request.is_rejected) == (accepted, rejected)


def test_ton_connect_requests_and_pending_request_methods():
    pending = raw.types.wallet.TonConnectPending(
        session=ton_connect_session(), requests=[ton_connect_request(21), ton_connect_request(22)])
    client = offline_client(PEERS, lambda q: pending)

    by_session = call(lambda: client.get_ton_connect_session_pending_requests(11))
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.TonConnectGetPending)
    assert (request.session_id, request.dapp_client_id) == (11, None)
    assert isinstance(by_session, types.TonConnectRequests) and by_session.session.id == 11
    assert [r.message_id for r in by_session.requests] == [21, 22]

    by_dapp = call(lambda: client.get_ton_connect_dapp_pending_requests(DAPP))
    request = client.sent[-1]
    assert (request.session_id, request.dapp_client_id) == (None, DAPP)
    assert isinstance(by_dapp.requests[0], types.TonConnectRequest)


def test_get_ton_connect_sessions():
    client = offline_client(PEERS, lambda q: raw.types.wallet.TonConnectSessions(
        sessions=[ton_connect_session(1), ton_connect_session(2, closing=True)]))
    sessions = call(lambda: client.get_ton_connect_sessions())

    assert isinstance(client.sent[-1], raw.functions.wallet.TonConnectGetSessions)
    assert [(s.id, s.state) for s in sessions] == [(1, TCS.READY), (2, TCS.CLOSING)]


def test_create_ton_connect_session():
    client = offline_client(PEERS, lambda q: ton_connect_session(pending=True))
    session = call(lambda: client.create_ton_connect_session(DAPP, "https://dapp.example/manifest.json"))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.TonConnectCreateSession)
    assert (request.dapp_client_id, request.manifest_url) == (DAPP, "https://dapp.example/manifest.json")
    assert isinstance(session, types.TonConnectSession) and session.state == TCS.PENDING


def test_set_ton_connect_session_wallet_client_id():
    client = offline_client(PEERS, lambda q: raw.types.wallet.TonConnectChallenge(challenge=b"\x0c" * 104, event_id=5))
    challenge = call(lambda: client.set_ton_connect_session_wallet_client_id(11, "cd" * 32))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.TonConnectRegisterKey)
    assert (request.session_id, request.client_id) == (11, "cd" * 32)
    assert isinstance(challenge, types.TonConnectChallenge)
    assert (challenge.challenge, challenge.event_id) == (b"\x0c" * 104, 5)


def test_send_ton_connect_session_connect_result():
    client = offline_client(PEERS, lambda q: True)

    assert call(lambda: client.send_ton_connect_session_connect_result(11, b"answer", b"body", True, "t1")) is True
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.TonConnectSubmitConnectResult)
    assert (request.session_id, request.challenge_answer, request.body) == (11, b"answer", b"body")
    assert request.error and request.trace_id == "t1"

    call(lambda: client.send_ton_connect_session_connect_result(11, b"answer", b"body"))
    assert not client.sent[-1].error and client.sent[-1].trace_id is None


def test_claim_ton_connect_request():
    client = offline_client(PEERS, lambda q: True)

    assert call(lambda: client.claim_ton_connect_request(11, 21, "req-1")) is True
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.TonConnectClaimRequest)
    assert (request.session_id, request.msg_id, request.app_request_id) == (11, 21, "req-1")
    assert not request.declined and request.challenge_answer is None

    call(lambda: client.claim_ton_connect_request(11, 21, "req-1", is_rejected=True, challenge_answer=b"a"))
    assert client.sent[-1].declined and client.sent[-1].challenge_answer == b"a"


def test_answer_ton_connect_request():
    client = offline_client(PEERS, lambda q: True)

    assert call(lambda: client.answer_ton_connect_request(11, 21, b"response", "t1")) is True
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.TonConnectSubmitResponse)
    assert (request.session_id, request.msg_id, request.body, request.trace_id) == (11, 21, b"response", "t1")

    call(lambda: client.answer_ton_connect_request(11, 21, b"response"))
    assert client.sent[-1].trace_id is None


def test_get_ton_connect_session_next_event_id():
    client = offline_client(PEERS, lambda q: raw.types.TonConnectNextEventId(event_id=42))

    assert call(lambda: client.get_ton_connect_session_next_event_id(11)) == 42
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.TonConnectNextEventId) and request.session_id == 11


def test_disconnect_ton_connect_session():
    client = offline_client(PEERS, lambda q: True)

    assert call(lambda: client.disconnect_ton_connect_session(11, b"event")) is True
    request = client.sent[-1]
    assert isinstance(request, raw.functions.wallet.TonConnectCloseSession)
    assert (request.session_id, request.body) == (11, b"event")

    call(lambda: client.disconnect_ton_connect_session(11))
    assert client.sent[-1].body is None
