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

import asyncio

import pytest

import pyrogram
from pyrogram import enums, raw, types, utils
from tests.test_layer_roundtrip import (
    CHANNEL, PEERS, RAW_BOT, RAW_CHANNEL, RAW_USER, USER, call, empty_updates, offline_client, roundtrip
)

COMMUNITY = raw.types.Community(
    id=900, access_hash=9, title="Club", photo=raw.types.ChatPhotoEmpty(), date=0, creator=True,
    collapsed_in_dialogs=True, default_banned_rights=raw.types.ChatBannedRights(until_date=0, manage_linked_peers=True)
)
INPUT_COMMUNITY = raw.types.InputPeerChannel(channel_id=900, access_hash=9)
COMMUNITY_ID = utils.get_channel_id(900)
CHANNEL_ID = utils.get_channel_id(123)
COMMUNITY_PEERS = {**PEERS, COMMUNITY_ID: INPUT_COMMUNITY, 99: raw.types.InputPeerUser(user_id=99, access_hash=3)}


def collect(agen):
    async def main():
        return [item async for item in agen]
    return main()


def test_community_chat_parsing():
    chat = types.Chat._parse_chat(None, roundtrip(COMMUNITY))

    assert chat.type == enums.ChatType.COMMUNITY and chat.id == COMMUNITY_ID and chat.title == "Club"
    assert chat.is_creator is True and chat.is_collapsed is True
    assert chat.permissions.can_edit_chat_list is False and chat.permissions.can_send_messages is True
    # Code paths that expect a channel (dialogs, send-as chats, ...) hand communities over too
    assert types.Chat._parse_channel_chat(None, roundtrip(COMMUNITY)).type == enums.ChatType.COMMUNITY

    forbidden = types.Chat._parse_chat(None, roundtrip(raw.types.CommunityForbidden(id=901, title="Gone")))
    assert forbidden.type == enums.ChatType.COMMUNITY and forbidden.id == utils.get_channel_id(901) and forbidden.title == "Gone"


def test_chats_belonging_to_a_community():
    channel = roundtrip(raw.types.Channel(
        id=123, access_hash=1, title="Test", photo=raw.types.ChatPhotoEmpty(), date=0, linked_community_id=900))
    bot = roundtrip(raw.types.User(id=99, bot=True, bot_info_version=1, first_name="Guard", access_hash=3,
                                   linked_community_id=900))

    assert types.Chat._parse_chat(None, channel).community_id == COMMUNITY_ID
    assert types.Chat._parse_chat(None, bot).community_id == COMMUNITY_ID
    assert types.User._parse(None, bot).community_id == COMMUNITY_ID
    assert types.Chat._parse_chat(None, roundtrip(RAW_CHANNEL)).community_id is None


def test_fetch_peers_stores_communities():
    async def main():
        client = pyrogram.Client("communities", api_id=1, api_hash="x", in_memory=True)
        await client.storage.open()
        await client.fetch_peers([roundtrip(COMMUNITY), roundtrip(raw.types.CommunityForbidden(id=901, title="Gone"))])
        stored = await client.storage.get_peer_by_id(COMMUNITY_ID)
        with pytest.raises(KeyError):
            await client.storage.get_peer_by_id(utils.get_channel_id(901))  # no access hash: nothing to store
        await client.storage.close()
        return stored

    assert asyncio.run(main()) == INPUT_COMMUNITY


def community_full(**kwargs):
    return raw.types.messages.ChatFull(
        full_chat=raw.types.CommunityFull(
            id=900, about="About the club", chat_photo=raw.types.PhotoEmpty(id=0),
            linked_peers=[
                raw.types.CommunityPeer(peer=raw.types.PeerChannel(channel_id=123), can_view_history=True,
                                        visible=False),
                raw.types.CommunityPeer(peer=raw.types.PeerUser(user_id=99), visible=True),
                raw.types.CommunityPeer(peer=raw.types.PeerChannel(channel_id=555)),  # missing from chats
            ],
            **kwargs
        ),
        chats=[COMMUNITY, RAW_CHANNEL],
        users=[RAW_BOT]
    )


def test_get_chat_of_a_community():
    client = offline_client(COMMUNITY_PEERS, lambda q: community_full(
        admins_count=2, kicked_count=1, peer_link_requests_pending=3))
    chat = call(lambda: client.get_chat(COMMUNITY_ID))

    [request] = client.sent
    assert isinstance(request, raw.functions.channels.GetFullChannel) and request.channel == INPUT_COMMUNITY
    assert chat.type == enums.ChatType.COMMUNITY and chat.description == "About the club"
    assert (chat.administrators_count, chat.banned_members_count, chat.chat_requests_count) == (2, 1, 3)

    channel, bot = chat.community_chats
    assert channel.chat.title == "Test" and channel.can_view_history is True and channel.is_hidden is True
    assert bot.chat.type == enums.ChatType.BOT and bot.can_view_history is False and bot.is_hidden is False


def test_get_chat_of_a_community_as_member():
    client = offline_client(COMMUNITY_PEERS, lambda q: community_full())
    chat = call(lambda: client.get_chat(COMMUNITY_ID))

    assert (chat.administrators_count, chat.banned_members_count, chat.chat_requests_count) == (None, None, None)
    assert [c.is_hidden for c in chat.community_chats] == [True, False]


@pytest.mark.parametrize("kwargs, hidden, about", [({}, False, None), (dict(is_chat_hidden=True, description="d"), True, "d")])
def test_create_community(kwargs, hidden, about):
    client = offline_client(COMMUNITY_PEERS, lambda q: empty_updates(chats=[RAW_CHANNEL, COMMUNITY]))
    community = call(lambda: client.create_community("Club", -100123, **kwargs))

    [request] = client.sent
    assert isinstance(request, raw.functions.communities.Create)
    assert (request.title, request.peer, request.hidden, request.about) == ("Club", CHANNEL, hidden, about)
    assert community.type == enums.ChatType.COMMUNITY and community.id == COMMUNITY_ID


def test_get_joined_communities():
    forbidden = raw.types.CommunityForbidden(id=901, title="Gone")
    client = offline_client(COMMUNITY_PEERS, lambda q: raw.types.messages.Chats(chats=[COMMUNITY, RAW_CHANNEL, forbidden]))
    communities = call(lambda: client.get_joined_communities())

    assert isinstance(client.sent[-1], raw.functions.communities.GetJoinedCommunities)
    assert [(c.id, c.type) for c in communities] == [(COMMUNITY_ID, enums.ChatType.COMMUNITY)]


@pytest.mark.parametrize("call_method, visible, hidden, deleted", [
    (lambda c: c.add_chat_to_community(COMMUNITY_ID, -100123), True, False, False),
    (lambda c: c.add_chat_to_community(COMMUNITY_ID, -100123, is_hidden=True), False, True, False),
    (lambda c: c.remove_chat_from_community(COMMUNITY_ID, -100123), False, False, True),
])
def test_toggle_community_chats(call_method, visible, hidden, deleted):
    client = offline_client(COMMUNITY_PEERS, lambda q: True)
    assert call(lambda: call_method(client)) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.communities.TogglePeerLink)
    assert request.community == INPUT_COMMUNITY and request.peer == CHANNEL
    assert (request.visible, request.hidden, request.deleted) == (visible, hidden, deleted)


@pytest.mark.parametrize("args, collapsed", [((), True), ((False,), False)])
def test_set_community_collapsed(args, collapsed):
    client = offline_client(COMMUNITY_PEERS, lambda q: empty_updates())
    assert call(lambda: client.set_community_collapsed(COMMUNITY_ID, *args)) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.communities.ToggleCommunityCollapsedInDialogs)
    assert request.community == INPUT_COMMUNITY and request.collapsed is collapsed


def peer_link_requests(query):
    first = query.offset == ""
    return raw.types.communities.PeerLinkRequests(
        total_count=2,
        requests=[raw.types.CommunityPeerRequest(
            peer=raw.types.PeerChannel(channel_id=123) if first else raw.types.PeerUser(user_id=99),
            requested_by=7, date=1700000000, visible=first
        )],
        next_offset="next" if first else None,
        chats=[RAW_CHANNEL],
        users=[RAW_USER, RAW_BOT]
    )


def test_get_community_chat_requests():
    client = offline_client(COMMUNITY_PEERS, peer_link_requests)
    channel, bot = call(lambda: collect(client.get_community_chat_requests(COMMUNITY_ID)))

    assert [(r.community, r.offset, r.limit) for r in client.sent] == [(INPUT_COMMUNITY, "", 100), (INPUT_COMMUNITY, "next", 100)]
    assert channel.chat.title == "Test" and channel.requested_by.id == 7 and channel.is_hidden is False
    assert channel.date.timestamp() == 1700000000
    assert bot.chat.type == enums.ChatType.BOT and bot.is_hidden is True


def test_get_community_chat_requests_limit():
    client = offline_client(COMMUNITY_PEERS, peer_link_requests)
    requests = call(lambda: collect(client.get_community_chat_requests(COMMUNITY_ID, limit=1)))

    assert len(requests) == 1 and len(client.sent) == 1 and client.sent[0].limit == 1


@pytest.mark.parametrize("call_method, request_type, reject", [
    (lambda c: c.approve_community_chat_request(COMMUNITY_ID, -100123), raw.functions.communities.TogglePeerLinkRequestApproval, False),
    (lambda c: c.decline_community_chat_request(COMMUNITY_ID, -100123), raw.functions.communities.TogglePeerLinkRequestApproval, True),
    (lambda c: c.approve_all_community_chat_requests(COMMUNITY_ID), raw.functions.communities.ToggleAllPeerLinkRequestApproval, False),
    (lambda c: c.decline_all_community_chat_requests(COMMUNITY_ID), raw.functions.communities.ToggleAllPeerLinkRequestApproval, True),
])
def test_process_community_chat_requests(call_method, request_type, reject):
    client = offline_client(COMMUNITY_PEERS, lambda q: True)
    assert call(lambda: call_method(client)) is True

    [request] = client.sent
    assert isinstance(request, request_type) and request.community == INPUT_COMMUNITY and request.reject is reject
    if request_type is raw.functions.communities.TogglePeerLinkRequestApproval:
        assert request.peer == CHANNEL


@pytest.mark.parametrize("method, unban", [("ban_community_member", False), ("unban_community_member", True)])
def test_ban_community_member(method, unban):
    client = offline_client(COMMUNITY_PEERS, lambda q: True)
    assert call(lambda: getattr(client, method)(COMMUNITY_ID, 7)) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.communities.ToggleParticipantBanned)
    assert request.community == INPUT_COMMUNITY and request.participant == USER and request.unban is unban


def test_get_community_member_chats():
    client = offline_client(COMMUNITY_PEERS, lambda q: raw.types.communities.ParticipantJoinedChats(
        creator_chat_ids=[123], joined_chat_ids=[123, 555], chats=[RAW_CHANNEL], users=[]))
    chats = call(lambda: client.get_community_member_chats(COMMUNITY_ID, 7))

    [request] = client.sent
    assert isinstance(request, raw.functions.communities.GetParticipantJoinedChats)
    assert request.community == INPUT_COMMUNITY and request.participant == USER
    assert [c.id for c in chats.owned_chats] == [CHANNEL_ID] and [c.id for c in chats.joined_chats] == [CHANNEL_ID]


def parse_service(action, chats):
    client = pyrogram.Client("communities", api_id=1, api_hash="x", in_memory=True)
    client.me = None
    message = roundtrip(raw.types.MessageService(
        id=1, peer_id=raw.types.PeerChannel(channel_id=123), from_id=raw.types.PeerUser(user_id=7), date=0,
        action=action))
    chats = {c.id: c for c in (roundtrip(c) for c in chats)}
    return asyncio.run(types.Message._parse(client, message, {7: roundtrip(RAW_USER)}, chats, replies=0))


def test_chat_added_to_community_message():
    message = parse_service(raw.types.MessageActionChangeCommunity(community_id=900), [RAW_CHANNEL, COMMUNITY])
    assert message.service == enums.MessageServiceType.CHAT_ADDED_TO_COMMUNITY
    assert message.chat_added_to_community.title == "Club" and message.chat_added_to_community.id == COMMUNITY_ID

    # The community is not always sent along with the message
    message = parse_service(raw.types.MessageActionChangeCommunity(community_id=900), [RAW_CHANNEL])
    community = message.chat_added_to_community
    assert community.id == COMMUNITY_ID and community.type == enums.ChatType.COMMUNITY and community.title is None


def test_chat_removed_from_community_message():
    message = parse_service(raw.types.MessageActionChangeCommunity(), [RAW_CHANNEL])
    assert message.service == enums.MessageServiceType.CHAT_REMOVED_FROM_COMMUNITY
    assert message.chat_removed_from_community is True and message.chat_added_to_community is None


def test_chat_joined_from_community_message():
    message = parse_service(raw.types.MessageActionChatJoinedViaCommunity(community_id=900), [RAW_CHANNEL, COMMUNITY])
    assert message.service == enums.MessageServiceType.NEW_CHAT_MEMBERS
    assert message.chat_join_type == enums.ChatJoinType.BY_COMMUNITY
    assert [u.id for u in message.new_chat_members] == [7]
    assert message.chat_joined_from_community.title == "Club"


@pytest.mark.parametrize("linked_peers, expected", [(True, False), (None, True)])
def test_community_rights(linked_peers, expected):
    rights = roundtrip(raw.types.ChatBannedRights(until_date=0, manage_linked_peers=linked_peers))
    assert types.ChatPermissions._parse(rights).can_edit_chat_list is expected

    admin_rights = roundtrip(raw.types.ChatAdminRights(manage_linked_peers=linked_peers))
    assert types.ChatPrivileges._parse(admin_rights).can_edit_chat_list is not expected


def test_community_right_does_not_change_all_perms():
    everything = dict.fromkeys([
        "send_messages", "send_media", "embed_links", "send_polls", "change_info", "invite_users", "pin_messages",
        "send_audios", "send_docs", "send_games", "send_gifs", "send_inline", "send_photos", "send_plain",
        "send_roundvideos", "send_stickers", "send_videos", "send_voices", "send_reactions", "edit_rank"
    ], True)
    rights = roundtrip(raw.types.ChatBannedRights(until_date=0, **everything))
    assert types.ChatPermissions._parse(rights).all_perms is False


def test_promote_chat_member_sends_can_edit_chat_list():
    client = offline_client(COMMUNITY_PEERS, lambda q: empty_updates())
    privileges = types.ChatPrivileges(can_edit_chat_list=True)
    assert call(lambda: client.promote_chat_member(COMMUNITY_ID, 7, privileges=privileges)) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.channels.EditAdmin) and request.admin_rights.manage_linked_peers is True


def test_set_chat_permissions_of_a_community():
    community = raw.types.Community(id=900, access_hash=9, title="Club", photo=raw.types.ChatPhotoEmpty(), date=0)

    def respond(query):
        if isinstance(query, raw.functions.channels.GetFullChannel):
            full = community_full()
            full.chats = [community]  # no default_banned_rights: nothing is banned yet
            return full
        return empty_updates(chats=[community])

    client = offline_client(COMMUNITY_PEERS, respond)
    chat = call(lambda: client.set_chat_permissions(COMMUNITY_ID, types.ChatPermissions(can_edit_chat_list=False)))

    get_full, edit = client.sent
    assert isinstance(edit, raw.functions.messages.EditChatDefaultBannedRights) and edit.peer == INPUT_COMMUNITY
    assert edit.banned_rights.manage_linked_peers is True
    assert not any([edit.banned_rights.send_messages, edit.banned_rights.send_plain, edit.banned_rights.send_media,
                    edit.banned_rights.invite_users, edit.banned_rights.change_info, edit.banned_rights.edit_rank])
    assert chat.type == enums.ChatType.COMMUNITY
