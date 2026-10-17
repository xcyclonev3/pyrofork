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
from types import SimpleNamespace

from pyrogram import enums, raw, types
from tests.test_layer_roundtrip import (
    PEERS, RAW_CHANNEL, RAW_USER, call, empty_updates, offline_client, parse_message, roundtrip
)


def test_set_chat_member_tag():
    client = offline_client(PEERS, lambda q: empty_updates())
    assert call(lambda: client.set_chat_member_tag(-100123, 7, "Designer")) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.EditChatParticipantRank)
    assert request.rank == "Designer" and request.participant.user_id == 7


def test_new_admin_privileges():
    privileges = types.ChatPrivileges._parse(roundtrip(raw.types.ChatAdminRights(
        manage_ranks=True, manage_welcome_messages=True, manage_direct_messages=True)))
    assert privileges.can_manage_tags and privileges.can_send_welcome_messages

    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.promote_chat_member(-100123, 7, types.ChatPrivileges(
        can_manage_tags=True, can_send_welcome_messages=True, can_manage_direct_messages=True)))
    rights = client.sent[-1].admin_rights
    assert rights.manage_ranks and rights.manage_welcome_messages and rights.manage_direct_messages


def test_new_member_permissions_are_kept():
    permissions = types.ChatPermissions._parse(roundtrip(raw.types.ChatBannedRights(
        until_date=0, send_reactions=True, edit_rank=True)))
    assert permissions.can_react_to_messages is False and permissions.can_edit_tag is False

    client = offline_client(PEERS, lambda q: empty_updates(chats=[RAW_CHANNEL]))

    async def get_chat(chat_id):
        return SimpleNamespace(permissions=permissions)

    client.get_chat = get_chat
    call(lambda: client.set_chat_permissions(-100123, types.ChatPermissions(can_send_polls=True)))
    rights = client.sent[-1].banned_rights
    # Not mentioned in the new permissions: kept as they were instead of being reset
    assert rights.send_reactions and rights.edit_rank and not rights.send_polls

    call(lambda: client.restrict_chat_member(-100123, 7, types.ChatPermissions(can_react_to_messages=True)))
    rights = client.sent[-1].banned_rights
    assert not rights.send_reactions and rights.edit_rank


def test_restrict_chat_member_without_known_chat_permissions():
    client = offline_client(PEERS, lambda q: empty_updates(chats=[RAW_CHANNEL]))

    async def get_chat(chat_id):
        return SimpleNamespace(permissions=None)

    client.get_chat = get_chat
    call(lambda: client.restrict_chat_member(-100123, 7, types.ChatPermissions(can_send_polls=False)))
    rights = client.sent[-1].banned_rights
    # The permissions that are not passed fall back to allowed, as in set_chat_permissions
    assert rights.send_polls and not rights.send_reactions and not rights.edit_rank and not rights.send_photos


def test_completely_restricted_members_cant_react_or_change_tags():
    client = offline_client(PEERS, lambda q: empty_updates(chats=[RAW_CHANNEL]))
    call(lambda: client.restrict_chat_member(-100123, 7, types.ChatPermissions()))
    rights = client.sent[-1].banned_rights
    assert rights.send_messages and rights.send_media and rights.send_reactions and rights.edit_rank


def test_member_tag_chat_event_and_filter():
    event = roundtrip(raw.types.ChannelAdminLogEvent(
        id=1, date=1, user_id=7,
        action=raw.types.ChannelAdminLogEventActionParticipantEditRank(user_id=7, prev_rank="old", new_rank="new")
    ))
    parsed = asyncio.run(types.ChatEvent._parse(None, event, [roundtrip(RAW_USER)], []))
    assert parsed.action == enums.ChatEventAction.MEMBER_TAG_CHANGED
    assert (parsed.member_tag_user.id, parsed.old_member_tag, parsed.new_member_tag) == (7, "old", "new")

    assert types.ChatEventFilter(member_tags=True).write().edit_rank
    assert not types.ChatEventFilter(video_chats=True).write().edit_rank


def test_message_sender_tag_and_summary_language():
    message = parse_message(raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7), date=1, message="hi",
        from_rank="Designer", summary_from_language="en"
    ), users=[RAW_USER])
    assert message.sender_tag == "Designer" and message.summary_language_code == "en"
