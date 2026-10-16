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

"""Offline tests for the misc Layer 221-230 additions and the official apps' internal APIs (worker E)."""

import pytest

from pyrogram import errors, raw, types, utils
from tests.test_layer_roundtrip import offline_client, call, roundtrip, empty_updates, PEERS

CHANNEL = PEERS[-100123]
BASIC_GROUP = PEERS[-45]
USER = PEERS[7]


def raw_user(user_id=7, **kwargs):
    return raw.types.User(id=user_id, access_hash=2, first_name="A", **kwargs)


def storage_client(respond, peers=PEERS):
    """An offline client with an opened in-memory storage, for methods that cache peers or the session."""
    client = offline_client(peers, respond)
    call(lambda: client.storage.open())
    return client


# get_chat_owner_after_leaving

@pytest.mark.parametrize("chat_id, peer", [(-100123, CHANNEL), (-45, BASIC_GROUP)])
def test_get_chat_owner_after_leaving(chat_id, peer):
    client = storage_client(lambda q: raw_user(8))
    user = call(lambda: client.get_chat_owner_after_leaving(chat_id))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.GetFutureChatCreatorAfterLeave)
    assert request.peer.write() == peer.write()
    assert isinstance(user, types.User) and user.id == 8 and user.first_name == "A"


def test_get_chat_owner_after_leaving_without_candidate():
    client = storage_client(lambda q: raw.types.UserEmpty(id=0))
    assert call(lambda: client.get_chat_owner_after_leaving(-100123)) is None


def test_get_chat_owner_after_leaving_rejects_private_chats():
    client = offline_client(PEERS, lambda q: raw_user())
    with pytest.raises(ValueError):
        call(lambda: client.get_chat_owner_after_leaving(7))
    assert client.sent == []


# get_personal_chat_history

RAW_CHANNEL = raw.types.Channel(id=123, access_hash=1, title="Personal", photo=raw.types.ChatPhotoEmpty(), date=0,
                                broadcast=True)


def channel_post(message_id, text):
    return raw.types.Message(id=message_id, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message=text)


def test_get_personal_chat_history():
    client = offline_client(PEERS, lambda q: raw.types.messages.Messages(
        messages=[channel_post(11, "new"), channel_post(10, "old")], topics=[], chats=[RAW_CHANNEL], users=[]
    ))
    messages = call(lambda: client.get_personal_chat_history(7, limit=2))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.GetPersonalChannelHistory)
    assert request.user_id.write() == USER.write()
    assert (request.limit, request.max_id, request.min_id, request.hash) == (2, (1 << 31) - 1, 0, 0)
    assert [(m.id, m.text) for m in messages] == [(11, "new"), (10, "old")]
    assert all(m.chat.id == utils.get_channel_id(123) and m.chat.title == "Personal" for m in messages)


def test_get_personal_chat_history_rejects_non_positive_limit():
    client = offline_client(PEERS, lambda q: None)
    with pytest.raises(ValueError):
        call(lambda: client.get_personal_chat_history(7, limit=0))


# delete_message_reactions_from_sender / delete_all_recent_message_reactions_from_sender

def test_delete_message_reactions_from_sender():
    client = offline_client(PEERS, lambda q: empty_updates())
    assert call(lambda: client.delete_message_reactions_from_sender(-100123, 42, 7)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.DeleteParticipantReaction)
    assert request.peer.write() == CHANNEL.write() and request.msg_id == 42
    assert request.participant.write() == USER.write()


def test_delete_message_reactions_from_sender_without_reactions():
    def respond(query):
        raise errors.MessageNotModified()

    client = offline_client(PEERS, respond)
    assert call(lambda: client.delete_message_reactions_from_sender(-100123, 42, 7)) is True


def test_delete_all_recent_message_reactions_from_sender():
    client = offline_client(PEERS, lambda q: True)
    assert call(lambda: client.delete_all_recent_message_reactions_from_sender(-45, -100123)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.DeleteParticipantReactions)
    assert request.peer.write() == BASIC_GROUP.write()
    assert request.participant.write() == CHANNEL.write()


# URL authorization: login buttons, external links and OAuth requests

RAW_BOT = raw.types.User(id=99, access_hash=3, first_name="Login", username="loginbot", bot=True, bot_info_version=1)
OAUTH_LINK = "tg://oauth?token=abc"


def url_auth_request(**kwargs):
    return raw.types.UrlAuthResultRequest(bot=RAW_BOT, domain="example.org", **kwargs)


def login_button_client(respond, button_id=3):
    client = offline_client(PEERS, respond)
    client.message_requests = []

    async def get_messages(chat_id, message_ids, replies=1):
        client.message_requests.append((chat_id, message_ids, replies))
        return types.Message(id=message_ids, reply_markup=types.InlineKeyboardMarkup([[
            types.InlineKeyboardButton("cb", callback_data="x"),
            types.InlineKeyboardButton("login", login_url=types.LoginUrl(
                url="https://example.org/login", button_id=button_id
            )),
        ]]))

    client.get_messages = get_messages
    return client


def test_login_url_info_parsing():
    request = roundtrip(url_auth_request(request_write_access=True, request_phone_number=True))
    info = types.LoginUrlInfo._parse(None, request, "https://example.org/login")
    assert isinstance(info, types.LoginUrlInfoRequestConfirmation) and isinstance(info, types.LoginUrlInfo)
    assert (info.url, info.domain) == ("https://example.org/login", "example.org")
    assert info.bot.id == 99 and info.bot.is_bot
    assert info.request_write_access and info.request_phone_number_access

    accepted = types.LoginUrlInfo._parse(None, roundtrip(raw.types.UrlAuthResultAccepted(url="https://a/b")), "x")
    assert isinstance(accepted, types.LoginUrlInfoOpen)
    assert (accepted.url, accepted.skip_confirmation) == ("https://a/b", True)

    default = types.LoginUrlInfo._parse(None, roundtrip(raw.types.UrlAuthResultDefault()), "https://example.org")
    assert isinstance(default, types.LoginUrlInfoOpen)
    assert (default.url, default.skip_confirmation) == ("https://example.org", False)


def test_get_login_url_info():
    client = login_button_client(lambda q: url_auth_request(request_write_access=True))
    info = call(lambda: client.get_login_url_info(-100123, 10, 3))

    assert client.message_requests == [(-100123, 10, 0)]
    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.RequestUrlAuth)
    assert request.peer.write() == CHANNEL.write() and (request.msg_id, request.button_id) == (10, 3)
    assert request.url is None and request.in_app_origin is None
    assert isinstance(info, types.LoginUrlInfoRequestConfirmation)
    assert info.url == "https://example.org/login" and info.request_write_access
    assert not info.request_phone_number_access


def test_get_login_url_info_unknown_button():
    client = login_button_client(lambda q: url_auth_request(), button_id=4)
    with pytest.raises(ValueError):
        call(lambda: client.get_login_url_info(-100123, 10, 3))
    assert client.sent == []


@pytest.mark.parametrize("result, expected", [
    (raw.types.UrlAuthResultAccepted(url="https://example.org/login?hash=1"), "https://example.org/login?hash=1"),
    (raw.types.UrlAuthResultDefault(), "https://example.org/login"),
])
def test_get_login_url(result, expected):
    client = login_button_client(lambda q: result)
    assert call(lambda: client.get_login_url(-100123, 10, 3, allow_write_access=True)) == expected

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.AcceptUrlAuth)
    assert request.peer.write() == CHANNEL.write() and (request.msg_id, request.button_id) == (10, 3)
    assert request.write_allowed and not request.share_phone_number
    assert request.url is None and request.match_code is None


def test_get_external_link_info():
    client = offline_client(PEERS, lambda q: raw.types.UrlAuthResultDefault())
    info = call(lambda: client.get_external_link_info("https://example.telegram.org"))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.RequestUrlAuth)
    assert request.url == "https://example.telegram.org" and request.peer is None
    assert isinstance(info, types.LoginUrlInfoOpen)
    assert (info.url, info.skip_confirmation) == ("https://example.telegram.org", False)


def test_get_external_link():
    client = offline_client(PEERS, lambda q: raw.types.UrlAuthResultAccepted())
    link = "https://example.telegram.org"
    assert call(lambda: client.get_external_link(link, allow_phone_number_access=True)) is None

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.AcceptUrlAuth)
    assert request.url == link and request.share_phone_number and not request.write_allowed
    assert request.peer is None and request.msg_id is None and request.button_id is None


def test_get_oauth_link_info():
    client = offline_client(PEERS, lambda q: url_auth_request(
        request_write_access=True, request_phone_number=True, match_codes_first=True, is_app=True,
        browser="Firefox", platform="Linux", ip="1.2.3.4", region="Somewhere",
        match_codes=["A", "B"], user_id_hint=7, verified_app_name="App"
    ))
    info = call(lambda: client.get_oauth_link_info(OAUTH_LINK, in_app_origin="https://app.example.org"))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.RequestUrlAuth)
    assert request.url == OAUTH_LINK and request.in_app_origin == "https://app.example.org"
    assert request.peer is None
    assert isinstance(info, types.OauthLinkInfo)
    assert (info.url, info.domain, info.bot.id, info.user_id) == (OAUTH_LINK, "example.org", 99, 7)
    assert info.is_from_app and info.verified_app_name == "App"
    assert info.request_write_access and info.request_phone_number_access
    assert (info.browser, info.platform, info.ip_address, info.location) == ("Firefox", "Linux", "1.2.3.4", "Somewhere")
    assert info.match_code_first and info.match_codes == ["A", "B"]


def test_oauth_link_info_minimal_request():
    info = types.OauthLinkInfo._parse(None, roundtrip(url_auth_request()), OAUTH_LINK)
    assert info.user_id is None and info.match_codes is None and info.browser is None
    assert not info.is_from_app and not info.match_code_first


@pytest.mark.parametrize("result", [raw.types.UrlAuthResultDefault(), raw.types.UrlAuthResultAccepted(url="x")])
def test_get_oauth_link_info_expired(result):
    client = offline_client(PEERS, lambda q: result)
    with pytest.raises(errors.UrlExpired):
        call(lambda: client.get_oauth_link_info(OAUTH_LINK))
    assert client.sent[-1].in_app_origin is None


def test_check_oauth_request_match_code():
    client = offline_client(PEERS, lambda q: False)
    assert call(lambda: client.check_oauth_request_match_code(OAUTH_LINK, "A")) is False

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.CheckUrlAuthMatchCode)
    assert (request.url, request.match_code) == (OAUTH_LINK, "A")


def test_accept_oauth_request():
    client = offline_client(PEERS, lambda q: raw.types.UrlAuthResultAccepted(url="https://example.org/done"))
    url = call(lambda: client.accept_oauth_request(
        OAUTH_LINK, match_code="A", allow_write_access=True, allow_phone_number_access=True
    ))
    assert url == "https://example.org/done"

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.AcceptUrlAuth)
    assert (request.url, request.match_code) == (OAUTH_LINK, "A")
    assert request.write_allowed and request.share_phone_number and request.peer is None


def test_accept_oauth_request_unexpected_confirmation():
    client = offline_client(PEERS, lambda q: url_auth_request())
    with pytest.raises(ValueError):
        call(lambda: client.accept_oauth_request(OAUTH_LINK))


def test_decline_oauth_request():
    client = offline_client(PEERS, lambda q: True)
    assert call(lambda: client.decline_oauth_request(OAUTH_LINK)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.DeclineUrlAuth) and request.url == OAUTH_LINK


# get_poll_vote_statistics / StatisticalGraph

@pytest.mark.parametrize("graph, cls, fields", [
    (raw.types.StatsGraph(json=raw.types.DataJSON(data='{"columns": []}'), zoom_token="zoom"),
     types.StatisticalGraphData, {"json_data": '{"columns": []}', "zoom_token": "zoom"}),
    (raw.types.StatsGraph(json=raw.types.DataJSON(data="{}")),
     types.StatisticalGraphData, {"json_data": "{}", "zoom_token": None}),
    (raw.types.StatsGraphAsync(token="async"), types.StatisticalGraphAsync, {"token": "async"}),
    (raw.types.StatsGraphError(error="Not enough data"), types.StatisticalGraphError,
     {"error_message": "Not enough data"}),
])
def test_statistical_graph(graph, cls, fields):
    parsed = types.StatisticalGraph._parse(None, roundtrip(graph))
    assert isinstance(parsed, cls) and isinstance(parsed, types.StatisticalGraph)
    assert {name: getattr(parsed, name) for name in fields} == fields


def test_get_poll_vote_statistics():
    client = offline_client(PEERS, lambda q: raw.types.stats.PollStats(
        votes_graph=raw.types.StatsGraph(json=raw.types.DataJSON(data='{"x": 1}'))
    ))
    statistics = call(lambda: client.get_poll_vote_statistics(-100123, 15, is_dark=True))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.stats.GetPollStats)
    assert request.peer.write() == CHANNEL.write() and request.msg_id == 15 and request.dark
    assert isinstance(statistics, types.PollVoteStatistics)
    assert isinstance(statistics.vote_graph, types.StatisticalGraphData)
    assert statistics.vote_graph.json_data == '{"x": 1}'
