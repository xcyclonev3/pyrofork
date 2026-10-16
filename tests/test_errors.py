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

import pytest

from pyrogram import errors, raw


def raised(code, message):
    with pytest.raises(errors.RPCError) as e:
        errors.RPCError.raise_it(
            raw.types.RpcError(error_code=code, error_message=message), raw.functions.messages.SendMessage)
    return e.value


@pytest.mark.parametrize("code, message, cls, base", [
    (400, "PEER_ID_INVALID", errors.PeerIdInvalid, errors.BadRequest),
    (406, "PEER_ID_INVALID", errors.PeerIdInvalid406, errors.NotAcceptable),
    (404, "PEER_ID_INVALID", errors.PeerIdInvalid404, errors.NotFound),
    (406, "TOPIC_CLOSED", errors.TopicClosed406, errors.NotAcceptable),
    (403, "VOICE_MESSAGES_FORBIDDEN", errors.VoiceMessagesForbidden403, errors.Forbidden),
    (500, "MSG_WAIT_FAILED", errors.MsgWaitFailed500, errors.InternalServerError),
    (400, "JOIN_GUARD_TIMEOUT", errors.JoinGuardTimeout, errors.BadRequest),
    (400, "INVITE_REQUEST_DECLINED", errors.InviteRequestDeclined, errors.BadRequest),
])
def test_errors_sharing_an_id_keep_their_own_class(code, message, cls, base):
    error = raised(code, message)
    assert type(error) is cls and isinstance(error, base)


def test_exported_classes_follow_the_pypi_import_order():
    # Ids under two codes export the class of the module imported last, as in the 2.3.69 wheel on PyPI
    assert errors.ChannelPrivate.CODE == 400
    assert errors.PeerIdInvalid.CODE == 400
    assert errors.UserRestricted.CODE == 403
