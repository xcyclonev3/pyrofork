#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
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

from .accept_oauth_request import AcceptOauthRequest
from .check_oauth_request_match_code import CheckOauthRequestMatchCode
from .check_password import CheckPassword
from .connect import Connect
from .decline_oauth_request import DeclineOauthRequest
from .disconnect import Disconnect
from .get_active_sessions import GetActiveSessions
from .get_external_link import GetExternalLink
from .get_external_link_info import GetExternalLinkInfo
from .get_oauth_link_info import GetOauthLinkInfo
from .get_password_hint import GetPasswordHint
from .initialize import Initialize
from .log_out import LogOut
from .recover_password import RecoverPassword
from .resend_code import ResendCode
from .send_code import SendCode
from .send_recovery_code import SendRecoveryCode
from .sign_in import SignIn
from .sign_in_bot import SignInBot
from .sign_in_qrcode import SignInQrcode
from .terminate import Terminate


class Auth(
    CheckPassword,
    Connect,
    Disconnect,
    GetActiveSessions,
    GetPasswordHint,
    Initialize,
    LogOut,
    RecoverPassword,
    ResendCode,
    SendCode,
    SendRecoveryCode,
    SignIn,
    SignInBot,
    SignInQrcode,
    Terminate,
    GetExternalLinkInfo,
    GetExternalLink,
    GetOauthLinkInfo,
    CheckOauthRequestMatchCode,
    AcceptOauthRequest,
    DeclineOauthRequest
):
    pass
