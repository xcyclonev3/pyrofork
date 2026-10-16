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

from .on_bot_business_connect import OnBotBusinessConnect
from .on_bot_business_message import OnBotBusinessMessage
from .on_business_connected_bot import OnBusinessConnectedBot
from .on_callback_query import OnCallbackQuery
from .on_chat_join_request import OnChatJoinRequest
from .on_chat_join_result import OnChatJoinResult
from .on_chat_member_updated import OnChatMemberUpdated
from .on_chosen_inline_result import OnChosenInlineResult
from .on_deleted_ephemeral_messages import OnDeletedEphemeralMessages
from .on_deleted_messages import OnDeletedMessages
from .on_deleted_bot_business_messages import OnDeletedBotBusinessMessages
from .on_disconnect import OnDisconnect
from .on_edited_ephemeral_message import OnEditedEphemeralMessage
from .on_edited_message import OnEditedMessage
from .on_edited_bot_business_message import OnEditedBotBusinessMessage
from .on_ephemeral_message import OnEphemeralMessage
from .on_error import OnError
from .on_guest_query import OnGuestQuery
from .on_inline_query import OnInlineQuery
from .on_managed_bot_updated import OnManagedBotUpdated
from .on_message import OnMessage
from .on_message_draft_stopped import OnMessageDraftStopped
from .on_pending_message import OnPendingMessage
from .on_poll import OnPoll
from .on_pre_checkout_query import OnPreCheckoutQuery
from .on_purchased_paid_media import OnPurchasedPaidMedia
from .on_raw_update import OnRawUpdate
from .on_ton_connect_pending_disconnect import OnTonConnectPendingDisconnect
from .on_ton_connect_session import OnTonConnectSession
from .on_ton_wallet_gasless_transfers_info import OnTonWalletGaslessTransfersInfo
from .on_ton_wallet_state import OnTonWalletState
from .on_user_status import OnUserStatus
from .on_story import OnStory
from .on_message_reaction_updated import OnMessageReactionUpdated
from .on_message_reaction_count_updated import OnMessageReactionCountUpdated
from .on_shipping_query import OnShippingQuery


class Decorators(
    OnBotBusinessConnect,
    OnMessage,
    OnBotBusinessMessage,
    OnEditedMessage,
    OnEditedBotBusinessMessage,
    OnError,
    OnDeletedMessages,
    OnDeletedBotBusinessMessages,
    OnCallbackQuery,
    OnShippingQuery,
    OnPreCheckoutQuery,
    OnRawUpdate,
    OnDisconnect,
    OnUserStatus,
    OnInlineQuery,
    OnPoll,
    OnChosenInlineResult,
    OnChatMemberUpdated,
    OnChatJoinRequest,
    OnStory,
    OnMessageReactionUpdated,
    OnMessageReactionCountUpdated,
    OnPurchasedPaidMedia,
    OnChatJoinResult,
    OnGuestQuery,
    OnManagedBotUpdated,
    OnBusinessConnectedBot,
    OnPendingMessage,
    OnMessageDraftStopped,
    OnTonWalletState,
    OnTonWalletGaslessTransfersInfo,
    OnTonConnectSession,
    OnTonConnectPendingDisconnect,
    OnEphemeralMessage,
    OnEditedEphemeralMessage,
    OnDeletedEphemeralMessages
):
    pass
