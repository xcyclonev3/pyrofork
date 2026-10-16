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

from .answer_ton_connect_request import AnswerTonConnectRequest
from .check_wallet_bot_balance import CheckWalletBotBalance
from .claim_ton_connect_request import ClaimTonConnectRequest
from .create_on_ramp_payment_session import CreateOnRampPaymentSession
from .create_ton_connect_session import CreateTonConnectSession
from .create_user_ton_wallet import CreateUserTonWallet
from .delete_ton_wallet import DeleteTonWallet
from .disable_ton_wallet_backup import DisableTonWalletBackup
from .disable_ton_wallet_backup_with_proof import DisableTonWalletBackupWithProof
from .disconnect_ton_connect_session import DisconnectTonConnectSession
from .enable_ton_wallet_backup import EnableTonWalletBackup
from .export_ton_wallet_secret_phrase import ExportTonWalletSecretPhrase
from .get_address_ton_wallet import GetAddressTonWallet
from .get_on_ramp_payment_availability import GetOnRampPaymentAvailability
from .get_on_ramp_payment_limits import GetOnRampPaymentLimits
from .get_on_ramp_payment_quote import GetOnRampPaymentQuote
from .get_on_ramp_provider_base_currencies import GetOnRampProviderBaseCurrencies
from .get_on_ramp_providers import GetOnRampProviders
from .get_ton_connect_dapp_pending_requests import GetTonConnectDappPendingRequests
from .get_ton_connect_session_next_event_id import GetTonConnectSessionNextEventId
from .get_ton_connect_session_pending_requests import GetTonConnectSessionPendingRequests
from .get_ton_connect_sessions import GetTonConnectSessions
from .get_ton_wallet_backup_holder_dcs import GetTonWalletBackupHolderDcs
from .get_ton_wallet_encrypted_secret_phrase_part import GetTonWalletEncryptedSecretPhrasePart
from .get_ton_wallet_gasless_transfers_info import GetTonWalletGaslessTransfersInfo
from .get_ton_wallet_nfts import GetTonWalletNfts
from .get_ton_wallet_ownership_proof_challenge import GetTonWalletOwnershipProofChallenge
from .get_ton_wallet_state import GetTonWalletState
from .get_ton_wallet_transaction import GetTonWalletTransaction
from .get_ton_wallet_transaction_by_msg_hash import GetTonWalletTransactionByMsgHash
from .get_ton_wallet_transactions import GetTonWalletTransactions
from .get_user_ton_wallet_addresses import GetUserTonWalletAddresses
from .replace_ton_wallet import ReplaceTonWallet
from .send_ton_connect_session_connect_result import SendTonConnectSessionConnectResult
from .send_ton_wallet_transfer import SendTonWalletTransfer
from .set_ton_connect_session_wallet_client_id import SetTonConnectSessionWalletClientId


class Wallet(
    GetTonWalletNfts,
    GetTonWalletState,
    GetTonWalletTransactions,
    GetTonWalletTransaction,
    GetTonWalletTransactionByMsgHash,
    GetTonWalletGaslessTransfersInfo,
    CheckWalletBotBalance,
    GetUserTonWalletAddresses,
    CreateUserTonWallet,
    GetAddressTonWallet,
    SendTonWalletTransfer,
    GetTonWalletOwnershipProofChallenge,
    GetTonWalletBackupHolderDcs,
    EnableTonWalletBackup,
    DisableTonWalletBackup,
    DisableTonWalletBackupWithProof,
    DeleteTonWallet,
    ReplaceTonWallet,
    ExportTonWalletSecretPhrase,
    GetTonWalletEncryptedSecretPhrasePart,
    GetTonConnectSessions,
    CreateTonConnectSession,
    SetTonConnectSessionWalletClientId,
    SendTonConnectSessionConnectResult,
    GetTonConnectSessionPendingRequests,
    GetTonConnectDappPendingRequests,
    ClaimTonConnectRequest,
    AnswerTonConnectRequest,
    GetTonConnectSessionNextEventId,
    DisconnectTonConnectSession,
    GetOnRampProviders,
    GetOnRampProviderBaseCurrencies,
    GetOnRampPaymentAvailability,
    GetOnRampPaymentLimits,
    GetOnRampPaymentQuote,
    CreateOnRampPaymentSession,
):
    pass
