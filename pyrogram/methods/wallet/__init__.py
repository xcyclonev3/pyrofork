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

from .check_wallet_bot_balance import CheckWalletBotBalance
from .create_user_ton_wallet import CreateUserTonWallet
from .delete_ton_wallet import DeleteTonWallet
from .disable_ton_wallet_backup import DisableTonWalletBackup
from .disable_ton_wallet_backup_with_proof import DisableTonWalletBackupWithProof
from .enable_ton_wallet_backup import EnableTonWalletBackup
from .export_ton_wallet_secret_phrase import ExportTonWalletSecretPhrase
from .get_address_ton_wallet import GetAddressTonWallet
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
from .send_ton_wallet_transfer import SendTonWalletTransfer


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
):
    pass
