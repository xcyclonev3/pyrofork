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

from enum import auto

from .auto_name import AutoName


class TonWalletTransactionType(AutoName):
    """Type of a transaction of a TON wallet, used in :obj:`~pyrogram.types.TonWalletTransaction`."""

    TRANSFER = auto()
    "A transfer of cryptocurrency to or from the wallet"

    NFT_TRANSFER = auto()
    "A transfer of an NFT to or from the wallet"

    ON_RAMP_DEPOSIT = auto()
    "A deposit of cryptocurrency bought from an on-ramp provider"

    KEY_CHANGE = auto()
    "A change of the key of the wallet"
