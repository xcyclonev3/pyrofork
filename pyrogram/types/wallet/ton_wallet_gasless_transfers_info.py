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

from datetime import datetime
from typing import Optional

import pyrogram
from pyrogram import raw, utils
from ..object import Object


class TonWalletGaslessTransfersInfo(Object):
    """State of the gasless transfers of the current user, whose fee is paid by Telegram.

    Parameters:
        is_available (``bool``):
            True, if gasless transfers are available to the current user.

        left_count (``int``):
            Number of gasless transfers left.

        min_amount (``int``):
            Minimum amount of a gasless transfer, in the smallest units of the cryptocurrency.

        relayer_address (``str``):
            Address of the relayer of gasless transfers.

        reset_date (:py:obj:`~datetime.datetime`, *optional*):
            Date the number of left gasless transfers will be reset. None, if it will never be reset.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        is_available: bool,
        left_count: int,
        min_amount: int,
        relayer_address: str,
        reset_date: Optional[datetime] = None
    ):
        super().__init__(client)

        self.is_available = is_available
        self.left_count = left_count
        self.min_amount = min_amount
        self.relayer_address = relayer_address
        self.reset_date = reset_date

    @staticmethod
    def _parse(client, info: "raw.types.UpdateWalletGaslessInfo") -> "TonWalletGaslessTransfersInfo":
        return TonWalletGaslessTransfersInfo(
            is_available=info.available,
            # Like TDLib: no transfers are left and there is no relayer while the service isn't available
            left_count=info.left if info.available else 0,
            min_amount=info.min_amount,
            relayer_address=(info.relayer_address or None) if info.available else None,
            reset_date=utils.timestamp_to_datetime(info.reset_at),
            client=client
        )
