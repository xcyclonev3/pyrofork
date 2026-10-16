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

from typing import List, Optional

import pyrogram
from pyrogram import raw
from ..object import Object


class StakeDiceState(Object):
    """Describes the state of the stake dice.

    Parameters:
        state_hash (``str``, *optional*):
            Hash of the state to use for sending the next dice with :meth:`~pyrogram.Client.send_stake_dice`.
            The hash can be used only if it was received recently enough, otherwise a new state must be requested
            with :meth:`~pyrogram.Client.get_stake_dice_state`.
            None if the stake dice can't be sent by the current user.

        stake_gram_amount (``int``, *optional*):
            The amount of TON Grams staked in the previous roll; in the smallest units of the currency.

        current_streak (``int``, *optional*):
            The number of rolled sixes towards the streak; 0-2.

        prize_per_mille (List of ``int``, *optional*):
            The number of Grams received by the user for each 1000 Grams staked if the dice outcome is 1-6
            correspondingly.

        streak_prize_per_mille (``int``, *optional*):
            The number of Grams received by the user for each 1000 Grams staked if the dice outcome is 6 three
            times in a row with the same stake.

        plays_left (``int``, *optional*):
            The number of plays left.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        state_hash: Optional[str] = None,
        stake_gram_amount: Optional[int] = None,
        current_streak: Optional[int] = None,
        prize_per_mille: Optional[List[int]] = None,
        streak_prize_per_mille: Optional[int] = None,
        plays_left: Optional[int] = None
    ):
        super().__init__(client)

        self.state_hash = state_hash
        self.stake_gram_amount = stake_gram_amount
        self.current_streak = current_streak
        self.prize_per_mille = prize_per_mille
        self.streak_prize_per_mille = streak_prize_per_mille
        self.plays_left = plays_left

    @staticmethod
    def _parse(client, game_info: "raw.base.messages.EmojiGameInfo") -> "StakeDiceState":
        # Like TDLib, messages.emojiGameUnavailable is a state without hash: the stake dice can't be sent
        if not isinstance(game_info, raw.types.messages.EmojiGameDiceInfo):
            return StakeDiceState(client=client)

        # The 7 parameters are the prizes for the outcomes 1-6 and for the streak (see TDLib's EmojiGameInfo.cpp)
        params = game_info.params

        return StakeDiceState(
            state_hash=game_info.game_hash or None,
            stake_gram_amount=game_info.prev_stake,
            current_streak=game_info.current_streak,
            prize_per_mille=params[:6] or None,
            streak_prize_per_mille=params[6] if len(params) > 6 else None,
            plays_left=game_info.plays_left,
            client=client
        )
