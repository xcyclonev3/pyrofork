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

import pyrogram
from pyrogram import raw
from ..object import Object


class Dice(Object):
    """A dice with a random value from 1 to 6 for currently supported base emoji.

    Parameters:
        emoji (``string``):
            Emoji on which the dice throw animation is based.

        value (``int``):
            Value of the dice, 1-6 for currently supported base emoji.

        stake_gram_amount (``int``, *optional*):
            The TON Gram amount that was staked; in the smallest units of the currency.
            For stake dice only, see :meth:`~pyrogram.Client.send_stake_dice`.

        prize_gram_amount (``int``, *optional*):
            The TON Gram amount that was gained from the roll; in the smallest units of the currency.
            For stake dice only.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        emoji: str,
        value: int,
        stake_gram_amount: int = None,
        prize_gram_amount: int = None
    ):
        super().__init__(client)

        self.emoji = emoji
        self.value = value
        self.stake_gram_amount = stake_gram_amount
        self.prize_gram_amount = prize_gram_amount

    @staticmethod
    def _parse(client, dice: "raw.types.MessageMediaDice") -> "Dice":
        # A stake dice carries the outcome of the game (TDLib: messageStakeDice)
        game_outcome = getattr(dice, "game_outcome", None)

        return Dice(
            emoji=dice.emoticon,
            value=dice.value,
            stake_gram_amount=game_outcome.stake_ton_amount if game_outcome else None,
            prize_gram_amount=game_outcome.ton_amount if game_outcome else None,
            client=client
        )
