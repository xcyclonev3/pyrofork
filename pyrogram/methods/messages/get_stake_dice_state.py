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

import pyrogram
from pyrogram import raw, types


class GetStakeDiceState:
    async def get_stake_dice_state(
        self: "pyrogram.Client"
    ) -> "types.StakeDiceState":
        """Get the current state of the stake dice of the current user.

        Use :meth:`~pyrogram.Client.send_stake_dice` to send a stake dice.

        .. include:: /_includes/usable-by/users.rst

        Returns:
            :obj:`~pyrogram.types.StakeDiceState`: On success, the stake dice state is returned.

        Example:
            .. code-block:: python

                state = await app.get_stake_dice_state()

                if state.state_hash:
                    print(state.prize_per_mille, state.current_streak)
        """
        r = await self.invoke(raw.functions.messages.GetEmojiGameInfo())

        return types.StakeDiceState._parse(self, r)
