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

import secrets
from typing import Union

import pyrogram
from pyrogram import raw


class SendMessageViewMetrics:
    async def send_message_view_metrics(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        time_in_view_ms: int,
        active_time_in_view_ms: int,
        height_to_viewport_ratio_per_mille: int,
        seen_range_ratio_per_mille: int
    ) -> bool:
        """Report details of a view of a message by the current user in a chat, a message thread or a forum topic.

        Report a view if the message wasn't seen for more than 300 milliseconds, the viewport was destroyed or the
        total view duration exceeded 5 minutes.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            message_id (``int``):
                Identifier of the viewed message.

            time_in_view_ms (``int``):
                The amount of time at least one pixel of the message was seen, after the 300 ms entry grace period;
                in milliseconds.

            active_time_in_view_ms (``int``):
                The part of *time_in_view_ms* during which the user was active (the message was seen within 15
                seconds after any action of the user); in milliseconds.

            height_to_viewport_ratio_per_mille (``int``):
                The ratio of the message height to the viewport height, in 1/1000 fractions; may exceed 1000 when the
                message is taller than the viewport.

            seen_range_ratio_per_mille (``int``):
                The ratio of the viewed part of the message height to the full message height, in 1/1000 fractions;
                0-1000.

        Returns:
            ``bool``: On success, True is returned.

        Raises:
            ValueError: In case the metrics are out of range.

        Example:
            .. code-block:: python

                await app.send_message_view_metrics(chat_id, message_id, 2500, 2000, 400, 1000)
        """
        if (
            not 0 <= time_in_view_ms <= 1_000_000_000
            or not 0 <= active_time_in_view_ms <= 1_000_000_000
            or height_to_viewport_ratio_per_mille < 0
            or not 0 <= seen_range_ratio_per_mille <= 1000
        ):
            raise ValueError("Invalid metrics specified.")

        return await self.invoke(
            raw.functions.messages.ReportReadMetrics(
                peer=await self.resolve_peer(chat_id),
                metrics=[
                    raw.types.InputMessageReadMetric(
                        msg_id=message_id,
                        # A random non-zero identifier of this view, as TDLib generates it
                        view_id=secrets.randbits(63) or 1,
                        time_in_view_ms=time_in_view_ms,
                        active_time_in_view_ms=active_time_in_view_ms,
                        height_to_viewport_ratio_permille=height_to_viewport_ratio_per_mille,
                        seen_range_ratio_permille=seen_range_ratio_per_mille
                    )
                ]
            )
        )
