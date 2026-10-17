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
from pyrogram import raw, utils
from pyrogram.file_id import FileType

# Telegram wants listens to be reported only from this accumulated duration on
MIN_LISTENED_DURATION = 3


class ListenToAudio:
    async def listen_to_audio(
        self: "pyrogram.Client",
        audio_file_id: str,
        duration: int
    ) -> bool:
        """Inform Telegram that the current user listened to an audio file (a song).

        Report the accumulated playing time of the song (without the paused time) when the playback stops, another
        track starts, the player is closed or the playback stays paused for at least 60 seconds.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            audio_file_id (``str``):
                Identifier of the audio file that was listened to, as in :obj:`~pyrogram.types.Audio`.
                Voice notes aren't songs and can't be reported.

            duration (``int``):
                Duration of the listening to the audio, in whole seconds.
                Durations shorter than 3 seconds are not reported to Telegram.

        Returns:
            ``bool``: True on success.

        Raises:
            ValueError: In case of an invalid file identifier or a negative duration.

        Example:
            .. code-block:: python

                # The audio of the message was played for 42 seconds
                await app.listen_to_audio(message.audio.file_id, 42)
        """
        if duration < 0:
            raise ValueError("Invalid duration specified")

        audio = utils.get_input_media_from_file_id(audio_file_id, FileType.AUDIO).id

        if duration < MIN_LISTENED_DURATION:
            return True

        return await self.invoke(
            raw.functions.messages.ReportMusicListen(
                id=audio,
                listened_duration=duration
            )
        )
