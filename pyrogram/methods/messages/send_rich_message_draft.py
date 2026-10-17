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

from typing import Union

import pyrogram
from pyrogram import raw, types


class SendRichMessageDraft:
    async def send_rich_message_draft(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        draft_id: int,
        rich_message: "types.InputRichMessage",
        message_thread_id: int = None,
        can_stop: bool = None,
        keep_on_stop: bool = None
    ) -> bool:
        """Send a draft of a rich message being generated, shown to the user as a pending message.

        Send drafts with the same *draft_id* to update the pending message, then send the final message.

        .. include:: /_includes/usable-by/bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.

            draft_id (``int`` ``64-bit``):
                Unique identifier of the draft.

            rich_message (:obj:`~pyrogram.types.InputRichMessage`):
                Draft of the message. Media should be passed as file ids: other media are uploaded first.
                Use :obj:`~pyrogram.types.InputPageBlockThinking` to show a "Thinking..." placeholder.

            message_thread_id (``int``, *optional*):
                Unique identifier of the forum topic in which the message will be sent.

            can_stop (``bool``, *optional*):
                Pass True to show the user a button to stop further drafts.

            keep_on_stop (``bool``, *optional*):
                Pass True to keep the current draft when the user stops further generation.

        Returns:
            ``bool``: On success, True is returned.

        Example:
            .. code-block:: python

                from pyrogram import types

                await app.send_rich_message_draft(
                    chat_id, 1,
                    types.InputRichMessage(types.RichMessageSourceBlocks([
                        types.InputPageBlockThinking("Thinking...")
                    ])),
                    can_stop=True
                )
        """
        return await self.invoke(
            raw.functions.messages.SetTyping(
                peer=await self.resolve_peer(chat_id),
                top_msg_id=message_thread_id,
                action=raw.types.InputSendMessageRichMessageDraftAction(
                    can_stop=can_stop,
                    keep_on_stop=keep_on_stop,
                    random_id=draft_id,
                    rich_message=await rich_message.write(self, chat_id)
                )
            )
        )
