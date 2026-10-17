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


class EditMessageRichMessage:
    async def edit_message_rich_message(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        message_id: int,
        rich_message: "types.InputRichMessage",
        reply_markup: "types.InlineKeyboardMarkup" = None,
        business_connection_id: str = None
    ) -> "types.Message":
        """Edit a text or rich message, replacing its content with a rich message.

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).
                You can also use chat public link in form of *t.me/<username>* (str).

            message_id (``int``):
                Message identifier in the chat specified in chat_id.

            rich_message (:obj:`~pyrogram.types.InputRichMessage`):
                New content of the message.

            reply_markup (:obj:`~pyrogram.types.InlineKeyboardMarkup`, *optional*):
                An InlineKeyboardMarkup object; for bots only.

            business_connection_id (``str``, *optional*):
                Unique identifier of the business connection on behalf of which the message was sent;
                for business bots only.

        Returns:
            :obj:`~pyrogram.types.Message`: On success, the edited message is returned.

        Example:
            .. code-block:: python

                from pyrogram import types

                await app.edit_message_rich_message(
                    chat_id, message_id,
                    types.InputRichMessage(types.RichMessageSourceBlocks([
                        types.InputPageBlockParagraph(types.RichTextBold("Updated!"))
                    ]))
                )
        """
        rpc = raw.functions.messages.EditMessage(
            peer=await self.resolve_peer(chat_id),
            id=message_id,
            reply_markup=await reply_markup.write(self) if reply_markup else None,
            rich_message=await rich_message.write(self, chat_id, business_connection_id)
        )

        if business_connection_id is not None:
            r = await self.invoke(
                raw.functions.InvokeWithBusinessConnection(
                    connection_id=business_connection_id,
                    query=rpc
                )
            )
        else:
            r = await self.invoke(rpc)

        for i in r.updates:
            if isinstance(i, (raw.types.UpdateEditMessage,
                              raw.types.UpdateEditChannelMessage,
                              raw.types.UpdateBotEditBusinessMessage)):
                return await types.Message._parse(
                    self, i.message,
                    {i.id: i for i in r.users},
                    {i.id: i for i in r.chats},
                    business_connection_id=business_connection_id
                )
