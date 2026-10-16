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

import os
from typing import List, Optional, Union

import pyrogram
from pyrogram import enums, raw, types, utils


class SendStakeDice:
    async def send_stake_dice(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        state_hash: str,
        stake_gram_amount: int,
        disable_notification: bool = None,
        message_thread_id: int = None,
        reply_to_message_id: int = None,
        reply_to_chat_id: Union[int, str] = None,
        reply_to_monoforum_id: Union[int, str] = None,
        quote_text: str = None,
        quote_entities: List["types.MessageEntity"] = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        protect_content: bool = None,
        message_effect_id: int = None
    ) -> Optional["types.Message"]:
        """Send a stake dice: a "🎲" dice rolled for a stake in TON, whose prize depends on the rolled value.

        The prize of the roll is available in the *dice* of the sent message.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).
                You can also use chat public link in form of *t.me/<username>* (str).

            state_hash (``str``):
                Hash of the stake dice state, as received from :meth:`~pyrogram.Client.get_stake_dice_state`.
                The hash can be used only if it was received recently enough, otherwise a new state must be
                requested.

            stake_gram_amount (``int``):
                The TON Gram amount that will be staked; in the smallest units of the currency.

            disable_notification (``bool``, *optional*):
                Sends the message silently.
                Users will receive a notification with no sound.

            message_thread_id (``int``, *optional*):
                Unique identifier for the target message thread (topic) of the forum.
                for forum supergroups only.

            reply_to_message_id (``int``, *optional*):
                If the message is a reply, ID of the original message.

            reply_to_chat_id (``int`` | ``str``, *optional*):
                Unique identifier for the origin chat.
                for reply to message from another chat.
                You can also use chat public link in form of *t.me/<username>* (str).

            reply_to_monoforum_id (``int`` | ``str``, *optional*):
                Unique identifier for the target user of monoforum.
                for reply to message from monoforum.
                for channel administrators only.

            quote_text (``str``, *optional*):
                Text to quote.
                for reply_to_message only.

            quote_entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
                List of special entities that appear in quote_text, which can be specified instead of *parse_mode*.
                for reply_to_message only.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, quote_text are parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.
                For quote_text.

            protect_content (``bool``, *optional*):
                Protects the contents of the sent message from forwarding and saving.

            message_effect_id (``int`` ``64-bit``, *optional*):
                Unique identifier of the message effect to be added to the message; for private chats only.

        Returns:
            :obj:`~pyrogram.types.Message`: On success, the sent stake dice message is returned.

        Example:
            .. code-block:: python

                state = await app.get_stake_dice_state()

                if state.state_hash:
                    # Stake 0.1 TON
                    message = await app.send_stake_dice(chat_id, state.state_hash, 100_000_000)
                    print(message.dice.value, message.dice.prize_gram_amount)
        """
        reply_to = await utils.get_reply_to(
            client=self,
            chat_id=chat_id,
            reply_to_message_id=reply_to_message_id,
            message_thread_id=message_thread_id,
            reply_to_chat_id=reply_to_chat_id,
            reply_to_monoforum_id=reply_to_monoforum_id,
            quote_text=quote_text,
            quote_entities=quote_entities,
            parse_mode=parse_mode
        )

        r = await self.invoke(
            raw.functions.messages.SendMedia(
                peer=await self.resolve_peer(chat_id),
                media=raw.types.InputMediaStakeDice(
                    game_hash=state_hash,
                    ton_amount=stake_gram_amount,
                    # Like TDLib (inputMessageStakeDice), 32 random bytes chosen by the client
                    client_seed=os.urandom(32)
                ),
                silent=disable_notification or None,
                reply_to=reply_to,
                random_id=self.rnd_id(),
                noforwards=protect_content,
                effect=message_effect_id,
                message=""
            )
        )

        for i in r.updates:
            if isinstance(i, (raw.types.UpdateNewMessage, raw.types.UpdateNewChannelMessage)):
                return await types.Message._parse(
                    self, i.message,
                    {i.id: i for i in r.users},
                    {i.id: i for i in r.chats}
                )
