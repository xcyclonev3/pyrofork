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


from typing import List, Optional, Union

import pyrogram
from pyrogram import enums, raw, types, utils


class SendResoldGift:
    async def send_resold_gift(
        self: "pyrogram.Client",
        gift_link: str,
        new_owner_chat_id: Union[int, str],
        text: str = None,
        parse_mode: Optional["enums.ParseMode"] = None,
        entities: List["types.MessageEntity"] = None,
        is_private: bool = None
    ) -> Optional["types.Message"]:
        """Send an upgraded gift that is available for resale to another user or channel chat.

        .. note::

            Gifts already owned by the current user must be transferred using :meth:`~pyrogram.Client.transfer_gift` and can't be passed to this method.

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            gift_link (``str``):
                Link to the gift.

            new_owner_chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat you want to transfer the star gift to.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).

            text (``str``, *optional*):
                Text to show along with the gift.
                Only bold, italic, underline, strikethrough, spoiler, custom emoji and date-time entities are kept.

            parse_mode (:obj:`~pyrogram.enums.ParseMode`, *optional*):
                By default, texts are parsed using both Markdown and HTML styles.
                You can combine both syntaxes together.

            entities (List of :obj:`~pyrogram.types.MessageEntity`):
                List of special entities that appear in the text, which can be specified instead of *parse_mode*.

            is_private (``bool``, *optional*):
                Pass True to show the text and the sender of the gift only to its receiver.

        Returns:
            :obj:`~pyrogram.types.Message`: On success, the sent message is returned.

        Example:
            .. code-block:: python

                # Transfer gift to another user
                await app.send_resold_gift(gift_link="https://t.me/nft/NekoHelmet-9215", new_owner_chat_id=123)
        """
        match = self.UPGRADED_GIFT_RE.match(gift_link)

        if not match:
            raise ValueError(
                "Invalid gift link provided."
            )

        peer = await self.resolve_peer(new_owner_chat_id)

        message = None

        if text:
            parsed = await utils.parse_text_entities(self, text, parse_mode, entities)
            message = raw.types.TextWithEntities(text=parsed["message"], entities=parsed["entities"] or [])

        invoice = raw.types.InputInvoiceStarGiftResale(
            slug=match.group(1),
            to_id=peer,
            # Like TDLib, the name of the sender is shown unless the gift is private
            show_name=not is_private,
            message=message
        )

        r = await self.invoke(
            raw.functions.payments.SendStarsForm(
                form_id=(await self.invoke(
                    raw.functions.payments.GetPaymentForm(
                        invoice=invoice
                    ),
                )).form_id,
                invoice=invoice
            ),
        )

        updates = r.updates if isinstance(r, raw.types.payments.PaymentResult) else r
        users = {u.id: u for u in getattr(updates, "users", [])}
        chats = {c.id: c for c in getattr(updates, "chats", [])}

        for update in getattr(updates, "updates", []):
            if isinstance(update, (raw.types.UpdateNewMessage, raw.types.UpdateNewChannelMessage)):
                return await types.Message._parse(self, update.message, users, chats)

        return None
