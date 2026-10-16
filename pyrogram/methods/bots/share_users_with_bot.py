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

from typing import List, Union

import pyrogram
from pyrogram import raw


async def send_bot_requested_peers(
    client: "pyrogram.Client",
    chat_id: Union[int, str],
    button_id: int,
    peer_ids: List[Union[int, str]],
    message_id: int = None,
    prepared_button_id: str = None
) -> bool:
    # The button is either in a message of the bot or prepared by the bot for a Mini App
    if (message_id is None) == (prepared_button_id is None):
        raise ValueError("Pass either message_id or prepared_button_id")

    await client.invoke(
        raw.functions.messages.SendBotRequestedPeer(
            peer=await client.resolve_peer(chat_id),
            msg_id=message_id,
            webapp_req_id=prepared_button_id,
            button_id=button_id,
            requested_peers=[await client.resolve_peer(peer_id) for peer_id in peer_ids]
        )
    )

    return True


class ShareUsersWithBot:
    async def share_users_with_bot(
        self: "pyrogram.Client",
        chat_id: Union[int, str],
        button_id: int,
        user_ids: Union[int, str, List[Union[int, str]]],
        message_id: int = None,
        prepared_button_id: str = None
    ) -> bool:
        """Share users with a bot after pressing a button that requests users, or a button that requests the creation
        of a managed bot (share the created bot).

        The button is either in a message of the bot (pass *message_id*) or prepared by the bot for a Mini App
        (pass *prepared_button_id*, see :meth:`~pyrogram.Client.get_prepared_keyboard_button`).

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the bot.

            button_id (``int``):
                Identifier of the button, as found in its *request_user* or *request_managed_bot* field.

            user_ids (``int`` | ``str`` | List of ``int`` or ``str``):
                Unique identifiers (int) or usernames (str) of the users to share.

            message_id (``int``, *optional*):
                Identifier of the message with the button.

            prepared_button_id (``str``, *optional*):
                Identifier of the button prepared for a Mini App.

        Returns:
            ``bool``: True on success.

        Raises:
            ValueError: In case neither or both of *message_id* and *prepared_button_id* are passed.

        Example:
            .. code-block:: python

                # A button of a message sent by the bot
                await app.share_users_with_bot(bot_id, 1, [user_id1, user_id2], message_id=123)

                # A button prepared by the bot for its Mini App
                await app.share_users_with_bot(bot_id, 1, user_id, prepared_button_id=prepared_button_id)
        """
        if isinstance(user_ids, (int, str)):
            user_ids = [user_ids]

        return await send_bot_requested_peers(self, chat_id, button_id, user_ids, message_id, prepared_button_id)
