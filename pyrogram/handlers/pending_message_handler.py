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


from typing import Callable

from .handler import Handler


class PendingMessageHandler(Handler):
    """The PendingMessage handler class. Used to handle pending messages: drafts of messages that bots stream
    into private chats. It is intended to be used with :meth:`~pyrogram.Client.add_handler`.

    For a nicer way to register this handler, have a look at the
    :meth:`~pyrogram.Client.on_pending_message` decorator.

    Parameters:
        callback (``Callable``):
            Pass a function that will be called when a bot sends a new version of a pending message. It takes
            *(client, pending_message)* as positional arguments (look at the section below for a detailed
            description).

        filters (:obj:`Filters`):
            Pass one or more filters to allow only a subset of updates to be passed in your callback function.

    Other parameters:
        client (:obj:`~pyrogram.Client`):
            The Client itself, useful when you want to call other API methods inside the handler.

        pending_message (:obj:`~pyrogram.types.PendingMessage`):
            The pending message. It replaces any pending message with the same draft_id and must be removed
            when a message or a pending message with another draft_id arrives.
    """

    def __init__(self, callback: Callable, filters=None):
        super().__init__(callback, filters)
