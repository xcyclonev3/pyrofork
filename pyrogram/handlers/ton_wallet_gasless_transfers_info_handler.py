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


class TonWalletGaslessTransfersInfoHandler(Handler):
    """The TonWalletGaslessTransfersInfo handler class. Used to handle changes of the gasless transfers available
    to the TON wallet of the current user. It is intended to be used with
    :meth:`~pyrogram.Client.add_handler`.

    For a nicer way to register this handler, have a look at the
    :meth:`~pyrogram.Client.on_ton_wallet_gasless_transfers_info` decorator.

    Parameters:
        callback (``Callable``):
            Pass a function that will be called when the gasless transfers info of the TON wallet changes. It
            takes *(client, ton_wallet_gasless_transfers_info)* as positional arguments (look at the section
            below for a detailed description).

        filters (:obj:`Filters`):
            Pass one or more filters to allow only a subset of updates to be passed in your callback function.

    Other parameters:
        client (:obj:`~pyrogram.Client`):
            The Client itself, useful when you want to call other API methods inside the handler.

        ton_wallet_gasless_transfers_info (:obj:`~pyrogram.types.TonWalletGaslessTransfersInfo`):
            The new gasless transfers info.
    """

    def __init__(self, callback: Callable, filters=None):
        super().__init__(callback, filters)
