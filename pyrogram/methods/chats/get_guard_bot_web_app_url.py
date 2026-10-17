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

import json

import pyrogram
from pyrogram import raw


class GetGuardBotWebAppUrl:
    async def get_guard_bot_web_app_url(
        self: "pyrogram.Client",
        query_id: int,
        platform: str = "unknown",
        theme_params: dict = None
    ) -> str:
        """Get the URL of the Web App in which a guard bot decides about a join request of the current user.

        Use it after :meth:`~pyrogram.Client.join_chat` raised :obj:`~pyrogram.errors.GuardBotApprovalRequired`;
        the decision arrives as a :obj:`~pyrogram.types.ChatJoinResult`
        (see :meth:`~pyrogram.Client.on_chat_join_result`).

        .. include:: /_includes/usable-by/users.rst

        Parameters:
            query_id (``int``):
                Identifier of the join request, as in :obj:`~pyrogram.errors.GuardBotApprovalRequired`.

            platform (``str``, *optional*):
                Short name of the application that opens the Web App (letters, digits and underscores).

            theme_params (``dict``, *optional*):
                Theme parameters to pass to the Web App.

        Returns:
            ``str``: The URL of the Web App.

        Example:
            .. code-block:: python

                from pyrogram.errors import GuardBotApprovalRequired

                try:
                    await app.join_chat("https://t.me/+AbCdEf0123456789")
                except GuardBotApprovalRequired as e:
                    print(await app.get_guard_bot_web_app_url(e.query_id))
        """
        r = await self.invoke(
            raw.functions.messages.RequestChatJoinWebView(
                query_id=query_id,
                theme_params=raw.types.DataJSON(data=json.dumps(theme_params)) if theme_params else None,
                platform=platform
            )
        )

        return r.url
