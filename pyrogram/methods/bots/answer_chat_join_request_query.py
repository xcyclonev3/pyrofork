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
from pyrogram import enums, raw


class AnswerChatJoinRequestQuery:
    async def answer_chat_join_request_query(
        self: "pyrogram.Client",
        query_id: Union[int, str],
        result: "enums.ChatJoinResultType",
        url: str = None
    ) -> bool:
        """Answer a join request query received by a guard bot.

        A guard bot of a supergroup decides whether users may join the chat: each join request it receives carries a
        query identifier, which is answered with this method.

        .. include:: /_includes/usable-by/bots.rst

        Parameters:
            query_id (``int`` | ``str``):
                Unique identifier of the join request query, as received with the chat join request.

            result (:obj:`~pyrogram.enums.ChatJoinResultType`):
                The decision: approve or decline the request, pass it on to the chat administrators
                or ask the user to continue in a Web App of the bot.

            url (``str``, *optional*):
                HTTPS URL of the Web App to open for the user.
                Required with :obj:`~pyrogram.enums.ChatJoinResultType.WEB_APP`, not allowed with the other results.

        Returns:
            ``bool``: True on success.

        Raises:
            ValueError: In case *url* is missing for a Web App, or passed with another result.

        Example:
            .. code-block:: python

                from pyrogram import enums

                # Let the user join the chat
                await app.answer_chat_join_request_query(query_id, enums.ChatJoinResultType.APPROVED)

                # Ask the user to complete a check in a Web App first
                await app.answer_chat_join_request_query(
                    query_id, enums.ChatJoinResultType.WEB_APP, url="https://example.org/check")
        """
        if result == enums.ChatJoinResultType.WEB_APP:
            if not url:
                raise ValueError("A Web App URL is required to answer with ChatJoinResultType.WEB_APP")

            join_result = raw.types.JoinChatBotResultWebView(url=url)
        elif url is not None:
            raise ValueError("A URL can only be passed with ChatJoinResultType.WEB_APP")
        elif result == enums.ChatJoinResultType.APPROVED:
            join_result = raw.types.JoinChatBotResultApproved()
        elif result == enums.ChatJoinResultType.DECLINED:
            join_result = raw.types.JoinChatBotResultDeclined()
        elif result == enums.ChatJoinResultType.QUEUED:
            join_result = raw.types.JoinChatBotResultQueued()
        else:
            raise ValueError(f"Unknown chat join result: {result}")

        return await self.invoke(
            raw.functions.bots.SetJoinChatResults(
                query_id=int(query_id),
                result=join_result
            )
        )
