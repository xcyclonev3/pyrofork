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

import re
from typing import Dict, Optional, Union

import pyrogram
from pyrogram import raw, types

from ..object import Object


class TextCompositionStyle(Object):
    """A style that can be used to compose a text with AI, see :meth:`~pyrogram.Client.compose_text_with_ai`.

    It is either a default style provided by Telegram or a custom style created by a user.

    Parameters:
        name (``str``):
            Name of the style: the identifier of a default style or the slug of a custom style.
            Methods that accept a style accept this name as well as the style itself.

        title (``str``):
            Title of the style in the language of the user's application.

        is_custom (``bool``):
            True, if the style was created by a user.

        custom_emoji_id (``int``, *optional*):
            Identifier of the custom emoji corresponding to the style.

        is_creator (``bool``, *optional*):
            True, if the current user created the style.

        install_count (``int``, *optional*):
            Number of users that installed the style; for custom styles only.

        prompt (``str``, *optional*):
            Prompt that describes how the style changes a text; for custom styles created by the current user only.

        creator (:obj:`~pyrogram.types.User`, *optional*):
            Creator of the style, if they chose to be shown; for custom styles only.

        english_example (:obj:`~pyrogram.types.TextCompositionStyleExample`, *optional*):
            Example of the style usage in English; for custom styles only.

        link (``str``, *property*):
            Link to add the style; for custom styles only.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        name: str,
        title: str,
        is_custom: bool,
        custom_emoji_id: int = None,
        is_creator: bool = None,
        install_count: int = None,
        prompt: str = None,
        creator: "types.User" = None,
        english_example: "types.TextCompositionStyleExample" = None
    ):
        super().__init__(client)

        self.name = name
        self.title = title
        self.is_custom = is_custom
        self.custom_emoji_id = custom_emoji_id
        self.is_creator = is_creator
        self.install_count = install_count
        self.prompt = prompt
        self.creator = creator
        self.english_example = english_example

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        tone: "raw.base.AiComposeTone",
        users: Dict[int, "raw.base.User"] = None
    ) -> Optional["TextCompositionStyle"]:
        if isinstance(tone, raw.types.AiComposeToneDefault):
            return TextCompositionStyle(
                name=tone.tone,
                title=tone.title,
                is_custom=False,
                custom_emoji_id=tone.emoji_id or None,
                client=client
            )

        if not isinstance(tone, raw.types.AiComposeTone):
            return None

        creator = None

        if tone.author_id:
            creator = types.User._parse(client, (users or {}).get(tone.author_id))
            me = getattr(client, "me", None)

            # The styles returned when creating or editing a style come without users
            if creator is None and me is not None and me.id == tone.author_id:
                creator = me

        return TextCompositionStyle(
            name=tone.slug,
            title=tone.title,
            is_custom=True,
            custom_emoji_id=tone.emoji_id,
            is_creator=tone.creator,
            install_count=tone.installs_count,
            prompt=tone.prompt,
            creator=creator,
            english_example=types.TextCompositionStyleExample._parse(client, tone.example_english),
            client=client
        )

    @property
    def link(self) -> Optional[str]:
        if not self.is_custom:
            return None

        return f"https://t.me/addstyle/{self.name}"

    def write(self) -> "raw.base.InputAiComposeTone":
        if self.is_custom:
            return raw.types.InputAiComposeToneSlug(slug=self.name)

        return raw.types.InputAiComposeToneDefault(tone=self.name)

    @staticmethod
    async def _get_input(
        client: "pyrogram.Client",
        style: Union[str, "TextCompositionStyle", None],
        custom_prompt: Optional[str] = None,
        optional: bool = False
    ) -> Optional["raw.base.InputAiComposeTone"]:
        """Get the InputAiComposeTone of a style, a style name or a single-use custom prompt.

        Like TDLib, a custom prompt is used instead of the style, and a style name is looked up in the
        list of styles of the user and otherwise used as the slug of a custom style.
        """
        if custom_prompt:
            return raw.types.InputAiComposeToneSingleUse(custom_prompt=custom_prompt)

        if isinstance(style, TextCompositionStyle):
            return style.write()

        if not style:
            if optional:
                return None

            raise ValueError("A text composition style must be specified")

        r = await client.invoke(raw.functions.aicompose.GetTones(hash=0))

        for tone in getattr(r, "tones", []):
            if isinstance(tone, raw.types.AiComposeToneDefault) and tone.tone == style:
                return raw.types.InputAiComposeToneDefault(tone=tone.tone)

            if isinstance(tone, raw.types.AiComposeTone) and tone.slug == style:
                return raw.types.InputAiComposeToneID(id=tone.id, access_hash=tone.access_hash)

        if re.fullmatch(r"[\w-]+", style, re.ASCII):
            return raw.types.InputAiComposeToneSlug(slug=style)

        raise ValueError(f"Text composition style {style!r} not found")
