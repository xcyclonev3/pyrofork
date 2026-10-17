#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

from datetime import datetime
from typing import Dict, List, Union, Optional

import pyrogram
from pyrogram import raw, enums, utils
from pyrogram import types
from ..object import Object
from ..update import Update


class Poll(Object, Update):
    """A Poll.

    Parameters:
        id (``str``):
            Unique poll identifier.

        question (``str``):
            Poll question, 1-255 characters.

        options (List of :obj:`~pyrogram.types.PollOption`):
            List of poll options.
            Poll updates may carry only the new results: then the options have just *voter_count* and *data*, in the
            order of the results, which may differ from the order of the poll options. Match them by *data*.

        question_entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
            Special entities like usernames, URLs, bot commands, etc. that appear in the poll question.

        total_voter_count (``int``):
            Total number of users that voted in the poll.

        is_closed (``bool``):
            True, if the poll is closed.

        is_anonymous (``bool``, *optional*):
            True, if the poll is anonymous

        type (:obj:`~pyrogram.enums.PollType`, *optional*):
            Poll type.

        allows_multiple_answers (``bool``, *optional*):
            True, if the poll allows multiple answers.

        chosen_option_id (``int``, *optional*):
            0-based index of the chosen option), None in case of no vote yet.

        correct_option_id (``int``, *optional*):
            0-based identifier of the correct answer option.
            Available only for polls in the quiz mode, which are closed, or was sent (not forwarded) by the bot or to
            the private chat with the bot.

        explanation (``str``, *optional*):
            Text that is shown when a user chooses an incorrect answer or taps on the lamp icon in a quiz-style poll,
            0-200 characters.

        explanation_entities (List of :obj:`~pyrogram.types.MessageEntity`, *optional*):
            Special entities like usernames, URLs, bot commands, etc. that appear in the explanation.

        open_period (``int``, *optional*):
            Amount of time in seconds the poll will be active after creation.

        close_date (:py:obj:`~datetime.datetime`, *optional*):
            Point in time when the poll will be automatically closed.

        recent_voters (List of :obj:`~pyrogram.types.User`, *optional*):
            List of user whos recently vote.

        media (:obj:`~pyrogram.types.Photo` | :obj:`~pyrogram.types.Video` | :obj:`~pyrogram.types.Animation` | :obj:`~pyrogram.types.Audio` | :obj:`~pyrogram.types.Document` | :obj:`~pyrogram.types.Location` | :obj:`~pyrogram.types.Venue` | :obj:`~pyrogram.types.WebPagePreview`, *optional*):
            Media attached to the poll.

        correct_option_ids (List of ``int``, *optional*):
            0-based identifiers of the correct answer options, for polls in the quiz mode that can have several
            correct answers. Available in the same cases as *correct_option_id*.

        explanation_media (:obj:`~pyrogram.types.Photo` | :obj:`~pyrogram.types.Video` | :obj:`~pyrogram.types.Animation` | :obj:`~pyrogram.types.Audio` | :obj:`~pyrogram.types.Document` | :obj:`~pyrogram.types.Location` | :obj:`~pyrogram.types.Venue` | :obj:`~pyrogram.types.WebPagePreview`, *optional*):
            Media shown with the explanation of a quiz.

        allows_revoting (``bool``, *optional*):
            True, if the vote can be changed.

        allows_adding_options (``bool``, *optional*):
            True, if users can add answer options to the poll.

        shuffle_options (``bool``, *optional*):
            True, if the options are shown in a fixed random order.

        hide_results_until_closes (``bool``, *optional*):
            True, if the results are shown only after the poll closes.

        members_only (``bool``, *optional*):
            True, if only users that are members of the chat for more than a day can vote.

        country_codes (List of ``str``, *optional*):
            Two-letter ISO 3166-1 alpha-2 codes of the countries whose users can vote; None if all users can vote.

        is_creator (``bool``, *optional*):
            True, if the current user created the poll.

        has_unread_votes (``bool``, *optional*):
            True, if the poll has votes the current user didn't see yet.

        can_view_statistics (``bool``, *optional*):
            True, if the current user can get the vote statistics of the poll.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        id: str,
        question: str,
        options: List["types.PollOption"],
        question_entities: List["types.MessageEntity"] = None,
        total_voter_count: int,
        is_closed: bool,
        is_anonymous: bool = None,
        type: "enums.PollType" = None,
        allows_multiple_answers: bool = None,
        chosen_option_id: Optional[int] = None,
        correct_option_id: Optional[int] = None,
        explanation: Optional[str] = None,
        explanation_entities: Optional[List["types.MessageEntity"]] = None,
        open_period: Optional[int] = None,
        close_date: Optional[datetime] = None,
        recent_voters: List["types.User"] = None,
        media=None,
        correct_option_ids: Optional[List[int]] = None,
        explanation_media=None,
        allows_revoting: Optional[bool] = None,
        allows_adding_options: Optional[bool] = None,
        shuffle_options: Optional[bool] = None,
        hide_results_until_closes: Optional[bool] = None,
        members_only: Optional[bool] = None,
        country_codes: Optional[List[str]] = None,
        is_creator: Optional[bool] = None,
        has_unread_votes: Optional[bool] = None,
        can_view_statistics: Optional[bool] = None
    ):
        super().__init__(client)

        self.id = id
        self.question = question
        self.options = options
        self.question_entities = question_entities
        self.total_voter_count = total_voter_count
        self.is_closed = is_closed
        self.is_anonymous = is_anonymous
        self.type = type
        self.allows_multiple_answers = allows_multiple_answers
        self.chosen_option_id = chosen_option_id
        self.correct_option_id = correct_option_id
        self.explanation = explanation
        self.explanation_entities = explanation_entities
        self.open_period = open_period
        self.close_date = close_date
        self.recent_voters = recent_voters
        self.media = media
        self.correct_option_ids = correct_option_ids
        self.explanation_media = explanation_media
        self.allows_revoting = allows_revoting
        self.allows_adding_options = allows_adding_options
        self.shuffle_options = shuffle_options
        self.hide_results_until_closes = hide_results_until_closes
        self.members_only = members_only
        self.country_codes = country_codes
        self.is_creator = is_creator
        self.has_unread_votes = has_unread_votes
        self.can_view_statistics = can_view_statistics

    @staticmethod
    async def _parse_media(client, media: "raw.base.MessageMedia"):
        """The object of a media attached to a poll, to one of its options or to a quiz explanation."""
        if isinstance(media, raw.types.MessageMediaPhoto):
            return types.Photo._parse(client, media.photo, media.ttl_seconds, live_video=media.video)

        if isinstance(media, raw.types.MessageMediaGeo):
            return types.Location._parse(client, media.geo)

        if isinstance(media, raw.types.MessageMediaVenue):
            return types.Venue._parse(client, media)

        if isinstance(media, raw.types.MessageMediaWebPage):
            return types.WebPagePreview._parse(client, media)

        if isinstance(media, raw.types.MessageMediaDocument) and isinstance(media.document, raw.types.Document):
            doc = media.document
            attributes = {type(i): i for i in doc.attributes}
            file_name = getattr(attributes.get(raw.types.DocumentAttributeFilename, None), "file_name", None)

            if raw.types.DocumentAttributeAnimated in attributes:
                return types.Animation._parse(client, doc, attributes.get(raw.types.DocumentAttributeVideo), file_name)

            if raw.types.DocumentAttributeSticker in attributes:
                return await types.Sticker._parse(client, doc, attributes)

            if raw.types.DocumentAttributeVideo in attributes:
                return types.Video._parse(client, doc, attributes[raw.types.DocumentAttributeVideo], file_name)

            if raw.types.DocumentAttributeAudio in attributes:
                audio_attributes = attributes[raw.types.DocumentAttributeAudio]

                if audio_attributes.voice:
                    return types.Voice._parse(client, doc, audio_attributes)

                return types.Audio._parse(client, doc, audio_attributes, file_name)

            return types.Document._parse(client, doc, file_name)

        return None

    @staticmethod
    async def _parse(
        client,
        media_poll: Union["raw.types.MessageMediaPoll", "raw.types.UpdateMessagePoll"],
        users: List["raw.base.User"],
        chats: Dict[int, "raw.base.Chat"] = None
    ) -> "Poll":
        # Callers pass the users/chats of an Updates object either as lists or as id -> object dicts
        users = users if isinstance(users, dict) else {i.id: i for i in users or []}
        chats = chats if isinstance(chats, dict) else {i.id: i for i in chats or []}

        poll: raw.types.Poll = media_poll.poll
        poll_results: raw.types.PollResults = media_poll.results
        # Match results by option bytes like tdesktop and TDLib do: with open answers and shuffled polls
        # the results are not guaranteed to follow the order of the answers
        results: Dict[bytes, raw.types.PollAnswerVoters] = {r.option: r for r in poll_results.results}

        chosen_option_id = None
        correct_option_ids = []
        options = []

        for i, answer in enumerate(poll.answers):
            voter_count = 0
            result = results.get(answer.option)

            if result is not None:
                voter_count = result.voters or 0

                if result.chosen:
                    chosen_option_id = i

                if result.correct:
                    correct_option_ids.append(i)

            options.append(await types.PollOption._parse(client, answer, users, chats, voter_count))

        q_entities = [types.MessageEntity._parse(client, entity, {}) for entity in poll.question.entities] if poll.question.entities else []
        question_entities = types.List(filter(lambda x: x is not None, q_entities))

        recent_voters = []
        for user in poll_results.recent_voters:
            try:
                voter = types.User._parse(client, users.get(user.user_id, None))
            except Exception:
                pass
            else:
                recent_voters.append(voter)

        return Poll(
            id=str(poll.id),
            question=poll.question.text,
            options=options,
            question_entities=question_entities,
            total_voter_count=media_poll.results.total_voters,
            is_closed=poll.closed,
            is_anonymous=not poll.public_voters,
            type=enums.PollType.QUIZ if poll.quiz else enums.PollType.REGULAR,
            allows_multiple_answers=poll.multiple_choice,
            chosen_option_id=chosen_option_id,
            correct_option_id=correct_option_ids[0] if correct_option_ids else None,
            explanation=poll_results.solution,
            explanation_entities=[
                types.MessageEntity._parse(client, i, {})
                for i in poll_results.solution_entities
            ] if poll_results.solution_entities else None,
            open_period=poll.close_period,
            close_date=utils.timestamp_to_datetime(poll.close_date),
            recent_voters=recent_voters if len(recent_voters) > 0 else None,
            media=await Poll._parse_media(client, getattr(media_poll, "attached_media", None)),
            correct_option_ids=correct_option_ids or None,
            explanation_media=await Poll._parse_media(client, poll_results.solution_media),
            allows_revoting=not poll.revoting_disabled,
            allows_adding_options=poll.open_answers,
            shuffle_options=poll.shuffle_answers,
            hide_results_until_closes=poll.hide_results_until_close,
            members_only=poll.subscribers_only,
            country_codes=poll.countries_iso2 or None,
            is_creator=poll.creator,
            has_unread_votes=poll_results.has_unread_votes,
            can_view_statistics=poll_results.can_view_stats,
            client=client
        )

    @staticmethod
    async def _parse_update(
        client,
        update: "raw.types.UpdateMessagePoll",
        users: List["raw.base.User"]
    ) -> "Poll":
        if update.poll is not None:
            return await Poll._parse(client, update, users)

        # Only the results are known: the ids refer to the options built from them, in their order
        results = update.results.results
        chosen_option_id = None
        correct_option_ids = []
        options = []

        for i, result in enumerate(results):
            if result.chosen:
                chosen_option_id = i

            if result.correct:
                correct_option_ids.append(i)

            options.append(
                types.PollOption(
                    text="",
                    voter_count=result.voters or 0,
                    data=result.option
                )
            )

        recent_voters = []
        if update.results.recent_voters:
            for user in update.results.recent_voters:
                try:
                    voter = types.User._parse(client, users.get(user.user_id, None))
                except Exception:
                    pass
                else:
                    recent_voters.append(voter)

        return Poll(
            id=str(update.poll_id),
            question="",
            options=options,
            total_voter_count=update.results.total_voters,
            is_closed=False,
            chosen_option_id=chosen_option_id,
            correct_option_id=correct_option_ids[0] if correct_option_ids else None,
            correct_option_ids=correct_option_ids or None,
            recent_voters=recent_voters if len(recent_voters) > 0 else None,
            client=client
        )
