#  Pyrogram - Telegram MTProto API Client Library for Python
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

"""Typical TL payloads: an update with one message, and a 100 message history (with entities and reactions)."""
import time

from pyrogram import raw


def user(i):
    return raw.types.User(id=i, access_hash=i * 7, first_name=f"User {i}", last_name="Last", username=f"user_{i}",
                          photo=raw.types.UserProfilePhoto(photo_id=i, dc_id=2),
                          status=raw.types.UserStatusRecently(), usernames=[])


def channel(i):
    return raw.types.Channel(id=i, access_hash=i * 3, title=f"Group {i}", photo=raw.types.ChatPhotoEmpty(), date=0,
                             megagroup=True, username=f"group_{i}", participants_count=1234, usernames=[])


def message(i, peer=777):
    return raw.types.Message(
        id=i, peer_id=raw.types.PeerChannel(channel_id=peer), from_id=raw.types.PeerUser(user_id=(i % 20) + 1),
        date=int(time.time()), message=f"Message number {i} with some text, a link https://example.org and #tag " * 2,
        entities=[raw.types.MessageEntityBold(offset=0, length=7), raw.types.MessageEntityUrl(offset=40, length=19),
                  raw.types.MessageEntityHashtag(offset=70, length=4)],
        reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=max(i - 1, 1)) if i % 3 == 0 else None,
        reactions=raw.types.MessageReactions(results=[
            raw.types.ReactionCount(reaction=raw.types.ReactionEmoji(emoticon="👍"), count=3)], recent_reactions=[]),
        views=100 + i, forwards=i % 5, edit_date=None)


UPDATE = raw.types.Updates(updates=[raw.types.UpdateNewChannelMessage(message=message(1), pts=10, pts_count=1)],
                           users=[user(i) for i in range(1, 3)], chats=[channel(777)], date=0, seq=0)
HISTORY = raw.types.messages.ChannelMessages(pts=1, count=100, messages=[message(i) for i in range(1, 101)],
                                             topics=[], chats=[channel(777)], users=[user(i) for i in range(1, 21)])
