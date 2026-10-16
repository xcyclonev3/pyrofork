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

from .add_chat_to_community import AddChatToCommunity
from .approve_all_community_chat_requests import ApproveAllCommunityChatRequests
from .approve_community_chat_request import ApproveCommunityChatRequest
from .ban_community_member import BanCommunityMember
from .create_community import CreateCommunity
from .decline_all_community_chat_requests import DeclineAllCommunityChatRequests
from .decline_community_chat_request import DeclineCommunityChatRequest
from .get_community_chat_requests import GetCommunityChatRequests
from .get_community_member_chats import GetCommunityMemberChats
from .get_joined_communities import GetJoinedCommunities
from .remove_chat_from_community import RemoveChatFromCommunity
from .set_community_collapsed import SetCommunityCollapsed
from .unban_community_member import UnbanCommunityMember


class Communities(
    CreateCommunity,
    GetJoinedCommunities,
    AddChatToCommunity,
    RemoveChatFromCommunity,
    SetCommunityCollapsed,
    GetCommunityChatRequests,
    ApproveCommunityChatRequest,
    DeclineCommunityChatRequest,
    ApproveAllCommunityChatRequests,
    DeclineAllCommunityChatRequests,
    BanCommunityMember,
    UnbanCommunityMember,
    GetCommunityMemberChats,
):
    pass
