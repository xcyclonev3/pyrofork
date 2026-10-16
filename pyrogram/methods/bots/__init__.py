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

from .add_bot_secondary_username import AddBotSecondaryUsername
from .answer_callback_query import AnswerCallbackQuery
from .answer_chat_join_request_query import AnswerChatJoinRequestQuery
from .answer_guest_query import AnswerGuestQuery
from .answer_inline_query import AnswerInlineQuery
from .answer_web_app_query import AnswerWebAppQuery
from .check_bot_username import CheckBotUsername
from .create_bot import CreateBot
from .delete_bot_commands import DeleteBotCommands
from .delete_bot_secondary_username import DeleteBotSecondaryUsername
from .get_bot_commands import GetBotCommands
from .get_bot_default_privileges import GetBotDefaultPrivileges
from .get_bot_info import GetBotInfo
from .get_chat_menu_button import GetChatMenuButton
from .get_collectible_item_info import GetCollectibleItemInfo
from .get_game_high_scores import GetGameHighScores
from .get_inline_bot_results import GetInlineBotResults
from .get_managed_bot_access_settings import GetManagedBotAccessSettings
from .get_managed_bot_token import GetManagedBotToken
from .get_prepared_keyboard_button import GetPreparedKeyboardButton
from .request_callback_answer import RequestCallbackAnswer
from .save_prepared_keyboard_button import SavePreparedKeyboardButton
from .send_game import SendGame
from .send_inline_bot_result import SendInlineBotResult
from .set_bot_commands import SetBotCommands
from .set_bot_default_privileges import SetBotDefaultPrivileges
from .set_bot_info import SetBotInfo
from .set_chat_menu_button import SetChatMenuButton
from .set_game_score import SetGameScore
from .get_owned_bots import GetOwnedBots
from .get_similar_bots import GetSimilarBots
from .set_managed_bot_access_settings import SetManagedBotAccessSettings
from .share_chat_with_bot import ShareChatWithBot
from .share_users_with_bot import ShareUsersWithBot
from .toggle_bot_username_is_active import ToggleBotUsernameIsActive


class Bots(
    AnswerCallbackQuery,
    AnswerInlineQuery,
    GetInlineBotResults,
    RequestCallbackAnswer,
    SendInlineBotResult,
    SendGame,
    SetGameScore,
    GetGameHighScores,
    SetBotCommands,
    GetBotCommands,
    DeleteBotCommands,
    SetBotDefaultPrivileges,
    GetBotDefaultPrivileges,
    SetBotInfo,
    GetBotInfo,
    SetChatMenuButton,
    GetChatMenuButton,
    AnswerWebAppQuery,
    GetCollectibleItemInfo,
    GetOwnedBots,
    GetSimilarBots,
    AnswerChatJoinRequestQuery,
    CreateBot,
    GetManagedBotToken,
    GetManagedBotAccessSettings,
    SetManagedBotAccessSettings,
    CheckBotUsername,
    AddBotSecondaryUsername,
    DeleteBotSecondaryUsername,
    ToggleBotUsernameIsActive,
    SavePreparedKeyboardButton,
    GetPreparedKeyboardButton,
    ShareUsersWithBot,
    ShareChatWithBot,
    AnswerGuestQuery,
):
    pass
