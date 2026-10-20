#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
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

"""How an interactive login tells where the confirmation code went, for every code type."""

import asyncio
import enum
import inspect

import pytest

import pyrogram
from pyrogram import enums, raw, types


# The login tells how the confirmation code was sent, whatever its type

def test_every_sent_code_type_has_a_description():
    descriptions = pyrogram.client.SENT_CODE_DESCRIPTIONS
    assert set(descriptions) == set(enums.SentCodeType)
    for sent_code_type in enums.SentCodeType:
        assert pyrogram.client.sent_code_description(sent_code_type) == descriptions[sent_code_type] != ""


class FutureSentCodeType(enum.Enum):
    SMS_WORD = raw.types.auth.SentCodeTypeSmsWord


@pytest.mark.parametrize("sent_code_type, description", [
    (FutureSentCodeType.SMS_WORD, "sms word"),  # a type the table doesn't know (yet)
    (None, "an unknown method"),
    (raw.types.auth.SentCodeTypeSmsWord(), "an unknown method"),  # not even hashable
])
def test_unknown_sent_code_types_are_described(sent_code_type, description):
    assert pyrogram.client.sent_code_description(sent_code_type) == description


def test_login_with_a_code_sent_by_missed_call(capsys):
    async def main():
        client = pyrogram.Client(
            "login", api_id=1, api_hash="x", in_memory=True, phone_number="+15550000000", phone_code="12345"
        )
        user = types.User(id=7, is_self=True)

        async def send_code(phone_number):
            return types.SentCode(type=enums.SentCodeType.MISSED_CALL, phone_code_hash="hash")

        async def sign_in(phone_number, phone_code_hash, phone_code):
            assert (phone_number, phone_code_hash, phone_code) == ("+15550000000", "hash", "12345")
            return user

        client.send_code, client.sign_in = send_code, sign_in
        return await client.authorize() is user

    assert asyncio.run(main())
    assert "The confirmation code has been sent via phone missed call" in capsys.readouterr().out


@pytest.mark.parametrize("code_type", sorted(
    (getattr(raw.types.auth, name) for name in dir(raw.types.auth) if name.startswith("SentCodeType")),
    key=lambda cls: cls.__name__
))
def test_sent_code_parses_every_code_type(code_type):
    required = {
        name: {"int": 1, "str": "x", "bytes": b"x", "bool": True}.get(str(parameter.annotation), None)
        for name, parameter in inspect.signature(code_type.__init__).parameters.items()
        if name != "self" and parameter.default is parameter.empty
    }
    sent_code = types.SentCode._parse(raw.types.auth.SentCode(type=code_type(**required), phone_code_hash="hash"))
    # Code types missing from the enum (e.g. SmsWord) made send_code, and so the login, fail with ValueError
    assert sent_code.type.value is code_type
    assert pyrogram.client.sent_code_description(sent_code.type)
