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

"""Session.invoke: what it does with flood waits and retries."""

import asyncio

import pytest

import pyrogram
from pyrogram import errors, raw
from pyrogram.session import Session


def invoke_with_answers(answers, monkeypatch, **kwargs):
    """Invoke a request whose attempts get these answers. Returns the result, or the error, and what was slept."""
    real_sleep = asyncio.sleep
    slept = []

    async def fast_sleep(seconds, *args, **kwargs):
        slept.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    async def send(query, *args, **kwargs):
        answer = answers.pop(0)

        if isinstance(answer, Exception):
            raise answer

        return answer

    async def main():
        client = pyrogram.Client("invoke", api_id=1, api_hash="x", in_memory=True)
        session = Session(client, 2, bytes(range(256)), False)
        session.is_started.set()
        session.send = send

        try:
            return await session.invoke(raw.functions.Ping(ping_id=1), **kwargs)
        except errors.FloodWait as e:
            return e

    return asyncio.run(main()), slept


@pytest.mark.parametrize("first_error", [OSError("reset"), errors.InternalServerError("x"), errors.ServiceUnavailable("x")], ids=lambda e: type(e).__name__)
@pytest.mark.parametrize("sleep_threshold", [60, -1])
def test_a_retried_request_keeps_its_sleep_threshold(monkeypatch, first_error, sleep_threshold):
    result, slept = invoke_with_answers([first_error, errors.FloodWait(value=30), "answer"], monkeypatch, sleep_threshold=sleep_threshold)

    assert result == "answer" and 30 in slept


def test_a_retried_request_still_raises_flood_waits_over_its_sleep_threshold(monkeypatch):
    result, slept = invoke_with_answers([OSError("reset"), errors.FloodWait(value=30)], monkeypatch, sleep_threshold=20)

    assert isinstance(result, errors.FloodWait) and 30 not in slept
