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

"""The cache of messages."""

from pyrogram.client import Cache


def test_the_cache_drops_the_oldest_half_when_full():
    cache = Cache(10)

    for i in range(11):
        cache[i] = i

    assert list(cache.store) == [6, 7, 8, 9, 10]


def test_setting_a_value_again_makes_it_the_newest():
    cache = Cache(10)

    for i in range(11):
        cache[i] = i

    cache[6] = "again"

    assert list(cache.store) == [7, 8, 9, 10, 6] and cache[6] == "again"


def test_the_cache_keeps_working_after_many_rounds():
    cache = Cache(100)

    for i in range(1000):
        cache[i] = i
        assert len(cache.store) <= 100

    assert list(cache.store)[-1] == 999 and cache[0] is None and cache[999] == 999


def test_a_cache_without_capacity_keeps_nothing():
    cache = Cache(0)
    cache[1] = 1

    assert cache.store == {}
