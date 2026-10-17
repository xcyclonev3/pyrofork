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

"""Serialize every constructor and method of the schema and read it back.

Each one is built three times: with every optional field set, with every optional field unset, and with optional
vectors set to empty lists (which is what reading an absent vector gives). The bytes must be read back completely
and written again identically.
"""
import importlib
import re
from io import BytesIO
from pathlib import Path

import pytest

from pyrogram.raw.all import objects
from pyrogram.raw.core import TLObject

SOURCE = Path(__file__).parent.parent / "compiler" / "api" / "source"
COMBINATOR_RE = re.compile(r"^([\w.]+)#([0-9a-f]+)\s*(.*?)\s*=\s*([\w<>.]+);")
FLAG_RE = re.compile(r"^flags\d?\.\d+\?(.+)$")
RENAMED = {"from": "from_peer", "self": "is_self"}  # fields renamed by the compiler
CORE_VALUES = {
    "int": 7, "long": 7, "int128": 7, "int256": 7, "double": 1.5,
    "string": "s", "bytes": b"b", "Bool": True, "true": True,
}


def parse_schema():
    combinators, by_type = {}, {}
    section = "types"
    for name in ("auth_key.tl", "sys_msgs.tl", "main_api.tl"):
        for line in (SOURCE / name).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("---"):
                section = line.strip("-")
                continue
            match = COMBINATOR_RE.match(line)
            if not match or match.group(1) in ("vector", "true", "boolTrue", "boolFalse", "error", "null"):
                continue
            name, cid, args, result = match.groups()
            fields = [tuple(arg.split(":", 1)) for arg in args.split() if ":" in arg and not arg.startswith("{")]
            combinators[(section, name)] = (fields, result, int(cid, 16))
            if section == "types":
                by_type.setdefault(result, []).append(name)
    return combinators, by_type


COMBINATORS, BY_TYPE = parse_schema()


def resolve(section, name):
    obj = objects[COMBINATORS[(section, name)][2]]
    if isinstance(obj, str):  # "pyrogram.raw.types.X", until pyrogram.raw resolves it to the class
        module, _, cls = obj.rpartition(".")
        obj = getattr(importlib.import_module(module), cls)
    return obj


def value(tl_type, mode, depth):
    tl_type = tl_type.lstrip("!")
    if tl_type in CORE_VALUES:
        return CORE_VALUES[tl_type]
    if tl_type.lower().startswith("vector<"):
        inner = tl_type[7:-1]
        # Recursive types (e.g. secureRequiredTypeOneOf) stop at an empty vector
        return [] if mode == "empty-vectors" or depth > 3 else [value(inner, mode, depth)]
    if tl_type == "X":  # the query of invokeWith* wrappers
        return resolve("functions", "help.getConfig")()
    if tl_type == "Object":  # the result of rpc_result
        return resolve("types", "inputPeerEmpty")()
    # Pick the constructor of this type that needs the fewest fields, to keep the object small
    candidates = BY_TYPE.get(tl_type) or BY_TYPE[tl_type[0].upper() + tl_type[1:]]
    name = min(candidates, key=lambda n: len(COMBINATORS[("types", n)][0]))
    return build("types", name, mode, depth + 1)


def build(section, name, mode, depth=0):
    fields, _, _ = COMBINATORS[(section, name)]
    kwargs = {}
    for field, tl_type in fields:
        if tl_type == "#":
            continue
        flag = FLAG_RE.match(tl_type)
        if flag:
            if mode == "unset" or depth > 2:
                continue
            tl_type = flag.group(1)
            if mode == "empty-vectors" and not tl_type.lower().startswith("vector<"):
                continue
        kwargs[RENAMED.get(field, field)] = value(tl_type, mode, depth)
    return resolve(section, name)(**kwargs)


@pytest.mark.parametrize("mode", ["set", "unset", "empty-vectors"])
def test_every_combinator_roundtrips(mode):
    failures = []
    for section, name in COMBINATORS:
        try:
            obj = build(section, name, mode)
            data = obj.write()
            # Vector.read guesses int/long items from the bytes left in the buffer; TL data is 4-byte aligned,
            # so 3 bytes of padding keep a trailing vector of 4-byte objects from being read as ints.
            buffer = BytesIO(data + b"\0\0\0")
            back = TLObject.read(buffer)
            assert buffer.tell() == len(data), "bytes not fully consumed"
            assert back.write() == data, "written again differently"
        except Exception as e:
            failures.append(f"{section}.{name}: {type(e).__name__}: {e}")
    assert not failures, f"{len(failures)} of {len(COMBINATORS)} failed:\n" + "\n".join(failures[:30])
