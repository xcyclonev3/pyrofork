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

from gzip import decompress
from io import BytesIO
from typing import Any, Dict, Optional

from .primitives.bytes import Bytes
from .primitives.int import Int, Long
from .primitives.vector import Vector
from .tl_object import TLObject

RPC_RESULT_ID = 0xF35C6D01
GZIP_PACKED_ID = 0x3072CFA1


class Message(TLObject):
    ID = 0x5BB8E511  # hex(crc32(b"message msg_id:long seqno:int bytes:int body:Object = Message"))

    __slots__ = ["msg_id", "seq_no", "length", "body"]

    QUALNAME = "Message"

    def __init__(self, body: TLObject, msg_id: int, seq_no: int, length: int):
        self.msg_id = msg_id
        self.seq_no = seq_no
        self.length = length
        self.body = body

    @staticmethod
    def read(data: BytesIO, *args: Any) -> "Message":
        """Read a message; *args* may hold the element types of the results of bare vectors, by request msg_id."""
        msg_id = Long.read(data)
        seq_no = Int.read(data)
        length = Int.read(data)
        body = BytesIO(data.read(length))

        result_vectors = args[0] if args else None

        if result_vectors:
            rpc_result = Message._read_vector_rpc_result(body, result_vectors)

            if rpc_result is not None:
                return Message(rpc_result, msg_id, seq_no, length)

        return Message(TLObject.read(body, *args), msg_id, seq_no, length)

    @staticmethod
    def _read_vector_rpc_result(body: BytesIO, result_vectors: Dict[int, Any]) -> Optional[TLObject]:
        """Read an rpc_result whose request returns a bare vector of primitive values with their declared type.

        Such vectors can't be told apart without it: a Vector<string> of three-letter strings looks exactly like a
        Vector<int>. Returns None for other messages, leaving *body* untouched.
        """
        from pyrogram.raw.types import RpcResult  # The generated types import this module

        if Int.read(body, False) != RPC_RESULT_ID:
            body.seek(0)
            return None

        req_msg_id = Long.read(body)
        element_type = result_vectors.get(req_msg_id)
        result = body

        if element_type is not None:
            constructor_id = Int.read(result, False)

            if constructor_id == GZIP_PACKED_ID:
                result = BytesIO(decompress(Bytes.read(result)))
                constructor_id = Int.read(result, False)

            if constructor_id == Vector.ID:
                return RpcResult(req_msg_id=req_msg_id, result=Vector.read(result, element_type))

        # Not a vector (e.g. an rpc_error): read as usual
        body.seek(0)
        return None

    def write(self, *args: Any) -> bytes:
        b = BytesIO()

        b.write(Long(self.msg_id))
        b.write(Int(self.seq_no))
        b.write(Int(self.length))
        b.write(self.body.write())

        return b.getvalue()
