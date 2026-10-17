# Benchmarks

Offline benchmarks of the hot paths of Pyrofork: nothing connects to Telegram. Run them from the repository root, with
Pyrofork's dependencies installed (TgCrypto too, to measure what users with it get):

```bash
python benchmarks/bench_mtproto.py
```

They measure this checkout. To compare with another one (whose raw API is generated, e.g. with `make api`), point
`TREE` to it:

```bash
TREE=/path/to/other/pyrofork python benchmarks/bench_mtproto.py
```

| Benchmark | What it measures |
|---|---|
| `bench_mtproto.py` | `mtproto.pack` and `unpack` from 64 B to 1 MiB, and the cost of the `crypto_executor` thread hop |
| `bench_tcp.py` | `TCP.recv` throughput against a local loopback server, from 64 B to 1 MiB packets |
| `bench_tl_read.py` | TL deserialization of an update, a 100 message history and a 1000 message history |
| `bench_send.py` | `Session.send` without waiting for answers: message creation, packing, hand over |
| `bench_recv.py` | `Session.handle_packet` for update packets of several sizes, as `recv_worker` runs it |
| `bench_pipeline.py` | Updates from packets to a message handler, through `handle_updates`, the storage and the dispatcher. `SESSION=file` uses a session file instead of an in-memory one; `PROFILE=1` prints a profile |
| `bench_transfer.py` | Downloads and uploads over a fake network with round trips, handshakes and shared bandwidth (`fake_network.py`) |
