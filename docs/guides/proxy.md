# Proxy settings

Pass a proxy dictionary to `Client`. Supported schemes are SOCKS4, SOCKS5, and HTTP CONNECT. The following example uses
SOCKS5; set the hostname, port, and optional credentials for your proxy:

```python
from pyrogram import Client

proxy = {
    "scheme": "socks5",
    "hostname": "127.0.0.1",
    "port": 1080,
    # Omit these two keys if the proxy does not require authentication.
    "username": "proxy-user",
    "password": "proxy-password",
}

app = Client("my_account", proxy=proxy)
app.run()
```

Keep proxy credentials private. See the [Client reference](../api/client.md) for constructor parameters and the
[client settings guide](client-settings.md) for other connection options.
