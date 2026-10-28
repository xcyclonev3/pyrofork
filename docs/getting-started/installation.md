# Installation

Pyrofork requires Python 3.10 or newer. Install the latest version from this repository with pip:

```bash
python -m pip install -U "git+https://github.com/xcyclonev3/pyrofork.git"
```

The package builds its Telegram raw API during installation. If you also want the optional cryptography speedup, install
TgCrypto:

```bash
python -m pip install -U tgcrypto-pyrofork
```

Check the installation:

```python
import pyrogram

print(pyrogram.__version__)
```

Next, [create your Telegram API credentials](setup.md), then follow the [quick start](quick-start.md).

---

[Next →](quick-start.md)
