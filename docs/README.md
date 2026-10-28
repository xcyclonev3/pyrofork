# Pyrofork Documentation

Pyrofork is a Python client for Telegram's MTProto API. This documentation is available as a searchable website and as
Markdown in this repository. Start with a working example, follow a guide for a specific task, or open the API reference
when you know which symbol you need.

## Start here

- [Installation](getting-started/installation.md) — install the library and optional speedups.
- [Quick start](getting-started/quick-start.md) — send your first message with a user account.
- [Authorization](getting-started/authorization.md) — sign in as a user or bot and understand sessions.
- [Project setup](getting-started/setup.md) — prepare Telegram API credentials and a project.

## Learn the basics

- [Getting started](getting-started/README.md) — follow the recommended learning path.
- [Examples](examples/README.md) — copy and adapt focused scripts for common tasks.
- [Topic guides](guides/README.md) — filters, storage, formatting, plugins, performance, and advanced use.

## Look up details

- [API reference](api/README.md) — every high-level API symbol and the complete generated Telegram raw API.
- [FAQ](faq/README.md) — troubleshooting and answers to common usage questions.
- [Release notes](releases/README.md) — Layer 230 highlights and upgrade notes.

## Contributing to the documentation

Pages are authored in Markdown and built with Zensical from the repository root. Run `python tools/prepare_site.py` to
stage the Markdown sources before previewing or building. Follow relative links from the indexes; long pages include a
contents list near the top. API reference pages are generated from the Pyrofork source tree. Run
`bash build-docs.sh check` to verify them or `bash build-docs.sh generate` after changing API sources. Do not edit
generated API pages by hand.
