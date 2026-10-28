# Release Notes

## Pyrofork 2.3.70+layer230

This repository updates Pyrogram to Telegram API Layer 230. Compared with 2.3.69 (Layer 220), the update adds 139
methods, 177 types, and 14 update handlers.

### Highlights

- Communities, join requests, and community bans.
- Ephemeral messages and configurable welcome messages.
- Expanded polls, rich messages, drafts, and message translation.
- Text composition and summarization features.
- Managed bots, guest queries, secondary usernames, and styled keyboard buttons.
- Member tags, guard bots, and additional group permissions.
- Gift crafting, resale filters, exchange rates, and stake dice.
- Live photos, music in stories, and audio transcription.
- TON wallet operations, backups, NFTs, and TON Connect.
- Telegram's formatted date and TON address entities.

### Upgrade notes

No public method, parameter, attribute, or enum was removed or renamed. A few behaviors changed:

- Joining a group protected by a guard bot can raise GuardBotApprovalRequired.
- GiftedPremium.month_count again stores months; day_count stores the exact duration.
- ChatPermissions also restricts reactions and member tag changes.
- send_poll ignores quiz-only arguments for regular polls.
- Raw API writes reject objects that set only part of fields controlled by the same flag.

See the [quick start](../getting-started/quick-start.md) and [API reference](../api/README.md) for current usage.
