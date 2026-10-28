# Text Formatting

## Contents

- [Basic Styles](#basic-styles)
- [HTML Style](#html-style)
- [Markdown Style](#markdown-style)
- [Different Styles](#different-styles)
- [Nested and Overlapping Entities](#nested-and-overlapping-entities)



Pyrogram uses a custom Markdown dialect for text formatting which adds some unique features that make writing styled
texts easier in both Markdown and HTML. You can send sophisticated text messages and media captions using a
variety of decorations that can also be nested in order to combine multiple styles together.

-----

## Basic Styles


When formatting your messages, you can choose between Markdown-style, HTML-style or both (default). The following is a
list of the basic styles currently supported by Pyrogram.

- **bold**
- *italic*
- ~~strike~~
- underline
- spoiler
- [text URL](https://pyrogram.org)
- [user text mention](tg://user?id=123456789)
- 🔥
- formatted date and time
- `inline fixed-width code`
- pre-formatted text:

  ```text
  pre-formatted
    fixed-width
      code block
  ```
- quoted text: `> Quoted text`
- expandable quoted text: `**> Quoted text`

## HTML Style


To strictly use this mode, pass `HTML` to the *parse_mode* parameter when using
[`send_message`](../api/methods/messages/send_message.md). The following tags are currently supported:

```text
<b>bold</b>, <strong>bold</strong>

<i>italic</i>, <em>italic</em>

<u>underline</u>

<s>strike</s>, <del>strike</del>, <strike>strike</strike>

<spoiler>spoiler</spoiler>

<a href="https://pyrogram.org/">text URL</a>

<a href="tg://user?id=123456789">inline mention</a>

<emoji id="12345678901234567890">🔥</emoji>

<tg-time unix="1647531900" format="wDT">22:45 tomorrow</tg-time>

<code>inline fixed-width code</code>

<pre>
pre-formatted
  fixed-width
    code block
</pre>

<blockquote>Quoted text</blockquote>

<blockquote expandable>Quoted text with collapse/expand button</blockquote>
```

**Example**:

```python
from pyrogram import enums

await app.send_message(
    "me",
    (
        "<b>bold</b>, "
        "<i>italic</i>, "
        "<u>underline</u>, "
        "<s>strike</s>, "
        "<spoiler>spoiler</spoiler>, "
        "<a href=\"https://pyrogram.org/\">URL</a>, "
        "<code>code</code>\n\n"
        "<pre>"
        "for i in range(10):\n"
        "    print(i)"
        "</pre>"
        "<blockquote>quote</blockquote>"
    ),
    parse_mode=enums.ParseMode.HTML
)
```

> [!NOTE]
> All `<`, `>` and `&` symbols that are not a part of a tag or an HTML entity must be replaced with the
> corresponding HTML entities (`<` with `&lt;`, `>` with `&gt;` and `&` with `&amp;`). You can use this
> snippet to quickly escape those characters:
>
> ```python
> import html
>
> text = "<my text>"
> text = html.escape(text)
>
> print(text)
> ```
>
> ```text
> &lt;my text&gt;
> ```

> [!NOTE]
> The format of a date is either `r`, to show the time relative to now, or any combination of `w` (the day of
> the week), `d` or `D` (the date, short or long) and `t` or `T` (the time, short or long). Like in TDLib, an
> invalid format is an error, and so is an invalid Unix time in Markdown, while a `<tg-time>` tag with an invalid
> Unix time is ignored.

## Markdown Style


> [!WARNING]
> The Markdown style is not recommended for complex text formatting.
> If you want to use complex text formatting such as nested entities, overlapping entities use the HTML style instead.

To strictly use this mode, pass `MARKDOWN` to the *parse_mode* parameter when using
[`send_message`](../api/methods/messages/send_message.md). Use the following syntax in your message:

````text
**bold**

__italic__

--underline--

~~strike~~

||spoiler||

[text URL](https://pyrogram.org/)

[text user mention](tg://user?id=123456789)

![🔥](tg://emoji?id=12345678901234567890)

![22:45 tomorrow](tg://time?unix=1647531900&format=wDT)

`inline fixed-width code`

```
pre-formatted
  fixed-width
    code block
```

> Quoted text
>
> **> Quoted text with collapse/expand button**
````

**Example**:

```python
from pyrogram import enums

await app.send_message(
    "me",
    (
        "**bold**, "
        "__italic__, "
        "--underline--, "
        "~~strike~~, "
        "||spoiler||, "
        "[URL](https://pyrogram.org), "
        "`code`, "
        "```"
        "for i in range(10):\n"
        "    print(i)"
        "```"
    ),
    parse_mode=enums.ParseMode.MARKDOWN
)
```

## Different Styles


By default, when ignoring the *parse_mode* parameter, both Markdown and HTML styles are enabled together.
This means you can combine together both syntaxes in the same text:

```python
await app.send_message("me", "**bold**, <i>italic</i>")
```

Result:

    **bold**, *italic*

If you don't like this behaviour you can always choose to only enable either Markdown or HTML in strict mode by passing
`MARKDOWN` or `HTML` as argument to the *parse_mode* parameter.

```python
from pyrogram import enums

await app.send_message("me", "**bold**, <i>italic</i>", parse_mode=enums.ParseMode.MARKDOWN)
await app.send_message("me", "**bold**, <i>italic</i>", parse_mode=enums.ParseMode.HTML)
```

Result:

    **bold**, <i>italic</i>

    \*\*bold**, *italic*

In case you want to completely turn off the style parser, simply pass `DISABLED` to *parse_mode*.
The text will be sent as-is.

```python
from pyrogram import enums

await app.send_message("me", "**bold**, <i>italic</i>", parse_mode=enums.ParseMode.DISABLED)
```

Result:

    \*\*bold**, <i>italic</i>

## Nested and Overlapping Entities


You can also style texts with more than one decoration at once by nesting entities together. For example, you can send
a text message with both **bold and underline** styles, or combine italic and strike formatting. Markdown and HTML
formatting can also be combined.

Here there are some example texts you can try sending:

**HTML**:

- `<b>bold, <u>underline</u></b>`
- `<b>bold <i>italic <u>underline <s>strike</s></u></i></b>`
- `<b>bold <i>and</b> italic</i>`

**Markdown (Not Recommended)**:

- `**bold, --underline--**`
- `**bold __italic --underline ~~strike~~--__**`
- `**bold __and** italic__`

**Combined**:

- `--you can combine <i>HTML</i> with **Markdown**--`
- `**and also <i>overlap** --entities</i> this way--`