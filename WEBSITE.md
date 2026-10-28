# Pyrofork documentation site

This repository contains the Zensical setup prepared for Pyrofork. The Markdown content is in `docs/`; `docs/README.md` is the site homepage. The site targets the GitHub Pages URL `https://xcyclonev3.github.io/pyrofork/` and the repository root of `xcyclonev3/pyrofork`.

## Local preview

Use Python 3.12 and `uv`, then install the hash-locked docs dependencies, prepare the build-only Markdown copy, and start the preview server:

```sh
uv venv --python 3.12 .venv-docs
uv pip install --python .venv-docs/bin/python --require-hashes -r requirements-docs.lock
.venv-docs/bin/python tools/prepare_site.py
.venv-docs/bin/zensical serve
```

Build the production site and run strict link validation with:

```sh
.venv-docs/bin/python tools/prepare_site.py
.venv-docs/bin/zensical build --clean --strict
.venv-docs/bin/python tools/check_site.py
```

In the Pyrofork repository root, check that generated API pages match their source before building:

```sh
bash build-docs.sh check
```

To regenerate the API Markdown after changing its source, run `bash build-docs.sh generate` in the Pyrofork repository and commit the generated documentation along with the source change.

## GitHub Pages

The workflow in `.github/workflows/pages.yml` builds pull requests and publishes pushes to `main` in `xcyclonev3/pyrofork`. Enable GitHub Pages for that repository with **GitHub Actions** as its build and deployment source.

The site generator is pinned at `0.0.73`; update the pin and lock file together, then require a successful strict build and browser search check before publishing. The build-only staging step divides the generated symbol manifest and other oversized tables into searchable sections without changing the Markdown sources.
