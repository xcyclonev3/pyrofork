#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

action="${1:-check}"
shift || true

case "$action" in
    check)
        python3 compiler/docs/markdown.py --check "$@"
        python3 compiler/docs/validate.py
        ;;
    generate)
        python3 compiler/docs/markdown.py "$@"
        python3 compiler/docs/validate.py
        ;;
    *)
        echo "Usage: bash build-docs.sh [check|generate]" >&2
        exit 2
        ;;
esac
