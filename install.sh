#!/bin/sh
# jobboard installer.
#
#   curl -fsSL https://raw.githubusercontent.com/KreitnerL/jobboard/main/install.sh | sh
#   curl -fsSL .../install.sh | sh -s -- --yes --agents claude,codex     # no questions
#   ./install.sh                                                         # from a clone
#
# Fetches jobboard (unless run from a clone) and starts `jobrun setup`, which asks what to install: the `jobrun`
# command, the VS Code extension, and the skill for your coding agents. `jobrun uninstall` removes all of it.
# Environment: JOBBOARD_REF (branch or tag to install, default main).
set -eu

command -v python3 >/dev/null 2>&1 || { echo "jobboard needs python3 (3.9 or newer)." >&2; exit 1; }
python3 -c 'import sys; sys.exit(sys.version_info < (3, 9))' || { echo "jobboard needs Python 3.9 or newer." >&2; exit 1; }

here=$(cd "$(dirname "$0")" 2>/dev/null && pwd || true)
if [ -n "$here" ] && [ -f "$here/jobrun.py" ] && [ -d "$here/skills/jobrun" ]; then
    src=$here  # a clone or an unpacked release
else
    ref=${JOBBOARD_REF:-main}
    src=$(mktemp -d)
    trap 'rm -rf "$src"' EXIT
    echo "Downloading jobboard ($ref) ..."
    url="https://codeload.github.com/KreitnerL/jobboard/tar.gz/$ref"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" | tar -xz --strip-components=1 -C "$src"
    else
        wget -qO- "$url" | tar -xz --strip-components=1 -C "$src"
    fi
fi

# Under `curl | sh`, stdin is the script itself: the wizard asks on the terminal instead (jobrun reads /dev/tty).
python3 "$src/jobrun.py" setup "$@"
