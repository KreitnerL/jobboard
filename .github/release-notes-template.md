<!--
How to release vX.Y.Z:
1. Bump the version in jobrun.py, pyproject.toml, vscode/package.json and .claude-plugin/plugin.json (a test
   checks that they agree).
2. Copy this template to docs/releases/vX.Y.Z.md and fill it in.
3. Commit, then push the tag vX.Y.Z. The release workflow (.github/workflows/release.yml) runs the tests, publishes
   jobboard.vsix and install.sh, and uses docs/releases/vX.Y.Z.md as the release text.

Readers: people updating from the previous release. Keep it short and in plain words:
- one bullet per change, what it does for the user (not how it is built)
- name the command, option or marker to use, in backticks
- leave out sections with nothing in them, and delete these comments
-->
## jobboard vX.Y.Z

### New features

#### <Feature name>
- **What it is:** one sentence.
- **How to use it:** the commands or options.
- <one bullet per other thing a user notices: display, notifications, edge cases>

#### <Smaller feature>
- <one or two bullets>

### Changed
- <behaviour that works differently now>

### Fixed
- <bug, as the user saw it>
