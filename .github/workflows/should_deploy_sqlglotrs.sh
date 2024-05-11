#!/usr/bin/env bash
set -ex

CARGO_TOML_PATH="./sqlglotrs/Cargo.toml"
CARGO_TOML_MOST_RECENT_COMMIT=$(git log -1 --pretty="%H" -- $CARGO_TOML_PATH | head -1)

PREVIOUS_TAG=$(git tag --sort=-creatordate | head -n 2 | tail -n 1)
PREVIOUS_TAG_COMMIT=$(git rev-list -n 1 $PREVIOUS_TAG)

# We should only deploy sqlglotrs if Cargo.toml was modified between this tag and the previous one
git merge-base --is-ancestor $PREVIOUS_TAG_COMMIT $CARGO_TOML_MOST_RECENT_COMMIT
