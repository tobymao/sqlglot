#!/usr/bin/env bash
set -euo pipefail

SUBMODULE_DIR="sqlglot-integration-tests"

# Graceful no-op when the submodule is absent (public contributors)
[ -e "$SUBMODULE_DIR/.git" ] || exit 0

# Ensure the submodule is on a branch matching the parent's branch name.
# Creates the branch if it doesn't exist.
ensure_branch() {
  local branch
  branch=$(git rev-parse --abbrev-ref HEAD)
  [ "$branch" = "HEAD" ] && return 0

  local current
  current=$(git -C "$SUBMODULE_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")

  if [ "$current" != "$branch" ]; then
    if git -C "$SUBMODULE_DIR" show-ref --verify --quiet "refs/heads/$branch"; then
      git -C "$SUBMODULE_DIR" checkout "$branch" --quiet
    else
      git -C "$SUBMODULE_DIR" checkout -b "$branch" --quiet
    fi
  fi
}

case "${1:-}" in
  checkout)
    ensure_branch
    ;;

  commit)
    ensure_branch

    if [ -n "$(git -C "$SUBMODULE_DIR" status --porcelain)" ]; then
      BRANCH=$(git rev-parse --abbrev-ref HEAD)
      git -C "$SUBMODULE_DIR" add -A
      git -C "$SUBMODULE_DIR" commit -m "Sync: $BRANCH"
    fi

    # Stage the updated submodule pointer in the parent
    if ! git diff --quiet -- "$SUBMODULE_DIR"; then
      git add "$SUBMODULE_DIR"
    fi
    ;;

  push)
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
    case "$BRANCH" in main|master) exit 0 ;; esac

    ensure_branch

    cd "$SUBMODULE_DIR"

    # Check if there are local commits to push
    if git rev-parse --verify "@{upstream}" >/dev/null 2>&1; then
      UNPUSHED=$(git rev-list --count "@{upstream}..HEAD")
    else
      UNPUSHED=$(git rev-list --count "origin/main..HEAD" 2>/dev/null || echo "0")
    fi
    [ "$UNPUSHED" = "0" ] && exit 0

    # Push the branch, rebasing first if needed
    git push -u origin "$BRANCH" 2>/dev/null || {
      git pull --rebase --autostash || {
        git rebase --abort 2>/dev/null || true
        echo "ERROR: Conflicts pushing integration tests. Resolve with:" >&2
        echo "  cd $SUBMODULE_DIR && git pull --rebase" >&2
        exit 1
      }
      git push -u origin "$BRANCH"
    }

    # Create a PR if one doesn't exist (requires gh CLI)
    if command -v gh >/dev/null 2>&1; then
      EXISTING=$(gh pr list --head "$BRANCH" --json number --jq 'length' 2>/dev/null || echo "0")
      if [ "$EXISTING" = "0" ]; then
        PARENT_URL=$(git -C .. remote get-url origin 2>/dev/null | sed 's/\.git$//' | sed 's|git@github.com:|https://github.com/|')
        BODY="Parent branch: ${PARENT_URL}/tree/${BRANCH}"
        gh pr create \
          --title "$BRANCH" \
          --body "$BODY" \
          --head "$BRANCH" 2>/dev/null || true
      fi
    fi
    ;;

  merge)
    cd "$SUBMODULE_DIR"
    BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")
    [ "$BRANCH" = "HEAD" ] && exit 0

    git fetch origin --quiet 2>/dev/null || exit 0

    BEHIND=$(git rev-list --count "HEAD..origin/$BRANCH" 2>/dev/null || echo "0")
    [ "$BEHIND" = "0" ] && exit 0

    if ! git pull --rebase --autostash --quiet 2>/dev/null; then
      git rebase --abort 2>/dev/null || true
      echo "WARNING: Rebase conflicts in $SUBMODULE_DIR." >&2
      echo "Resolve with: cd $SUBMODULE_DIR && git pull --rebase" >&2
    fi
    ;;

  *)
    echo "Usage: $0 {checkout|commit|push|merge}" >&2
    exit 1
    ;;
esac
