#!/usr/bin/env bash
set -euo pipefail

# Unset git env vars that leak from parent hook context into submodule commands
unset GIT_INDEX_FILE GIT_DIR

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
    # Stash any uncommitted/untracked changes so branch switch doesn't fail
    local stashed=0
    if [ -n "$(git -C "$SUBMODULE_DIR" status --porcelain)" ]; then
      git -C "$SUBMODULE_DIR" stash push --include-untracked --quiet 2>/dev/null && stashed=1
    fi

    if git -C "$SUBMODULE_DIR" show-ref --verify --quiet "refs/heads/$branch"; then
      git -C "$SUBMODULE_DIR" checkout "$branch" --quiet
    else
      git -C "$SUBMODULE_DIR" checkout -b "$branch" --quiet
    fi

    # Restore stashed changes on the new branch
    if [ "$stashed" = "1" ]; then
      git -C "$SUBMODULE_DIR" stash pop --quiet 2>/dev/null || true
    fi
  fi
}

case "${1:-}" in
  checkout)
    ensure_branch
    ;;

  commit)
    # Skip if submodule has no changes
    [ -n "$(git -C "$SUBMODULE_DIR" status --porcelain)" ] || exit 0

    # Only auto-commit submodule changes when the submodule pointer is already
    # staged in the parent (i.e. the user explicitly `git add`-ed it). This
    # prevents surprise commits of unrelated WIP in the submodule working tree.
    git diff --cached --quiet -- "$SUBMODULE_DIR" && exit 0

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

  post-commit)
    # The pre-commit framework's stashing undoes submodule changes made during
    # pre-commit. Re-commit the submodule and amend the parent to include it.

    # Only act if the most recent parent commit references the submodule. This
    # prevents the post-commit hook from auto-committing unrelated submodule WIP
    # (e.g. when switching branches or pulling on main).
    if ! git diff-tree --no-commit-id --name-only -r HEAD | grep -q "^${SUBMODULE_DIR}$"; then
      exit 0
    fi

    # Skip if submodule has no changes and pointer is up to date
    if [ -z "$(git -C "$SUBMODULE_DIR" status --porcelain)" ] && git diff --quiet -- "$SUBMODULE_DIR"; then
      exit 0
    fi

    ensure_branch

    if [ -n "$(git -C "$SUBMODULE_DIR" status --porcelain)" ]; then
      BRANCH=$(git rev-parse --abbrev-ref HEAD)
      git -C "$SUBMODULE_DIR" add -A
      git -C "$SUBMODULE_DIR" commit -m "Sync: $BRANCH"
    fi

    if ! git diff --quiet -- "$SUBMODULE_DIR"; then
      git add "$SUBMODULE_DIR"
      git commit --amend --no-edit --no-verify
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
    # Sync submodule to the parent's pointer
    git submodule update --init 2>/dev/null || true
    ensure_branch

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
    echo "Usage: $0 {checkout|commit|post-commit|push|merge}" >&2
    exit 1
    ;;
esac
