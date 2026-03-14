#!/bin/bash
# Cowork session setup script for vfs-s3
# Run this at the start of every Cowork session

set -e

# When sourced, $0 is the shell binary, not the script path.
# Use BASH_SOURCE which always points to the script file.
COWORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$COWORK_DIR")"
LOCAL_BIN="$HOME/.local/bin"

echo "=== vfs-s3 Cowork Session Setup ==="

# 1. Install gh CLI if not present
if ! command -v gh &>/dev/null && [ ! -f "$LOCAL_BIN/gh" ]; then
    echo "Installing GitHub CLI..."
    ARCH=$(uname -m)
    case "$ARCH" in
        aarch64) GH_ARCH="arm64" ;;
        x86_64)  GH_ARCH="amd64" ;;
        *)       echo "Unsupported arch: $ARCH"; exit 1 ;;
    esac
    GH_VERSION="2.67.0"
    mkdir -p "$LOCAL_BIN"
    curl -fsSL "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${GH_ARCH}.tar.gz" \
        -o /tmp/gh.tar.gz
    tar xzf /tmp/gh.tar.gz -C /tmp
    mv "/tmp/gh_${GH_VERSION}_linux_${GH_ARCH}/bin/gh" "$LOCAL_BIN/gh"
    rm -rf /tmp/gh.tar.gz "/tmp/gh_${GH_VERSION}_linux_${GH_ARCH}"
    echo "gh installed: $($LOCAL_BIN/gh --version | head -1)"
else
    echo "gh already available"
fi

# Add to PATH
export PATH="$LOCAL_BIN:$PATH"

# 2. Configure gh with bot token
TOKEN_FILE="$COWORK_DIR/github-bot-token"
if [ -f "$TOKEN_FILE" ]; then
    export GH_TOKEN=$(cat "$TOKEN_FILE")
    echo "Bot token loaded from $TOKEN_FILE"
    gh auth status 2>&1 | head -3
else
    echo "WARNING: No bot token found at $TOKEN_FILE"
    echo "Create a fine-grained PAT for the bot account and save it there."
fi

# 3. Configure git to avoid creating lock files on read-only operations.
# Claude Code polls `git status` frequently, which creates stale index.lock files
# (see https://github.com/anthropics/claude-code/issues/11005).
# The --no-optional-locks flag prevents this.
git -C "$PROJECT_DIR" config alias.s "status --no-optional-locks"
git -C "$PROJECT_DIR" config alias.d "diff --no-optional-locks"
echo "Git aliases configured: 'git s' and 'git d' use --no-optional-locks"

# 4. Clean up stale worktrees from previous Cowork sessions
if [ -d "$PROJECT_DIR/.git/worktrees" ]; then
    for wt in "$PROJECT_DIR/.git/worktrees"/*/; do
        [ -d "$wt" ] || continue
        wt_name=$(basename "$wt")
        git -C "$PROJECT_DIR" worktree unlock "$wt_name" 2>/dev/null || true
        rm -f "${wt}HEAD.lock" "${wt}index.lock"
    done
    PRUNED=$(git -C "$PROJECT_DIR" worktree prune -v 2>&1)
    if [ -n "$PRUNED" ]; then
        echo "Pruned stale worktrees:"
        echo "$PRUNED"
    fi
fi

# 5. Set git author via environment variables (does NOT modify .git/config)
export GIT_AUTHOR_NAME="Claude (vfs-s3 bot)"
export GIT_AUTHOR_EMAIL="267615948+vfs-s3-bot@users.noreply.github.com"
export GIT_COMMITTER_NAME="Claude (vfs-s3 bot)"
export GIT_COMMITTER_EMAIL="267615948+vfs-s3-bot@users.noreply.github.com"
echo "Git author: $GIT_AUTHOR_NAME <$GIT_AUTHOR_EMAIL>"

# 5. Install mise if not present
if ! command -v mise &>/dev/null && [ ! -f "$LOCAL_BIN/mise" ]; then
    echo "Installing mise..."
    curl -fsSL https://mise.jdx.dev/install.sh | MISE_INSTALL_PATH="$LOCAL_BIN/mise" sh
    echo "mise installed: $($LOCAL_BIN/mise --version)"
else
    echo "mise already available"
fi

# 6. Run mise trust and install tools
if command -v mise &>/dev/null; then
    mise trust 2>/dev/null && echo "mise trusted"
    mise install 2>/dev/null && echo "mise tools installed"
fi

echo "=== Setup complete ==="
