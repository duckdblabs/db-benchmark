#!/bin/bash
set -e

unset CC
unset CXX
unset LD

# Install Cabal (Haskell build tool) if not present
if ! command -v ghcup &> /dev/null; then
    echo "Installing Cabal..."
    curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | BOOTSTRAP_HASKELL_NONINTERACTIVE=1 BOOTSTRAP_HASKELL_MINIMAL=1 sh
fi

[ -f "$HOME/.ghcup/env" ] && source "$HOME/.ghcup/env"

# Only install and set GHC if 9.12.2 isn't already the active version.
# Re-running these on every benchmark invocation emits warnings to stderr
# that then get flagged by validate_no_errors.sh.
if ! ghc --version 2>/dev/null | grep -q "9.12.2"; then
    ghcup install ghc 9.12.2
    ghcup set ghc 9.12.2
fi

source ./haskell/ver-haskell.sh

[ -d "$HOME/.cache/cabal/packages/hackage.haskell.org" ] || cabal update

## Cap GHC's heap at 90% of machine memory so we exit gracefully.
if [ -r /proc/meminfo ]; then
    MEM_KB=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
else
    MEM_KB=$(( $(sysctl -n hw.memsize) / 1024 ))
fi
export GHCRTS="-M$(( MEM_KB * 9 / 10 / 1024 ))m"

## Best-effort THP=always (GHC heap on 2MB pages); skipped when sudo is unavailable.
THP=/sys/kernel/mm/transparent_hugepage
if [ -w "$THP/enabled" ]; then
    echo always > "$THP/enabled" || true
    echo defer+madvise > "$THP/defrag" || true
elif sudo -n true 2>/dev/null; then
    echo always | sudo tee "$THP/enabled" > /dev/null || true
    echo defer+madvise | sudo tee "$THP/defrag" > /dev/null || true
fi

cabal --project-dir=./haskell run -O2 "$1-haskell"
