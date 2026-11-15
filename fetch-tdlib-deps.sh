#!/usr/bin/env bash
#
# fetch-tdlib-deps.sh — Build aria2c-tg, the Telegram helper aria2c runs for Telegram jobs (tg-helper/), against
# TDLib at a pinned revision. Same conventions as fetch-curl-impersonate-deps.sh: the pinned commit is fetched
# READ-ONLY (shallow) into a gitignored scratch dir, build/tdlib-src/, and never modified; the output lands in
# deps/tdlib/ (gitignored) with a stamp file, deps/tdlib/.built-<sha>, that makes a second run a no-op.
#
# Why a separate helper rather than linking TDLib into aria2c: aria2c links curl-impersonate's BoringSSL, TDLib
# needs OpenSSL, and the two export the same symbols. See tg-helper/aria2c-tg.cc.
#
# Output: deps/tdlib/bin/aria2c-tg (statically linked against TDLib; dynamically against the system's OpenSSL
# and zlib) -- what configure's --with-tdlib (bare flag) and `make install` pick up. Unlike the curl-impersonate
# deps, the stamp alone doesn't skip the build: an edited tg-helper/ source rebuilds the helper (incrementally --
# TDLib itself is only rebuilt when the pin changes or with --force).
#
# Usage:
#   ./fetch-tdlib-deps.sh            # build (no-op if already built at the pinned revision from current sources)
#   ./fetch-tdlib-deps.sh --force    # re-fetch TDLib and rebuild everything (about an hour on a Raspberry Pi 5)
#   ./fetch-tdlib-deps.sh --bump     # pin upstream's current master in this script, then build it
#
# Needs: git, cmake, make, a C++17 compiler, gperf, OpenSSL + zlib headers (Debian: git cmake make g++ gperf
# libssl-dev zlib1g-dev). TDLib's largest sources need ~3 GB of memory each to compile, hence the default -j2
# (override: JOBS=N).
set -euo pipefail

# Upstream tdlib/td has no regular release tags; master is what its own docs build. Bump with --bump.
TDLIB_REPO="https://github.com/tdlib/td.git"
TDLIB_SHA="c8828d50f5eac2fc7089ce1089d7ce9095c61b5e"   # master 2026-11-14 (TDLib 1.8.70)

FORCE=0
BUMP=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    --bump) BUMP=1 ;;
    *) echo "unknown arg: $arg (usage: $0 [--force] [--bump])" >&2; exit 2 ;;
  esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF="$ROOT/$(basename "${BASH_SOURCE[0]}")"
WORK="$ROOT/build/tdlib-src"            # scratch clone (gitignored)
BUILD="$ROOT/build/tg-helper-build"     # cmake build dir (gitignored)
OUT="$ROOT/deps/tdlib"
JOBS="${JOBS:-2}"

need() { # need TOOL [DEBIAN_PACKAGE]
  command -v "$1" >/dev/null 2>&1 || { echo "error: '$1' not found — install it and retry (Debian: apt install ${2:-$1})." >&2; exit 1; }
}
need git; need cmake; need make; need gperf; need c++ g++

if [ $BUMP = 1 ]; then
  latest="$(git ls-remote "$TDLIB_REPO" HEAD | cut -f1)"
  [ -n "$latest" ] || { echo "error: couldn't read upstream HEAD of $TDLIB_REPO" >&2; exit 1; }
  if [ "$latest" = "$TDLIB_SHA" ]; then
    echo "✓ already pinned to upstream master @ $TDLIB_SHA"
  else
    sed -i "s/^TDLIB_SHA=\"$TDLIB_SHA\"   # .*/TDLIB_SHA=\"$latest\"   # master $(date +%F) (bumped by --bump)/" "$SELF"
    echo "→ pin bumped $TDLIB_SHA → $latest in $(basename "$SELF") (review and commit it)"
    TDLIB_SHA="$latest"
  fi
fi

STAMP="$OUT/.built-$TDLIB_SHA"
HELPER="$OUT/bin/aria2c-tg"
if [ $FORCE = 0 ] && [ -f "$STAMP" ] && [ -x "$HELPER" ] && \
   [ -z "$(find "$ROOT/tg-helper" -newer "$HELPER" -type f -print -quit)" ]; then
  echo "✓ aria2c-tg already built @ $TDLIB_SHA (stamp: $STAMP). Use --force to rebuild."
  exit 0
fi

if [ $FORCE = 1 ] || [ "$(git -C "$WORK" rev-parse HEAD 2>/dev/null || true)" != "$TDLIB_SHA" ]; then
  echo "→ fetching TDLib @ $TDLIB_SHA"
  rm -rf "$WORK" "$BUILD"
  mkdir -p "$WORK"
  git -C "$WORK" init --quiet
  git -C "$WORK" remote add origin "$TDLIB_REPO"
  git -C "$WORK" fetch --quiet --depth 1 origin "$TDLIB_SHA"
  git -C "$WORK" checkout --quiet FETCH_HEAD
else
  echo "✓ TDLib source already @ $TDLIB_SHA (build/tdlib-src)"
fi

echo "→ building aria2c-tg (TDLib JSON interface, static) with -j$JOBS — a first build takes about an hour"
cmake -S "$ROOT/tg-helper" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release -DTD_SOURCE_DIR="$WORK" >/dev/null
cmake --build "$BUILD" --target aria2c-tg -j "$JOBS"

echo "→ installing into deps/"
mkdir -p "$OUT/bin"
install -m 0755 "$BUILD/aria2c-tg" "$HELPER"
rm -f "$OUT"/.built-* "$OUT/TDLIB_SHA" "$WORK"/.fx-cloned-*   # (TDLIB_SHA, .fx-cloned-*: older stamp names)
touch "$STAMP"
echo "✓ Built aria2c-tg @ $TDLIB_SHA → deps/tdlib/bin/aria2c-tg"
echo "  Next: ./configure ... --with-tdlib && make && sudo make install; set fx-telegram-helper (see INSTALL.fxplayer)"
