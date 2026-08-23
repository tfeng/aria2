#!/usr/bin/env bash
#
# fetch-curl-impersonate-deps.sh — Build the two vendored static-lib dependencies aria2's
# --with-boringssl-impersonate / --with-brotli-impersonate configure flags need (Docs/Aria2.md's "Build"
# section; Docs/SSL.md Part 1 for the full investigation): a patched BoringSSL carrying Chrome 116's TLS
# profile, and brotli for its certificate-compression extension.
#
# Clones lwthiker/curl-impersonate at a pinned revision — READ-ONLY, never modified; this script only
# drives ITS OWN Makefile targets for these two library sets, exactly as upstream ships them, and never
# builds curl or nghttp2 (aria2 uses its own HTTP stack; it only borrows curl-impersonate's TLS backend +
# brotli decompression — see src/LibsslTLSContext.cc / src/LibsslTLSSession.cc). That Makefile already
# does the right native-Linux CMake/autotools build for BoringSSL/brotli — this script doesn't
# reimplement any of it, just points `make` at the two library paths and copies the result into deps/.
#
# Output: deps/boringssl-impersonate/{lib,include}/, deps/brotli-impersonate/{lib,include}/ — the exact
# shape configure.ac's --with-boringssl-impersonate/--with-brotli-impersonate expect (and, as bare flags
# with no =DIR, default to).
#
# Usage:
#   ./fetch-curl-impersonate-deps.sh            # build (no-op if already built at the pinned revision)
#   ./fetch-curl-impersonate-deps.sh --force    # rebuild even if the stamp matches
#
set -euo pipefail

# Must match FXPlayer's Scripts/build-curl-impersonate.sh exactly — both repos have to reference the same
# curl-impersonate revision. See Docs/SSL.md's "bumping the pinned curl-impersonate revision" section for
# the full checklist when updating this.
CURL_IMPERSONATE_REPO="https://github.com/lwthiker/curl-impersonate.git"
CURL_IMPERSONATE_SHA="822dbefe42e077fb9f3f16eaf0eca24944e5aadc"   # tag v0.6.1 + 3 commits (2024-03-03)

FORCE=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$ROOT/build/curl-impersonate-src"          # scratch clone (gitignored)
OUT_BORINGSSL="$ROOT/deps/boringssl-impersonate"
OUT_BROTLI="$ROOT/deps/brotli-impersonate"
STAMP="$OUT_BORINGSSL/.built-$CURL_IMPERSONATE_SHA"

for tool in git make cmake ninja patch; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: '$tool' not found — install it and retry." >&2; exit 1; }
done
if ! command -v gcc >/dev/null 2>&1 && ! command -v cc >/dev/null 2>&1; then
  echo "error: no C compiler found." >&2; exit 1
fi

if [ -f "$STAMP" ] && [ $FORCE = 0 ]; then
  echo "✓ curl-impersonate deps already built @ $CURL_IMPERSONATE_SHA (stamp: $STAMP). Use --force to rebuild."
  exit 0
fi

echo "→ cloning curl-impersonate @ $CURL_IMPERSONATE_SHA"
rm -rf "$WORK"
mkdir -p "$(dirname "$WORK")"
git clone --quiet "$CURL_IMPERSONATE_REPO" "$WORK"
git -C "$WORK" checkout --quiet "$CURL_IMPERSONATE_SHA"

echo "→ configuring curl-impersonate's own build (native host — this generates the Makefile the targets"
echo "  below need; nothing here builds curl itself)"
( cd "$WORK" && ./configure >/dev/null )

echo "→ building BoringSSL + brotli static libs only (curl-impersonate's own Makefile targets; curl and"
echo "  nghttp2 are deliberately NOT built — aria2 doesn't use them)"
(
  cd "$WORK"
  # These targets are declared in the Makefile via $(abspath ...) (e.g. `boringssl_static_libs :=
  # $(abspath boringssl/build)/lib/libssl.a ...`), so the literal target string `make` matches against is
  # an ABSOLUTE path, not "boringssl/build/lib/libssl.a" — a relative argument here just gets "No rule to
  # make target" even though the file the rule produces is exactly that path.
  make \
    "$WORK/boringssl/build/lib/libssl.a" "$WORK/boringssl/build/lib/libcrypto.a" \
    "$WORK/brotli-1.0.9/out/installed/lib/libbrotlicommon-static.a" \
    "$WORK/brotli-1.0.9/out/installed/lib/libbrotlidec-static.a"
)

echo "→ installing into deps/"
rm -rf "$OUT_BORINGSSL" "$OUT_BROTLI"
mkdir -p "$OUT_BORINGSSL/lib" "$OUT_BROTLI/lib" "$OUT_BROTLI/include"
# lib/{libssl,libcrypto}.a under boringssl/build are symlinks to ../ssl, ../crypto — plain cp (no -d/-P)
# dereferences them, copying the real archive content, which is what we want here.
cp "$WORK/boringssl/build/lib/libssl.a" "$WORK/boringssl/build/lib/libcrypto.a" "$OUT_BORINGSSL/lib/"
cp -r "$WORK/boringssl/build/include" "$OUT_BORINGSSL/include"
cp "$WORK/brotli-1.0.9/out/installed/lib/libbrotlicommon-static.a" \
   "$WORK/brotli-1.0.9/out/installed/lib/libbrotlidec-static.a" "$OUT_BROTLI/lib/"
cp -r "$WORK/brotli-1.0.9/c/include/brotli" "$OUT_BROTLI/include/brotli"

touch "$STAMP"
echo "✓ Built curl-impersonate deps @ $CURL_IMPERSONATE_SHA → deps/{boringssl,brotli}-impersonate/"
echo "  Next: ./configure --with-boringssl-impersonate --with-brotli-impersonate [--without-gnutls ...] && make"
