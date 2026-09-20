#!/usr/bin/env bash
# Architecture grep gate for QuickMedia.
#
# The point of this script is to make the layering rules mechanically checkable
# instead of a review convention. Two things are forbidden:
#
#   1. The media plane (kernel/, memory/) must not know a protocol. If it did,
#      adding a protocol would require touching the kernel, which is exactly the
#      coupling the plugin registry exists to prevent.
#   2. The transport layer (transport/) must not know a codec. Transport
#      forwards bytes; codecs are a layer-L2 concern.
#
# The gate fails on any match and prints every offending line, so a violation is
# fixable from the CI output alone.

set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root" || exit 2

proto_pat='rtsp|rtmp|hls|srt|moq|gb28181|nvenc|qsv|vaapi|ffmpeg'
codec_pat='h264|h265|aac|opus|vp8|vp9|av1|mpeg4|x264|x265'

fail=0
scan() {
  local dir="$1" pat="$2" label="$3"
  [[ -d "$dir" ]] || return 0
  local hits
  hits=$(grep -rniE --include='*.go' --include='*.mod' --include='*.sum' "$pat" "$dir" 2>/dev/null)
  if [[ -n "$hits" ]]; then
    echo "FAIL: $label — $dir matched /$pat/"
    echo "$hits"
    fail=1
  fi
}

scan kernel "$proto_pat" "media plane must not name a protocol"
scan memory "$proto_pat" "memory plane must not name a protocol"
scan transport "$codec_pat" "transport must not name a codec"

# The kernel and the memory plane are the two packages the plugin contract is
# built to keep clean. If either starts importing a protocol library, the "add
# a protocol without touching the kernel" guarantee is gone.
if grep -rqE 'gortsplib|gortmplib|gohlslib|pion/webrtc' --include='*.go' kernel memory 2>/dev/null; then
  echo "FAIL: kernel or memory imports a protocol library"
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo ""
  echo "Architecture gate failed. See hits above."
  exit 1
fi
echo "Architecture gate passed."
