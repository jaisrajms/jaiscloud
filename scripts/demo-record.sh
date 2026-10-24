#!/usr/bin/env bash
#
# SPK7 — capture a real-browser demo take from an isolated Xvfb display.
#
# The crypto-medallion demo (plan_docs/gcp-demo-crypto-medallion-design.md §8)
# is recorded as a real browser session with a real OS cursor, not a screen-share
# of the operator's desktop. This script owns that pipeline: start Xvfb, open the
# page in Chrome on that display, capture with ffmpeg x11grab, then verify the
# result with ffprobe.
#
# It records any URL. With no --url it records a local stand-in page, so the
# pipeline is self-testable without the full demo (`make record-gcp-demo`). For
# the demo itself, point it at the Cloud Run leaderboard served on the emulator
# host's LAN address (see the README-GCP.md "Browser reachability" recipe and
# `make demo-browser-lan`) — a *remote* emulator is reachable from the recording
# browser only under that LAN/nip.io authority.
#
#   scripts/demo-record.sh [options]
#
#   --url URL          page to open (default: a generated stand-in page)
#   --out FILE         output mp4 (default: /tmp/opencode/demo-capture/demo-<ts>.mp4)
#   --duration SECS    capture length (default: 20)
#   --warmup SECS      wait for the page to render before capturing (default: 4)
#   --display DISP     reuse an existing X display (e.g. :1) instead of Xvfb
#   --display-num N    Xvfb display number (default: 99)
#   --size WxH         screen + capture size (default: 1920x1080)
#   --framerate N      capture frame rate (default: 30)
#   --no-browser       do not launch Chrome (pipeline self-test)
#   --keep-open        leave the browser/display up after capture (debug)
#   --help
#
# Env:
#   DEMO_RECORD_URL    same as --url
#   DEMO_RECORD_OUT    same as --out
#   DEMO_RECORD_DISPLAY  same as --display
#
# Requires: ffmpeg + ffprobe, google-chrome/chromium (unless --no-browser), and
# Xvfb unless --display reuses an existing display. Install the isolated display
# once (operator, sudo):
#   sudo apt-get install -y xvfb x11-utils xdotool fonts-dejavu-core
set -euo pipefail

SIZE="1920x1080"
FPS=30
DURATION=20
WARMUP=4
DISPLAY_NUM=99
OUT=""
URL="${DEMO_RECORD_URL:-}"
REUSE_DISPLAY="${DEMO_RECORD_DISPLAY:-}"
LAUNCH_BROWSER=1
KEEP_OPEN=0

if [ -n "${DEMO_RECORD_OUT:-}" ]; then OUT="$DEMO_RECORD_OUT"; fi

usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --url) URL="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --display) REUSE_DISPLAY="$2"; shift 2 ;;
    --display-num) DISPLAY_NUM="$2"; shift 2 ;;
    --size) SIZE="$2"; shift 2 ;;
    --framerate) FPS="$2"; shift 2 ;;
    --no-browser) LAUNCH_BROWSER=0; shift ;;
    --keep-open) KEEP_OPEN=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: $1 not found on PATH" >&2; return 1; }; }

need ffmpeg || exit 1
need ffprobe || exit 1

CHROME=""
if [ "$LAUNCH_BROWSER" = 1 ]; then
  for c in google-chrome google-chrome-stable chromium chromium-browser; do
    if command -v "$c" >/dev/null 2>&1; then CHROME="$c"; break; fi
  done
  [ -n "$CHROME" ] || { echo "ERROR: no google-chrome/chromium on PATH" >&2; exit 1; }
fi

if [ -z "$OUT" ]; then
  OUT="/tmp/opencode/demo-capture/demo-$(date -u +%Y%m%dT%H%M%SZ).mp4"
fi
OUT_DIR="$(dirname "$OUT")"
mkdir -p "$OUT_DIR"

if [ -z "$URL" ]; then
  STANDIN="$OUT_DIR/standin.html"
  cat > "$STANDIN" <<'HTML'
<!doctype html><html><head><meta charset="utf-8"><title>jaiscloud-gcp demo</title>
<style>body{margin:0;font-family:system-ui,sans-serif;background:#0b1220;color:#e6edf3;
display:flex;align-items:center;justify-content:center;height:100vh}
.card{text-align:center}h1{font-size:3rem;margin:0}p{opacity:.7}</style></head>
<body><div class="card"><h1>jaiscloud-gcp</h1>
<p>demo recording pipeline stand-in</p></div></body></html>
HTML
  URL="file://$STANDIN"
fi

XVFB_PID=""
CHROME_PID=""
WIDTH="${SIZE%x*}"
HEIGHT="${SIZE#*x}"

cleanup() {
  [ -n "$CHROME_PID" ] && kill "$CHROME_PID" 2>/dev/null || true
  [ -n "$XVFB_PID" ] && kill "$XVFB_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

if [ -n "$REUSE_DISPLAY" ]; then
  EXPORT_DISPLAY="$REUSE_DISPLAY"
  echo "==> reusing X display $EXPORT_DISPLAY"
else
  need Xvfb || { echo "Install it with:" >&2; echo "  sudo apt-get install -y xvfb x11-utils xdotool fonts-dejavu-core" >&2; exit 1; }
  EXPORT_DISPLAY=":$DISPLAY_NUM"
  echo "==> starting Xvfb $EXPORT_DISPLAY (${SIZE}x24)"
  Xvfb "$EXPORT_DISPLAY" -screen 0 "${WIDTH}x${HEIGHT}x24" -nolisten tcp >"$OUT_DIR/xvfb.log" 2>&1 &
  XVFB_PID=$!
  for _ in $(seq 1 50); do
    [ -e "/tmp/.X11-unix/X$DISPLAY_NUM" ] && break
    sleep 0.2
  done
  [ -e "/tmp/.X11-unix/X$DISPLAY_NUM" ] || { echo "ERROR: Xvfb $EXPORT_DISPLAY did not come up (see $OUT_DIR/xvfb.log)" >&2; exit 1; }
fi
export DISPLAY="$EXPORT_DISPLAY"
# The dev session is Wayland; without this Chrome's ozone auto-detection picks
# the Wayland backend and paints to a surface x11grab never sees. Pin X11.
unset WAYLAND_DISPLAY

if [ "$LAUNCH_BROWSER" = 1 ]; then
  PROFILE="$(mktemp -d /tmp/opencode/demo-chrome-XXXXXX)"
  echo "==> opening $URL in $CHROME"
  "$CHROME" \
    --user-data-dir="$PROFILE" \
    --ozone-platform=x11 \
    --no-first-run --no-default-browser-check \
    --disable-dev-shm-usage --disable-features=Translate \
    --kiosk --window-position=0,0 --window-size="${WIDTH},${HEIGHT}" \
    --app="$URL" >"$OUT_DIR/chrome.log" 2>&1 &
  CHROME_PID=$!
  echo "==> warming up ${WARMUP}s"
  sleep "$WARMUP"
fi

echo "==> capturing ${DURATION}s to $OUT"
ffmpeg -y -loglevel error -stats \
  -f x11grab -draw_mouse 1 -video_size "${WIDTH}x${HEIGHT}" -framerate "$FPS" \
  -i "$EXPORT_DISPLAY" \
  -t "$DURATION" -c:v libx264 -preset veryfast -crf 18 -pix_fmt yuv420p \
  "$OUT"

# Verify the take with ffprobe so a bad capture fails the run, not the edit.
if ! ffprobe_out="$(ffprobe -v error -select_streams v:0 \
      -show_entries stream=codec_name,width,height -show_entries format=duration \
      -of default=noprint_wrappers=1 "$OUT")"; then
  echo "ERROR: ffprobe rejected $OUT" >&2
  exit 1
fi
echo "==> $ffprobe_out"
dur="$(printf '%s\n' "$ffprobe_out" | awk -F= '/^duration=/{print $2}')"
awk -v d="$dur" 'BEGIN { exit (d > 0) ? 0 : 1 }' || { echo "ERROR: $OUT has zero duration" >&2; exit 1; }
echo "==> recorded $OUT (${dur}s)"

if [ "$KEEP_OPEN" = 1 ]; then
  echo "==> keeping browser/display up (Ctrl-C to exit)"; wait
fi
