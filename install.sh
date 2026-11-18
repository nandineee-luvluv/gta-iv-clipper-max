#!/usr/bin/env bash
# Install this clipper and the separately installed ClippyMe local backend.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd "$HERE/.." && pwd)"
CLIPPYME="${CLIPPYME_HOME:-$WORKSPACE/clippyme}"
CLIPPYME_MCP="${CLIPPYME_MCP_HOME:-$WORKSPACE/clippyme-mcp}"
YTK="${YTK_PATH:-$WORKSPACE/youtube-kit/ytk}"
PYTHON="${AUTO_CLIP_PYTHON:-/usr/bin/python3}"
UNIT_DIR="$HOME/.config/systemd/user"

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
command -v "$PYTHON" >/dev/null || fail "Python not found: $PYTHON"
[ -x "$CLIPPYME/.venv/bin/python" ] || fail "ClippyMe venv missing: $CLIPPYME/.venv"
[ -x "$CLIPPYME_MCP/run_backend.sh" ] || fail "ClippyMe backend launcher missing: $CLIPPYME_MCP/run_backend.sh"
[ -x "$YTK" ] || fail "youtube-kit CLI missing: $YTK"
command -v curl >/dev/null || fail "curl is required"
command -v ffmpeg >/dev/null || fail "ffmpeg is required"
command -v ffprobe >/dev/null || fail "ffprobe is required"
command -v yt-dlp >/dev/null || "$CLIPPYME/.venv/bin/python" -m yt_dlp --version >/dev/null \
  || fail "yt-dlp is required"
command -v systemctl >/dev/null || fail "systemd user services are required"

if [ ! -f "$HERE/config.json" ]; then
  cp "$HERE/config.example.json" "$HERE/config.json"
  chmod 600 "$HERE/config.json"
  printf 'Created local config.json from the safe example. Publishing is disabled.\n'
fi

install -d "$UNIT_DIR" "$HOME/.local/bin"
for unit in auto-clip clippyme-backend; do
  sed -e "s|@AUTO_CLIP@|$HERE|g" \
      -e "s|@WORKSPACE@|$WORKSPACE|g" \
      -e "s|@CLIPPYME_MCP@|$CLIPPYME_MCP|g" \
      -e "s|@PYTHON@|$PYTHON|g" \
      "$HERE/systemd/$unit.service" > "$UNIT_DIR/$unit.service"
  chmod 644 "$UNIT_DIR/$unit.service"
done
systemctl --user daemon-reload
ln -sfn "$HERE/clip" "$HOME/.local/bin/clip"

if command -v loginctl >/dev/null; then
  loginctl enable-linger "$USER" 2>/dev/null || true
fi
systemctl --user enable --now clippyme-backend.service
for _ in $(seq 1 60); do
  curl -fsS -m 3 http://127.0.0.1:8000/api/health >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS -m 5 http://127.0.0.1:8000/api/health >/dev/null \
  || fail "ClippyMe backend did not become healthy; inspect its user journal"
systemctl --user enable --now auto-clip.service

if [ "$#" -gt 0 ]; then
  "$PYTHON" "$HERE/auto_clip.py" submit "$1" --start
fi
printf '\nInstalled. Publishing remains disabled until config.json is edited.\n'
printf 'Status: %s/auto_clip.py status\n' "$PYTHON"
printf 'Logs:   journalctl --user -u auto-clip -f\n'
