# gta-iv-clipper-max

Local, hands-off live-stream clipper for GTA VI. Submit a stream URL or scan a
configured channel list; it records accessible live streams, detects chat
spikes, cuts vertical moments, asks ClippyMe to render Shorts with burned-in
captions, saves SRT sidecars, and can upload videos and English caption tracks
to an authorized YouTube channel.

The repository name is `gta-iv-clipper-max`; the default title filter and tags
target GTA 6 / GTA VI. No channel, upload account, credentials, or creator
watchlist is selected by default.

## Features

- YouTube live chat monitoring; Twitch chat via anonymous IRC.
- YouTube, Twitch, and Kick capture (Kick chat spikes are not supported).
- Chat-spike windows with configurable pre/post padding, thresholds, and
  cooldown.
- Concurrent live captures (default maximum: four) and persistent SQLite queue.
- ClippyMe 9:16 rendering, burned-in subtitles, YouTube-friendly metadata, SRT
  sidecars, and optional YouTube caption-track uploads.
- Optional scan of user-configured live channels; only GTA 6/VI-titled streams
  are queued by default.
- User-scoped systemd services; no root or Docker required.

## Architecture and dependencies

This repository contains the queue, stream/chat capture, spike detector,
publisher, CLI, and service templates. It intentionally does **not** bundle
ClippyMe, `youtube-kit`, cookies, API keys, OAuth tokens, recordings, or the
library of generated clips.

The supported workspace layout is:

```text
workspace/
├── gta-iv-clipper-max/   # this repository
├── clippyme/             # ClippyMe backend and Python environment
├── clippyme-mcp/         # local backend launcher
└── youtube-kit/          # ytk CLI and local OAuth login
```

Install those companion projects separately and configure their credentials
locally. ClippyMe needs its own transcription/metadata credentials. YouTube
OAuth credentials are stored by `youtube-kit` outside this repository.

## Install

Requirements: Linux with systemd user services, Python 3, `ffmpeg`, `ffprobe`,
`curl`, `yt-dlp`, and the companion projects above. `streamlink` is optional
for Twitch/Kick.

```bash
cd workspace/gta-iv-clipper-max
cp config.example.json config.json
# Edit config.json locally: set ClippyMe paths, ytk path, channel/watchlist,
# and explicitly enable publishing only after verifying your YouTube login.
./install.sh
```

The sample config defaults to **publishing disabled** and **private**. Keep
`config.json` local; it is git-ignored. The installer enables user services
and copies service templates with local paths. `AUTO_CLIP_PYTHON`,
`CLIPPYME_HOME`, `CLIPPYME_MCP_HOME`, and `YTK_PATH` may be set to override the
workspace defaults.

## Usage

Queue one stream:

```bash
./clip 'https://www.youtube.com/@channel/live'
```

Check a configured watchlist without queuing anything, then scan and queue
matching live GTA streams:

```bash
python3 auto_clip.py scan-live --dry-run
python3 auto_clip.py scan-live
```

Add channels to `watchlist.channels` in your ignored local `config.json`:

```json
{"name":"Example creator","url":"https://www.youtube.com/@handle/live"}
```

Other useful commands:

```bash
python3 auto_clip.py status
python3 auto_clip.py list
python3 auto_clip.py probe 'https://www.youtube.com/@channel/live'
python3 auto_clip.py doctor
python3 auto_clip.py cancel <item-id>
```

## Publishing and captions

Set `publish.enabled` to `true` only after `ytk auth login` is configured for
the intended channel. Set `publish.channel` to that channel, or leave it empty
to use ytk's locally configured default. `publish.privacy` defaults to
`private`; public publishing must be explicitly chosen by the operator.

For spike clips, subtitles are burned into the video. When
`publish.upload_caption_tracks` is enabled, auto-clip also obtains an SRT from
ClippyMe, stores it beside the clip, and uploads it as a YouTube caption track
after video upload succeeds. A caption-track failure is logged without
re-uploading the video.

Automated `scan-live` only checks the channel URLs in your local config. The
default watchlist is empty. `watchlist.require_gta_title` defaults to `true`
to avoid attaching GTA metadata to unrelated streams.

## Privacy and safety

- Never commit `config.json`, `.env`, OAuth files, cookies, or service state.
- Do not add API key values to documentation, command examples, or issue logs.
- The default repo contains no personal upload channel or creator watchlist.
- Public uploads can receive copyright claims or strikes. Only publish clips
  you are authorized to use and follow YouTube policies.
- Review the privacy, daily cap, and spacing settings before enabling uploads.

## Operations

```bash
systemctl --user status auto-clip clippyme-backend
journalctl --user -u auto-clip -f
journalctl --user -u clippyme-backend -f
```

Runtime state, captures, and generated clips are local under `state/`,
`captures/`, and `library/`; these paths are ignored by Git.
