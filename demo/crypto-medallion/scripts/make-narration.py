#!/usr/bin/env python3
"""Build the crypto-medallion narration track (design §8).

Synthesizes one clip per beat and concatenates them into a single narration
track, then reports the total duration so the take can be cut to it.

Engines (--tts):
  auto    (default) Google Cloud TTS with the active credentials, falling back
          to edge-tts for every beat if any Google call fails;
  google  Google Cloud TTS only;
  edge    edge-tts only (a real neural voice, no API key).

The engine is all-or-nothing per run: a mid-run Google failure rebuilds the
whole track with edge-tts, so the voice never changes partway through.

Google Cloud TTS needs a quota project. It comes from --quota-project, then
$DEMO_TTS_QUOTA_PROJECT, then the ADC file's quota_project_id, then the active
gcloud project. The access token comes from `gcloud auth print-access-token`.

    python3 demo/crypto-medallion/scripts/make-narration.py [--out narration.mp3] [--tts auto]

Outputs (gitignored): narration/clips/<id>.mp3 and narration/narration.mp3.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

NARRATION_DIR = Path(__file__).resolve().parent.parent / "narration"
CLIPS_DIR = NARRATION_DIR / "clips"
TTS_ENDPOINT = "https://texttospeech.googleapis.com/v1/text:synthesize"


def ffprobe_duration(path: Path) -> float:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", str(path)],
        capture_output=True, text=True, check=True).stdout.strip()
    return float(out)


def quota_project(cli: str = "") -> str:
    if cli:
        return cli
    if os.environ.get("DEMO_TTS_QUOTA_PROJECT"):
        return os.environ["DEMO_TTS_QUOTA_PROJECT"]
    adc = Path.home() / ".config/gcloud/application_default_credentials.json"
    if adc.exists():
        try:
            qp = json.loads(adc.read_text()).get("quota_project_id")
            if qp:
                return qp
        except Exception:
            pass
    try:
        return subprocess.run(["gcloud", "config", "get-value", "project"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return ""


def access_token() -> str:
    try:
        return subprocess.run(["gcloud", "auth", "print-access-token"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return ""


def rate_pct(rate: str) -> float:
    try:
        return float(rate.strip().rstrip("%"))
    except ValueError:
        return 0.0


def synth_google(text: str, voice: str, rate: str, quota: str) -> bytes:
    """Synthesize one beat as plain text (Journey voices reject SSML). Mark times
    are estimated from word position by estimate_marks."""
    if not quota:
        raise RuntimeError("no Google TTS quota project")
    token = access_token()
    if not token:
        raise RuntimeError("no gcloud access token")
    lang = "-".join(voice.split("-")[:2])  # en-US-Journey-F -> en-US
    body = {"input": {"text": text},
            "voice": {"languageCode": lang, "name": voice},
            "audioConfig": {"audioEncoding": "MP3",
                            "speakingRate": max(0.25, min(4.0, 1.0 + rate_pct(rate) / 100.0))}}
    req = urllib.request.Request(TTS_ENDPOINT, data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("x-goog-user-project", quota)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode(errors='replace')[:240]}")
    if "audioContent" not in payload:
        raise RuntimeError(f"no audioContent: {str(payload)[:200]}")
    return base64.b64decode(payload["audioContent"])


def estimate_marks(beat: dict, duration: float) -> dict:
    """Approximate each <mark> time (seconds) by its word position in the beat."""
    ssml = beat.get("ssml") or beat["text"]
    token = re.compile(r'<mark name="([^"]+)"/>')
    prefixes = {}
    parts = []
    pos = 0
    for m in token.finditer(ssml):
        parts.append(ssml[pos:m.start()])
        prefixes[m.group(1)] = "".join(parts)
        pos = m.end()
    parts.append(ssml[pos:])
    full = re.sub(r"<[^>]+>", "", "".join(parts))
    total = max(1, len(full.split()))
    return {name: round(duration * len(re.sub(r"<[^>]+>", "", p).split()) / total, 3)
            for name, p in prefixes.items()}


def synth_edge(text: str, voice: str, rate: str) -> bytes:
    tmp = CLIPS_DIR / ".edge.mp3"
    cmd = [sys.executable, "-m", "edge_tts", "--voice", voice, f"--rate={rate}",
           "--text", text, "--write-media", str(tmp)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip())
    data = tmp.read_bytes()
    tmp.unlink(missing_ok=True)
    return data


def apply_pronounce(text: str, mapping: dict) -> str:
    """Respell brand/technical words the TTS voice would say wrong."""
    for k, v in (mapping or {}).items():
        text = re.sub(re.escape(k), v, text, flags=re.IGNORECASE)
    return text


def synth_all(engine: str, beats: list, google_voice: str, edge_voice: str,
              rate: str, quota: str, pronounce: dict):
    """Return (clips, {beat_id: {mark: seconds}})."""
    clips = []
    marks_by_beat = {}
    for beat in beats:
        clip = CLIPS_DIR / f"{beat['id']}.mp3"
        say = apply_pronounce(beat["text"], pronounce)
        last = None
        for attempt in range(3):
            try:
                data = (synth_google(say, google_voice, rate, quota)
                        if engine == "google"
                        else synth_edge(say, edge_voice, rate))
                clip.write_bytes(data)
                last = None
                break
            except Exception as exc:  # retry a transient quota/propagation 403
                last = exc
                time.sleep(1.5 * (attempt + 1))
        if last is not None:
            raise RuntimeError(f"{beat['id']}: {last}")
        clips.append(clip)
        marks_by_beat[beat["id"]] = estimate_marks(beat, ffprobe_duration(clip))
        print(f"  {beat['id']}: {ffprobe_duration(clip):.1f}s  {beat['text'][:48]}…")
    return clips, marks_by_beat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(NARRATION_DIR / "narration.mp3"))
    ap.add_argument("--beats", default=str(NARRATION_DIR / "beats.json"))
    ap.add_argument("--tts", choices=["auto", "google", "edge"], default="auto")
    ap.add_argument("--quota-project", default="")
    args = ap.parse_args()

    spec = json.loads(Path(args.beats).read_text())
    beats = spec["beats"]
    edge_voice = spec.get("voice", "en-US-AriaNeural")
    google_voice = spec.get("google_voice", "en-US-Journey-F")
    rate = spec.get("rate", "+0%")
    pronounce = spec.get("pronounce", {})
    quota = quota_project(args.quota_project)
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)

    engine = args.tts
    marks_by_beat = {}
    if engine in ("auto", "google"):
        if not quota:
            if engine == "google":
                raise SystemExit("--tts google needs a quota project (--quota-project or ADC)")
            engine = "edge"
        else:
            try:
                print(f"google TTS: voice={google_voice} quota={quota}")
                clips, marks_by_beat = synth_all("google", beats, google_voice,
                                                 edge_voice, rate, quota, pronounce)
                engine = "google"
            except Exception as exc:
                if args.tts == "google":
                    raise
                print(f"google TTS failed ({exc}); rebuilding every beat with edge-tts")
                engine = "edge"
    if engine == "edge":
        clips, marks_by_beat = synth_all("edge", beats, google_voice, edge_voice, rate, quota, pronounce)

    (NARRATION_DIR / "timepoints.json").write_text(
        json.dumps({"engine": engine, "beats": marks_by_beat}, indent=2) + "\n")

    listing = CLIPS_DIR / "concat.txt"
    listing.write_text("".join(f"file '{c.resolve()}'\n" for c in clips))
    out = Path(args.out)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat",
                    "-safe", "0", "-i", str(listing), "-c:a", "libmp3lame",
                    "-q:a", "4", str(out)], check=True)
    print(f"narration: {out} ({ffprobe_duration(out):.1f}s, engine={engine}, {len(clips)} beats)")


if __name__ == "__main__":
    main()
