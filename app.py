
import os
import re
import time
import logging
import tempfile
import threading
import subprocess
import requests

from flask import Flask, request, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stream-finder")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
PROCESSOR_SECRET = os.getenv("PROCESSOR_SECRET")
WORKER_URL = "https://stream-finder-bot.stfualmceo.workers.dev"
KICK_API = "https://web.kick.com/api/v1/stream/{}/playback"

CHUNK_SECONDS = 600
active_chunks = set()
active_lock = threading.Lock()


def format_time(seconds):
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def get_video_id(url):
    match = re.search(
        r"kick\.com/video/([a-fA-F0-9-]+)",
        url
    )
    if not match:
        match = re.search(
            r"kick\.com/[^/]+/videos/([a-fA-F0-9-]+)",
            url
        )
    if not match:
        raise ValueError("Не удалось определить ID Kick VOD")
    return match.group(1)


def get_playback_url(video_url):
    video_id = get_video_id(video_url)

    response = requests.post(
        KICK_API.format(video_id),
        json={
            "video_player": {"player": {}},
            "video_session": {},
            "user_session": {"non_personalised_ads": True}
        },
        headers={
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json"
        },
        timeout=30
    )
    response.raise_for_status()

    data = response.json()
    playback = data.get("playback_url", {})

    playlist = playback.get("vod") or playback.get("dvr")

    if not playlist:
        raise RuntimeError("Kick не вернул ссылку на видео")

    return playlist


def resolve_media_playlist(url):
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30
    )
    response.raise_for_status()

    content = response.text

    # Если это master playlist, выбираем вариант с максимальной пропускной способностью.
    if "#EXT-X-STREAM-INF" in content:
        lines = content.splitlines()
        variants = []

        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                bandwidth_match = re.search(r"BANDWIDTH=(\d+)", line)
                bandwidth = int(bandwidth_match.group(1)) if bandwidth_match else 0

                for next_line in lines[i + 1:]:
                    if next_line.strip() and not next_line.startswith("#"):
                        from urllib.parse import urljoin
                        variants.append((bandwidth, urljoin(url, next_line.strip())))
                        break

        if not variants:
            raise RuntimeError("Не удалось найти видеопоток в playlist")

        variants.sort(key=lambda x: x[0], reverse=True)
        return variants[0][1]

    return url


def get_video_duration(playlist_url):
    media_url = resolve_media_playlist(playlist_url)

    response = requests.get(
        media_url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30
    )
    response.raise_for_status()

    content = response.text

    if "#EXTM3U" not in content:
        raise RuntimeError("Получен некорректный HLS playlist")

    durations = re.findall(
        r"#EXTINF:([0-9]+(?:\.[0-9]+)?)",
        content
    )

    if not durations:
        raise RuntimeError("Не удалось определить длительность видео")

    duration = sum(float(x) for x in durations)

    # Для конечного VOD обязательно ожидаем маркер конца плейлиста.
    if "#EXT-X-ENDLIST" not in content:
        raise RuntimeError(
            "Kick вернул незавершённый playlist. "
            "Не запускаю бесконечную обработку."
        )

    log.info("VOD duration: %.2f seconds", duration)
    return media_url, duration


def download_audio_chunk(playlist_url, offset, duration):
    temp = tempfile.NamedTemporaryFile(
        suffix=".mp3",
        delete=False
    )
    output_path = temp.name
    temp.close()

    command = [
        "ffmpeg",
        "-y",
        "-ss", str(offset),
        "-i", playlist_url,
        "-t", str(duration),
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-b:a", "48k",
        output_path
    ]

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=900
    )

    if result.returncode != 0:
        error = result.stderr.decode(errors="ignore")[-2000:]
        raise RuntimeError("FFmpeg error: " + error)

    if not os.path.exists(output_path) or os.path.getsize(output_path) < 1000:
        raise RuntimeError("Не удалось скачать аудио этого фрагмента")

    return output_path


def transcribe_chunk(audio_path, offset):
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY не настроен")

    url = "https://api.groq.com/openai/v1/audio/transcriptions"

    for attempt in range(3):
        try:
            with open(audio_path, "rb") as audio:
                response = requests.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}"
                    },
                    data={
                        "model": "whisper-large-v3-turbo",
                        "response_format": "verbose_json",
                        "timestamp_granularities[]": "segment"
                    },
                    files={
                        "file": ("audio.mp3", audio, "audio/mpeg")
                    },
                    timeout=600
                )

            response.raise_for_status()
            data = response.json()

            segments = []

            for item in data.get("segments", []):
                start = float(item.get("start", 0)) + offset
                end = float(item.get("end", 0)) + offset
                text = item.get("text", "").strip()

                if text:
                    segments.append({
                        "start": start,
                        "end": end,
                        "time": format_time(start),
                        "text": text
                    })

            return segments

        except Exception:
            if attempt == 2:
                raise
            time.sleep(3 * (attempt + 1))


def send_callback(payload):
    response = requests.post(
        WORKER_URL + "/processor-chunk",
        json=payload,
        headers={
            "X-Processor-Secret": PROCESSOR_SECRET,
            "Content-Type": "application/json"
        },
        timeout=60
    )
    response.raise_for_status()
    log.info("Callback delivered")


def send_error(job_id, chat_id, chunk_index, error):
    try:
        send_callback({
            "job_id": job_id,
            "chat_id": chat_id,
            "chunk_index": chunk_index,
            "status": "error",
            "error": str(error)[:1000]
        })
    except Exception:
        log.exception("Could not deliver error callback")


def process_chunk_job(job_id, chat_id, video_url, chunk_index):
    key = f"{job_id}:{chunk_index}"

    try:
        log.info(
            "Starting job=%s chunk=%s offset=%s",
            job_id, chunk_index, chunk_index * CHUNK_SECONDS
        )

        offset = chunk_index * CHUNK_SECONDS

        playlist_url = get_playback_url(video_url)
        media_url, total_duration = get_video_duration(playlist_url)

        # Главное исправление: достигли конца видео — завершаем задание.
        if offset >= total_duration:
            log.info(
                "End of VOD reached: offset=%s duration=%s",
                offset, total_duration
            )

            send_callback({
                "job_id": job_id,
                "chat_id": chat_id,
                "chunk_index": chunk_index,
                "status": "completed",
                "segments": []
            })
            return

        remaining = total_duration - offset
        chunk_duration = min(CHUNK_SECONDS, remaining)

        log.info(
            "Downloading chunk=%s duration=%.2f",
            chunk_index, chunk_duration
        )

        audio_path = download_audio_chunk(
            media_url,
            offset,
            chunk_duration
        )

        try:
            segments = transcribe_chunk(audio_path, offset)
        finally:
            if os.path.exists(audio_path):
                os.remove(audio_path)

        log.info(
            "Chunk completed: job=%s chunk=%s segments=%s",
            job_id, chunk_index, len(segments)
        )

        send_callback({
            "job_id": job_id,
            "chat_id": chat_id,
            "chunk_index": chunk_index,
            "status": "chunk_completed",
            "segments": segments
        })

    except Exception as error:
        log.exception("Processing failed")
        send_error(job_id, chat_id, chunk_index, error)

    finally:
        with active_lock:
            active_chunks.discard(key)


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Stream Finder Processor is online",
        "groq_configured": bool(GROQ_API_KEY),
        "processor_secret_configured": bool(PROCESSOR_SECRET)
    })


@app.route("/process-chunk", methods=["POST"])
def process_chunk():
    if not PROCESSOR_SECRET:
        return jsonify({"error": "Processor secret is not configured"}), 500

    if request.headers.get("X-Processor-Secret") != PROCESSOR_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}

    job_id = data.get("job_id")
    chat_id = data.get("chat_id")
    video_url = data.get("url")
    chunk_index = data.get("chunk_index")

    if not all([job_id, chat_id, video_url]) or chunk_index is None:
        return jsonify({"error": "Missing required fields"}), 400

    try:
        chunk_index = int(chunk_index)
        if chunk_index < 0:
            raise ValueError()
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid chunk_index"}), 400

    key = f"{job_id}:{chunk_index}"

    with active_lock:
        if key in active_chunks:
            return jsonify({
                "status": "already_processing",
                "job_id": job_id,
                "chunk_index": chunk_index
            }), 202

        active_chunks.add(key)

    thread = threading.Thread(
        target=process_chunk_job,
        args=(job_id, chat_id, video_url, chunk_index),
        daemon=True
    )
    thread.start()

    return jsonify({
        "status": "accepted",
        "job_id": job_id,
        "chunk_index": chunk_index
    }), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
