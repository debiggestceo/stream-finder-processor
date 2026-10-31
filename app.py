
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

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
PROCESSOR_SECRET = os.environ.get("PROCESSOR_SECRET")

CALLBACK_URL = (
    "https://stream-finder-bot.stfualmceo.workers.dev/processor-chunk"
)

GROQ_API = "https://api.groq.com/openai/v1/audio/transcriptions"

WHISPER_MODEL = "whisper-large-v3-turbo"
CHUNK_SECONDS = 600

active_chunks = set()
active_lock = threading.Lock()


def format_time(seconds):
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def authorized(req):
    if not PROCESSOR_SECRET:
        return False

    supplied = req.headers.get("X-Processor-Secret", "")

    return (
        len(supplied) == len(PROCESSOR_SECRET)
        and __import__("hmac").compare_digest(
            supplied,
            PROCESSOR_SECRET
        )
    )


def extract_video_id(url):
    match = re.search(
        r"kick\.com/[^/\s]+/videos/([a-fA-F0-9-]{36})",
        url
    )

    if not match:
        raise ValueError("Не удалось определить ID видео Kick.")

    return match.group(1)


def get_kick_video_url(video_id):
    api_url = (
        f"https://web.kick.com/api/v1/stream/"
        f"{video_id}/playback"
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://kick.com",
        "Referer": "https://kick.com/"
    }

    payload = {
        "video_player": {"player": {}},
        "video_session": {},
        "user_session": {
            "non_personalised_ads": True
        }
    }

    response = requests.post(
        api_url,
        headers=headers,
        json=payload,
        timeout=40
    )

    response.raise_for_status()

    data = response.json()
    playback = data.get("playback_url") or {}

    playlist_url = playback.get("vod") or playback.get("dvr")

    if not playlist_url:
        raise Exception(
            "Kick не вернул ссылку на VOD: "
            + str(data)[:1000]
        )

    return playlist_url


def run_ffmpeg(command, timeout=900):
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout
    )

    if result.returncode != 0:
        raise Exception(
            "FFmpeg error: " + result.stderr[-3000:]
        )

    return result


def download_audio_chunk(playlist_url, output_file, offset):
    command = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel", "error",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "10",
        "-user_agent", "Mozilla/5.0",
        "-referer", "https://kick.com/",
        "-ss", str(offset),
        "-i", playlist_url,
        "-t", str(CHUNK_SECONDS),
        "-map", "0:a:0",
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-c:a", "libmp3lame",
        "-b:a", "48k",
        output_file
    ]

    run_ffmpeg(command)

    if not os.path.exists(output_file):
        return False

    return os.path.getsize(output_file) > 0


def transcribe_chunk(chunk_file, offset):
    if not GROQ_API_KEY:
        raise Exception("GROQ_API_KEY не настроен на Render.")

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}"
    }

    data = {
        "model": WHISPER_MODEL,
        "response_format": "verbose_json",
        "language": "ru",
        "temperature": "0",
        "timestamp_granularities[]": "segment"
    }

    last_error = None

    for attempt in range(3):
        try:
            with open(chunk_file, "rb") as audio:
                response = requests.post(
                    GROQ_API,
                    headers=headers,
                    data=data,
                    files={
                        "file": (
                            os.path.basename(chunk_file),
                            audio,
                            "audio/mpeg"
                        )
                    },
                    timeout=300
                )

            response.raise_for_status()
            result = response.json()

            segments = []

            for item in result.get("segments", []):
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

            if not segments and result.get("text"):
                text = result["text"].strip()

                if text:
                    segments.append({
                        "start": offset,
                        "end": offset,
                        "time": format_time(offset),
                        "text": text
                    })

            return segments

        except Exception as error:
            last_error = error
            log.exception("Whisper attempt failed")

            if attempt < 2:
                time.sleep(5 * (attempt + 1))

    raise Exception(f"Ошибка расшифровки: {last_error}")


def send_callback(payload):
    if not PROCESSOR_SECRET:
        raise Exception("PROCESSOR_SECRET не настроен на Render.")

    response = requests.post(
        CALLBACK_URL,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "X-Processor-Secret": PROCESSOR_SECRET
        },
        timeout=120
    )

    response.raise_for_status()
    log.info("Callback delivered")


def process_chunk_job(url, chat_id, job_id, chunk_index):
    lock_key = (str(job_id), int(chunk_index))

    with active_lock:
        if lock_key in active_chunks:
            log.info("Chunk already running: %s", lock_key)
            return

        active_chunks.add(lock_key)

    try:
        offset = int(chunk_index) * CHUNK_SECONDS

        log.info(
            "Starting job=%s chunk=%s offset=%s",
            job_id,
            chunk_index,
            offset
        )

        video_id = extract_video_id(url)
        playlist_url = get_kick_video_url(video_id)

        with tempfile.TemporaryDirectory(
            prefix="stream_finder_"
        ) as temp_dir:

            audio_file = os.path.join(
                temp_dir,
                "chunk.mp3"
            )

            has_audio = download_audio_chunk(
                playlist_url,
                audio_file,
                offset
            )

            if not has_audio:
                send_callback({
                    "job_id": str(job_id),
                    "chat_id": str(chat_id),
                    "chunk_index": int(chunk_index),
                    "status": "completed",
                    "segments": []
                })

                log.info("No more audio. Job completed.")
                return

            segments = transcribe_chunk(
                audio_file,
                offset
            )

        send_callback({
            "job_id": str(job_id),
            "chat_id": str(chat_id),
            "chunk_index": int(chunk_index),
            "status": "chunk_completed",
            "segments": segments
        })

        log.info(
            "Chunk completed: job=%s chunk=%s segments=%s",
            job_id,
            chunk_index,
            len(segments)
        )

    except Exception as error:
        log.exception("Chunk processing failed")

        try:
            send_callback({
                "job_id": str(job_id),
                "chat_id": str(chat_id),
                "chunk_index": int(chunk_index),
                "status": "error",
                "error": str(error)
            })
        except Exception:
            log.exception("Error callback failed")

    finally:
        with active_lock:
            active_chunks.discard(lock_key)


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Stream Finder Processor is online",
        "groq_configured": bool(GROQ_API_KEY),
        "processor_secret_configured": bool(PROCESSOR_SECRET)
    })


@app.route("/process", methods=["POST"])
@app.route("/process-chunk", methods=["POST"])
def process():
    if not authorized(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}

    url = data.get("url")
    chat_id = data.get("chat_id")
    job_id = data.get("job_id")
    chunk_index = data.get("chunk_index", 0)

    if not url or not chat_id or not job_id:
        return jsonify({
            "error": "Missing url, chat_id or job_id"
        }), 400

    try:
        chunk_index = int(chunk_index)
        if chunk_index < 0:
            raise ValueError()
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid chunk_index"}), 400

    if "kick.com/" not in url.lower():
        return jsonify({
            "error": "Only Kick VOD links are supported"
        }), 400

    lock_key = (str(job_id), chunk_index)

    with active_lock:
        if lock_key in active_chunks:
            return jsonify({
                "status": "already_processing",
                "job_id": job_id,
                "chunk_index": chunk_index
            }), 202

    thread = threading.Thread(
        target=process_chunk_job,
        args=(url, chat_id, job_id, chunk_index),
        daemon=True
    )

    thread.start()

    return jsonify({
        "status": "processing",
        "job_id": job_id,
        "chunk_index": chunk_index
    }), 202


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
