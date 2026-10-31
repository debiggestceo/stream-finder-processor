
import os
import re
import time
import shutil
import logging
import tempfile
import threading
import subprocess

import requests
import yt_dlp

from flask import Flask, request, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stream-finder")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
PROCESSOR_SECRET = os.getenv("PROCESSOR_SECRET")
WORKER_URL = "https://stream-finder-bot.stfualmceo.workers.dev"

CHUNK_SECONDS = 600

active_chunks = set()
active_lock = threading.Lock()

# Кэш целиком скачанных видео. Ключ — job_id.
jobs = {}
jobs_lock = threading.Lock()


def format_time(seconds):
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def validate_video_url(url):
    if not isinstance(url, str):
        raise ValueError("Ссылка должна быть текстом")

    url = url.strip()

    if not url.startswith(("https://", "http://")):
        raise ValueError("Нужна ссылка, начинающаяся с https://")

    host = re.sub(r"^www\.", "", url.split("/")[2].lower())

    allowed = (
        host == "kick.com"
        or host.endswith(".kick.com")
        or host == "twitch.tv"
        or host.endswith(".twitch.tv")
        or host == "youtube.com"
        or host.endswith(".youtube.com")
        or host == "youtu.be"
    )

    if not allowed:
        raise ValueError("Поддерживаются только Kick, Twitch и YouTube")

    return url


def detect_platform(url):
    host = url.split("/")[2].lower()

    if "kick.com" in host:
        return "Kick"

    if "twitch.tv" in host:
        return "Twitch"

    return "YouTube"


def download_full_audio(job_id, video_url):
    folder = tempfile.mkdtemp(prefix=f"stream_{job_id}_")

    options = {
        "format": "bestaudio/best",
        "outtmpl": os.path.join(folder, "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 40,
        "retries": 10,
        "fragment_retries": 10,
        "file_access_retries": 3,
        "concurrent_fragment_downloads": 4,
        "continuedl": True,
        "nopart": True,
        "overwrites": False,
    }

    try:
        log.info("Downloading full %s video", detect_platform(video_url))

        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(video_url, download=True)

            if not info:
                raise RuntimeError("Не удалось получить информацию о видео")

            if info.get("_type") == "playlist":
                raise RuntimeError("Отправь ссылку на отдельное видео, не плейлист")

            duration = info.get("duration")

            if not duration or float(duration) <= 0:
                raise RuntimeError("Не удалось определить длительность видео")

            filepath = None

            requested = info.get("requested_downloads") or []
            if requested:
                filepath = requested[0].get("filepath")

            if not filepath:
                filepath = info.get("_filename")

            if not filepath:
                filepath = ydl.prepare_filename(info)

            if not os.path.isfile(filepath):
                candidates = [
                    os.path.join(folder, name)
                    for name in os.listdir(folder)
                    if os.path.isfile(os.path.join(folder, name))
                ]

                if candidates:
                    filepath = max(candidates, key=os.path.getsize)

            if not filepath or not os.path.isfile(filepath):
                raise RuntimeError("Загруженный аудиофайл не найден")

            if os.path.getsize(filepath) < 1000:
                raise RuntimeError("Загруженный файл пустой или повреждён")

            log.info(
                "Full download completed: duration=%.2f file_size=%s",
                float(duration),
                os.path.getsize(filepath)
            )

            return {
                "folder": folder,
                "filepath": filepath,
                "duration": float(duration),
                "platform": detect_platform(video_url),
                "title": info.get("title", "Видео")
            }

    except Exception:
        shutil.rmtree(folder, ignore_errors=True)
        raise


def get_job_video(job_id, video_url):
    with jobs_lock:
        cached = jobs.get(job_id)

    if cached:
        return cached

    video = download_full_audio(job_id, video_url)

    with jobs_lock:
        # Защита от повторного создания кэша при одновременном запросе.
        existing = jobs.get(job_id)

        if existing:
            shutil.rmtree(video["folder"], ignore_errors=True)
            return existing

        jobs[job_id] = video

    return video


def extract_audio_chunk(source_path, offset, duration):
    temp = tempfile.NamedTemporaryFile(
        suffix=".mp3",
        delete=False
    )

    output_path = temp.name
    temp.close()

    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-y",
        "-ss", str(offset),
        "-i", source_path,
        "-t", str(duration),
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-b:a", "48k",
        "-f", "mp3",
        output_path
    ]

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=300
    )

    if result.returncode != 0:
        error = result.stderr.decode(errors="ignore")[-2000:]
        if os.path.exists(output_path):
            os.remove(output_path)
        raise RuntimeError("Ошибка нарезки аудио: " + error)

    if not os.path.isfile(output_path) or os.path.getsize(output_path) < 1000:
        if os.path.exists(output_path):
            os.remove(output_path)
        raise RuntimeError("Не удалось создать аудиофрагмент")

    return output_path


def transcribe_chunk(audio_path, offset):
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY не настроен")

    endpoint = "https://api.groq.com/openai/v1/audio/transcriptions"

    for attempt in range(3):
        try:
            with open(audio_path, "rb") as audio:
                response = requests.post(
                    endpoint,
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
    if not PROCESSOR_SECRET:
        raise RuntimeError("PROCESSOR_SECRET не настроен")

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
    log.info("Callback delivered: status=%s", payload.get("status"))


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


def cleanup_job(job_id):
    with jobs_lock:
        video = jobs.pop(job_id, None)

    if video:
        shutil.rmtree(video["folder"], ignore_errors=True)
        log.info("Temporary files removed: job=%s", job_id)


def process_chunk_job(job_id, chat_id, video_url, chunk_index):
    key = f"{job_id}:{chunk_index}"

    try:
        offset = chunk_index * CHUNK_SECONDS

        log.info(
            "Starting job=%s platform=%s chunk=%s offset=%s",
            job_id,
            detect_platform(video_url),
            chunk_index,
            offset
        )

        video = get_job_video(job_id, video_url)
        duration = video["duration"]

        # Видео закончилось: никаких новых скачиваний.
        if offset >= duration:
            log.info(
                "End of video: duration=%.2f offset=%s",
                duration,
                offset
            )

            send_callback({
                "job_id": job_id,
                "chat_id": chat_id,
                "chunk_index": chunk_index,
                "status": "completed",
                "segments": []
            })

            cleanup_job(job_id)
            return

        chunk_duration = min(CHUNK_SECONDS, duration - offset)

        log.info(
            "Extracting audio: chunk=%s duration=%.2f",
            chunk_index,
            chunk_duration
        )

        audio_path = extract_audio_chunk(
            video["filepath"],
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
            job_id,
            chunk_index,
            len(segments)
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
        "processor_secret_configured": bool(PROCESSOR_SECRET),
        "platforms": ["Kick", "Twitch", "YouTube"]
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

        video_url = validate_video_url(video_url)

    except (TypeError, ValueError) as error:
        return jsonify({"error": str(error)}), 400

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
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 10000))
    )
