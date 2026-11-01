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
from yt_dlp.utils import download_range_func

from flask import Flask, request, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stream-finder")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
VISION_SHARED_SECRET = os.getenv("PROCESSOR_SECRET")
VISION_ENABLED = os.getenv("VISION_ENABLED", "true").lower() == "true"
VISION_MODEL = os.getenv("VISION_MODEL", "@cf/qwen/qwen3.8-27b")
VISION_FRAME_INTERVAL = 120
PROCESSOR_SECRET = os.getenv("PROCESSOR_SECRET")
WORKER_URL = "https://stream-finder-bot.stfualmceo.workers.dev"

CHUNK_SECONDS = 600

active_chunks = set()
cancelled_jobs = set()
active_lock = threading.Lock()

# Cache metadata only; never keep complete VOD media files on local disk.
video_metadata = {}
metadata_locks = {}
metadata_locks_guard = threading.Lock()
download_slots = threading.Semaphore(2)
MAX_TEMP_BYTES = 600 * 1024 * 1024

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


def get_video_info(job_id, video_url):
    with metadata_locks_guard:
        lock = metadata_locks.setdefault(job_id, threading.Lock())

    with lock:
        cached = video_metadata.get(job_id)
        if cached:
            return cached

        options = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "skip_download": True,
            "socket_timeout": 40,
            "retries": 5,
        }

        log.info("Reading video metadata: job=%s platform=%s", job_id, detect_platform(video_url))
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(video_url, download=False)

        if not info:
            raise RuntimeError("Не удалось получить информацию о видео")
        if info.get("_type") == "playlist":
            raise RuntimeError("Отправь ссылку на отдельное видео, не плейлист")

        duration = info.get("duration")
        if not duration or float(duration) <= 0:
            raise RuntimeError("Не удалось определить длительность видео")

        result = {
            "duration": float(duration),
            "platform": detect_platform(video_url),
            "title": info.get("title", "Видео")
        }
        video_metadata[job_id] = result
        log.info("Video metadata ready: job=%s duration=%.2f", job_id, result["duration"])
        return result


def download_audio_chunk(job_id, video_url, chunk_index, offset, duration):
    folder = tempfile.mkdtemp(prefix=f"stream_{job_id}_{chunk_index}_")

    def guard_temp_size(_status):
        total = 0
        try:
            for name in os.listdir(folder):
                path = os.path.join(folder, name)
                if os.path.isfile(path):
                    total += os.path.getsize(path)
        except FileNotFoundError:
            return
        if total > MAX_TEMP_BYTES:
            raise RuntimeError(
                "Загрузка фрагмента остановлена: временный файл превысил безопасный лимит 1.5 ГБ"
            )

    options = {
        # Keep a muxed video+audio source: the visual analyzer needs video frames.
        # Cap resolution to keep temporary downloads within the worker's disk budget.
        "format": "best[height<=360]/bestvideo[height<=360]+bestaudio/best[height<=480]",
        "outtmpl": os.path.join(folder, "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 40,
        "retries": 5,
        "fragment_retries": 5,
        "file_access_retries": 3,
        "concurrent_fragment_downloads": 2,
        "continuedl": True,
        "nopart": True,
        "overwrites": False,
        # yt-dlp downloads only this VOD interval instead of the complete recording.
        "download_ranges": download_range_func(None, [(offset, offset + duration)]),
        "progress_hooks": [guard_temp_size],
    }

    try:
        log.info(
            "Downloading ranged chunk: job=%s chunk=%s range=%s-%s",
            job_id, chunk_index, format_time(offset), format_time(offset + duration)
        )

        with download_slots:
            with yt_dlp.YoutubeDL(options) as ydl:
                ydl.extract_info(video_url, download=True)

        candidates = []
        for name in os.listdir(folder):
            path = os.path.join(folder, name)
            if os.path.isfile(path) and not name.endswith((".part", ".ytdl")):
                candidates.append(path)

        if not candidates:
            raise RuntimeError("yt-dlp не создал файл для выбранного фрагмента")

        filepath = max(candidates, key=os.path.getsize)
        size = os.path.getsize(filepath)
        if size < 1000:
            raise RuntimeError("Загруженный фрагмент пустой или повреждён")

        log.info(
            "Ranged chunk downloaded: job=%s chunk=%s size=%s",
            job_id, chunk_index, size
        )
        return folder, filepath

    except Exception:
        shutil.rmtree(folder, ignore_errors=True)
        raise


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


VISION_PROMPT = (
    "Ты анализируешь отдельный кадр записи стрима. Определи, что видно на экране: "
    "игра, видео в браузере/плеере, сайт, человек или приложение. Для игр используй "
    "узнаваемый интерфейс, HUD, персонажей, локации и графику; если уверенно узнаёшь игру, "
    "назови её. Для видео ищи заголовок, автора, субтитры и узнаваемые детали. "
    "Не путай просмотр игры с тем, что сам стример играет. Верни короткую конкретную "
    "метку на русском. Не додумывай детали. Если определить нельзя, ответь строго NONE."
)


def analyze_frame_groq(image_base64):
    """Analyze one frame with Groq's free-plan vision model using the existing Groq key."""
    if not GROQ_API_KEY:
        return None, "not_configured"
    model = os.getenv("GROQ_VISION_MODEL", "qwen/qwen3.8-27b")
    endpoint = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": model,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": VISION_PROMPT},
                {"type": "image_url", "image_url": {
                    "url": "data:image/jpeg;base64," + image_base64
                }}
            ]
        }],
        "temperature": 0.1,
        "max_completion_tokens": 160
    }
    try:
        response = requests.post(
            endpoint,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                     "Content-Type": "application/json"},
            json=payload,
            timeout=90
        )
        if response.status_code == 429:
            log.warning("Groq vision free quota/rate limit reached; stopping scan. No paid fallback.")
            return None, "quota"
        if response.status_code in (401, 403):
            log.error("Groq vision authorization/access error (HTTP %s)", response.status_code)
            return None, "configuration"
        response.raise_for_status()
        data = response.json()
        answer = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        if isinstance(answer, list):
            answer = " ".join(p.get("text", "") for p in answer if isinstance(p, dict))
        return (answer.strip() if isinstance(answer, str) else "") or "NONE", "ok"
    except requests.RequestException as error:
        log.warning("Groq vision request failed: %s", str(error)[:300])
        return None, "error"
    except (ValueError, KeyError, IndexError, TypeError) as error:
        log.warning("Groq vision response parse failed: %s", str(error)[:200])
        return None, "error"


def analyze_video_frames(source_path, offset, duration):
    """Use Groq vision first, with Cloudflare as a free fallback only for transient errors."""
    if not VISION_ENABLED:
        log.error("Vision analysis disabled by VISION_ENABLED=false")
        return []
    if not VISION_SHARED_SECRET:
        log.error("Vision analysis disabled: PROCESSOR_SECRET is missing from Render")
        return []

    positions = [min(duration - 1, t) for t in range(0, max(1, int(duration)), VISION_FRAME_INTERVAL)]
    results = []
    attempted = extracted = empty = 0
    provider = "Groq" if GROQ_API_KEY else "Cloudflare"
    log.info("Vision started: provider=%s offset=%s frames=%s interval=%ss",
             provider, format_time(offset), len(positions), VISION_FRAME_INTERVAL)

    for local_time in positions:
        attempted += 1
        frame = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        frame_path = frame.name
        frame.close()
        try:
            command = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                       "-ss", str(local_time), "-i", source_path,
                       "-frames:v", "1", "-vf", "scale=768:-2", "-q:v", "4", frame_path]
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
            if result.returncode != 0 or not os.path.exists(frame_path) or os.path.getsize(frame_path) < 1000:
                log.warning("Frame extraction failed at %s", format_time(offset + local_time))
                continue
            extracted += 1
            import base64
            with open(frame_path, "rb") as image_file:
                encoded = base64.b64encode(image_file.read()).decode("ascii")

            description, groq_status = (None, "not_configured")
            if GROQ_API_KEY:
                description, groq_status = analyze_frame_groq(encoded)
                if groq_status == "quota":
                    break

            # Cloudflare is only a free fallback for temporary/provider errors.
            if description is None and groq_status not in ("quota", "configuration"):
                response = requests.post(
                    WORKER_URL + "/vision",
                    headers={"Content-Type": "application/json",
                             "x-stream-finder-secret": VISION_SHARED_SECRET},
                    json={"image": encoded}, timeout=120)
                if response.status_code in (429, 503):
                    log.warning("Cloudflare vision limited (HTTP %s); stopping scan", response.status_code)
                    break
                if not response.ok and ("4006" in response.text or "daily free allocation" in response.text.lower()):
                    log.error("Cloudflare daily free quota exhausted. No paid fallback.")
                    break
                response.raise_for_status()
                payload = response.json()
                description = payload.get("result") if isinstance(payload, dict) else None

            if not isinstance(description, str):
                continue
            description = " ".join(description.strip().split())
            if not description or description.upper() in ("NONE", "НЕТ", "НЕ ОПРЕДЕЛЕНО"):
                empty += 1
                continue

            absolute_time = offset + local_time
            log.info("Vision recognized at %s: %s", format_time(absolute_time), description[:140])
            results.append({"start": absolute_time, "end": absolute_time,
                            "time": format_time(absolute_time),
                            "text": "[ВИДЕО-МЕТКА] " + description[:180]})
        except Exception:
            log.exception("Vision analysis failed at %s", format_time(offset + local_time))
        finally:
            try:
                if os.path.exists(frame_path):
                    os.remove(frame_path)
            except OSError:
                pass

    log.info("Vision finished: provider=%s model=%s offset=%s attempted=%s extracted=%s empty=%s recognized=%s",
             provider, os.getenv("GROQ_VISION_MODEL", "qwen/qwen3.8-27b"),
             format_time(offset), attempted, extracted, empty, len(results))
    return results

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
        if job_id in cancelled_jobs:
            log.info("Job cancelled before callback: %s", job_id)
            return

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
    chunk_folder = None
    audio_path = None

    try:
        if job_id in cancelled_jobs:
            log.info("Job cancelled before start: %s", job_id)
            return

        offset = chunk_index * CHUNK_SECONDS

        log.info(
            "Starting ranged job=%s platform=%s chunk=%s offset=%s",
            job_id,
            detect_platform(video_url),
            chunk_index,
            offset
        )

        if job_id in cancelled_jobs:
            return

        video = get_video_info(job_id, video_url)
        duration = video["duration"]

        if offset >= duration:
            log.info(
                "End of video: job=%s duration=%.2f offset=%s",
                job_id, duration, offset
            )
            video_metadata.pop(job_id, None)
            send_callback({
                "job_id": job_id,
                "chat_id": chat_id,
                "chunk_index": chunk_index,
                "status": "completed",
                "segments": []
            })
            return

        chunk_duration = min(CHUNK_SECONDS, duration - offset)
        if job_id in cancelled_jobs:
            return

        chunk_folder, source_path = download_audio_chunk(
            job_id, video_url, chunk_index, offset, chunk_duration
        )

        if job_id in cancelled_jobs:
            return

        audio_path = extract_audio_chunk(source_path, 0, chunk_duration)
        segments = transcribe_chunk(audio_path, offset)

        if job_id in cancelled_jobs:
            return

        # Visual analysis runs on the downloaded chunk before it is deleted.
        video_segments = analyze_video_frames(source_path, offset, chunk_duration)
        segments.extend(video_segments)
        segments.sort(key=lambda item: float(item.get("start", 0)))

        log.info(
            "Chunk completed: job=%s chunk=%s audio_segments=%s video_segments=%s",
            job_id,
            chunk_index,
            len(segments) - len(video_segments),
            len(video_segments)
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
        if audio_path and os.path.exists(audio_path):
            try:
                os.remove(audio_path)
            except OSError:
                log.warning("Could not remove temporary audio: %s", audio_path)

        if chunk_folder:
            shutil.rmtree(chunk_folder, ignore_errors=True)

        with active_lock:
            active_chunks.discard(key)


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Stream Finder Processor is online",
        "groq_configured": bool(GROQ_API_KEY),
        "vision_enabled": bool(VISION_SHARED_SECRET) and VISION_ENABLED,
        "vision_frame_interval_seconds": VISION_FRAME_INTERVAL,
        "vision_provider": "groq" if GROQ_API_KEY else "cloudflare",
        "vision_model": os.getenv("GROQ_VISION_MODEL", "qwen/qwen3.8-27b") if GROQ_API_KEY else VISION_MODEL,
        "processor_secret_configured": bool(PROCESSOR_SECRET),
        "platforms": ["Kick", "Twitch", "YouTube"]
    })


@app.route("/cancel-job", methods=["POST"])
def cancel_job():
    if not PROCESSOR_SECRET:
        return jsonify({"error": "Processor secret is not configured"}), 500
    if request.headers.get("X-Processor-Secret") != PROCESSOR_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    job_id = str(data.get("job_id") or "")
    if not job_id:
        return jsonify({"error": "Missing job_id"}), 400

    cancelled_jobs.add(job_id)
    log.info("Cancellation requested for job=%s", job_id)
    return jsonify({"status": "cancellation_requested", "job_id": job_id})


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
