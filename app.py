
import os
import re
import glob
import json
import time
import uuid
import shutil
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

# Адрес нашего Cloudflare Worker
CALLBACK_URL = (
    "https://stream-finder-bot.stfualmceo.workers.dev/processor-result"
)

KICK_API = "https://kick.com/api/v1/video"
GROQ_API = "https://api.groq.com/openai/v1/audio/transcriptions"

WHISPER_MODEL = "whisper-large-v3-turbo"
CHUNK_SECONDS = 600


def format_time(seconds):
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def extract_video_id(url):
    match = re.search(
        r"kick\.com/[^/\s]+/videos/([a-fA-F0-9-]{36})",
        url
    )

    if not match:
        raise ValueError("Не удалось определить ID видео Kick.")

    return match.group(1)


def get_kick_video_url(video_id):
    """
    Получает HLS-ссылку на запись Kick.
    Новый endpoint: /api/v1/video/{uuid}
    """

    api_url = f"{KICK_API}/{video_id}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://kick.com/"
    }

    response = requests.get(
        api_url,
        headers=headers,
        timeout=40
    )

    response.raise_for_status()

    data = response.json()

    # Ищем HLS-ссылку в ответе Kick.
    def find_m3u8(value):
        if isinstance(value, str):
            if ".m3u8" in value:
                return value

        elif isinstance(value, dict):
            for item in value.values():
                result = find_m3u8(item)
                if result:
                    return result

        elif isinstance(value, list):
            for item in value:
                result = find_m3u8(item)
                if result:
                    return result

        return None

    playlist_url = find_m3u8(data)

    if not playlist_url:
        raise Exception(
            "Kick не вернул ссылку на видео. "
            "Возможно, запись удалена, приватная или недоступна."
        )

    log.info("Kick VOD source successfully received")

    return playlist_url


def run_ffmpeg(command, timeout=10800):
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout
    )

    if result.returncode != 0:
        error = result.stderr[-3000:] if result.stderr else "Unknown ffmpeg error"
        raise Exception(f"FFmpeg error: {error}")

    return result


def download_audio(playlist_url, output_file):
    """
    Скачивает аудио всей записи и преобразует его в MP3.
    """

    command = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel", "error",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "10",
        "-user_agent",
        "Mozilla/5.0",
        "-referer",
        "https://kick.com/",
        "-i", playlist_url,
        "-map", "0:a:0",
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-c:a", "libmp3lame",
        "-b:a", "48k",
        output_file
    ]

    log.info("Starting full audio download")

    run_ffmpeg(command)

    if not os.path.exists(output_file):
        raise Exception("Аудиофайл не был создан.")

    if os.path.getsize(output_file) == 0:
        raise Exception("Скачанный аудиофайл пустой.")

    log.info(
        "Audio downloaded: %s bytes",
        os.path.getsize(output_file)
    )


def split_audio(audio_file, output_dir):
    """
    Делит полную аудиозапись на части по 10 минут.
    """

    pattern = os.path.join(output_dir, "chunk_%04d.mp3")

    command = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel", "error",
        "-i", audio_file,
        "-map", "0:a:0",
        "-f", "segment",
        "-segment_time", str(CHUNK_SECONDS),
        "-reset_timestamps", "1",
        "-c:a", "libmp3lame",
        "-b:a", "48k",
        pattern
    ]

    run_ffmpeg(command)

    chunks = sorted(
        glob.glob(os.path.join(output_dir, "chunk_*.mp3"))
    )

    if not chunks:
        raise Exception("Не удалось разделить аудио на части.")

    log.info("Audio divided into %s chunks", len(chunks))

    return chunks


def transcribe_chunk(chunk_file, offset):
    """
    Отправляет одну часть аудио в Groq Whisper.
    """

    if not GROQ_API_KEY:
        raise Exception("На Render отсутствует GROQ_API_KEY.")

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
                files = {
                    "file": (
                        os.path.basename(chunk_file),
                        audio,
                        "audio/mpeg"
                    )
                }

                response = requests.post(
                    GROQ_API,
                    headers=headers,
                    data=data,
                    files=files,
                    timeout=300
                )

            response.raise_for_status()
            result = response.json()

            segments = []

            for segment in result.get("segments", []):
                start = float(segment.get("start", 0)) + offset
                end = float(segment.get("end", start - offset)) + offset
                text = segment.get("text", "").strip()

                if not text:
                    continue

                segments.append({
                    "start": start,
                    "end": end,
                    "time": format_time(start),
                    "text": text
                })

            # Если Whisper вернул только общий текст.
            if not segments and result.get("text"):
                text = result["text"].strip()

                if text:
                    segments.append({
                        "start": offset,
                        "end": offset,
                        "time": format_time(offset),
                        "text": text
                    })

            log.info(
                "Chunk transcribed: %s segments",
                len(segments)
            )

            return segments

        except Exception as error:
            last_error = error

            log.exception(
                "Transcription attempt %s failed",
                attempt + 1
            )

            if attempt < 2:
                time.sleep(5 * (attempt + 1))

    raise Exception(
        f"Не удалось расшифровать аудиочасть: {last_error}"
    )


def send_callback(payload):
    """
    Отправляет результат обратно в Cloudflare Worker.
    """

    response = requests.post(
        CALLBACK_URL,
        json=payload,
        headers={
            "Content-Type": "application/json"
        },
        timeout=120
    )

    response.raise_for_status()

    log.info("Callback successfully delivered")


def process_job(url, chat_id, job_id):
    """
    Полный цикл обработки одной записи Kick.
    """

    temp_dir = tempfile.mkdtemp(prefix="stream_finder_")

    try:
        log.info("Started job %s", job_id)

        video_id = extract_video_id(url)

        playlist_url = get_kick_video_url(video_id)

        audio_file = os.path.join(temp_dir, "full_audio.mp3")

        # 1. Скачиваем всю аудиозапись.
        download_audio(playlist_url, audio_file)

        # 2. Делим её на части.
        chunks_dir = os.path.join(temp_dir, "chunks")
        os.makedirs(chunks_dir, exist_ok=True)

        chunks = split_audio(audio_file, chunks_dir)

        # 3. Расшифровываем каждую часть.
        all_segments = []

        for index, chunk in enumerate(chunks):
            offset = index * CHUNK_SECONDS

            log.info(
                "Processing chunk %s/%s",
                index + 1,
                len(chunks)
            )

            segments = transcribe_chunk(
                chunk,
                offset
            )

            all_segments.extend(segments)

        all_segments.sort(
            key=lambda item: item["start"]
        )

        transcript = "\n".join(
            f"[{item['time']}] {item['text']}"
            for item in all_segments
        )

        # 4. Отправляем результат в Cloudflare.
        callback_data = {
            "job_id": job_id,
            "chat_id": str(chat_id),
            "status": "completed",
            "segments": all_segments,
            "transcript": transcript
        }

        send_callback(callback_data)

        log.info(
            "Job %s completed. Total segments: %s",
            job_id,
            len(all_segments)
        )

    except Exception as error:
        log.exception("Job %s failed", job_id)

        try:
            send_callback({
                "job_id": job_id,
                "chat_id": str(chat_id),
                "status": "error",
                "error": str(error)
            })

        except Exception:
            log.exception("Could not send error callback")

    finally:
        shutil.rmtree(
            temp_dir,
            ignore_errors=True
        )


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Stream Finder Processor is online",
        "groq_configured": bool(GROQ_API_KEY)
    })


@app.route("/process", methods=["POST"])
def process():
    data = request.get_json(silent=True) or {}

    url = data.get("url")
    chat_id = data.get("chat_id")
    job_id = data.get("job_id")

    if not url or not chat_id or not job_id:
        return jsonify({
            "error": "Missing url, chat_id or job_id"
        }), 400

    if "kick.com/" not in url.lower():
        return jsonify({
            "error": "Only Kick VOD links are supported"
        }), 400

    thread = threading.Thread(
        target=process_job,
        args=(url, chat_id, job_id),
        daemon=True
    )

    thread.start()

    return jsonify({
        "status": "processing",
        "job_id": job_id,
        "message": "Video processing started"
    }), 202


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
