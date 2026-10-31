
import os
import re
import glob
import time
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

CALLBACK_URL = (
    "https://stream-finder-bot.stfualmceo.workers.dev/processor-result"
)

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
        "video_player": {
            "player": {}
        },
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

    playlist_url = (
        playback.get("vod")
        or playback.get("dvr")
    )

    if not playlist_url:
        raise Exception(
            "Kick не вернул ссылку на VOD. "
            f"Ответ: {str(data)[:1000]}"
        )

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
        raise Exception(
            "FFmpeg error: " + result.stderr[-3000:]
        )

    return result


def download_audio(playlist_url, output_file):
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
        "-i", playlist_url,
        "-map", "0:a:0",
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-c:a", "libmp3lame",
        "-b:a", "48k",
        output_file
    ]

    log.info("Downloading full audio")
    run_ffmpeg(command)

    if not os.path.exists(output_file):
        raise Exception("Аудиофайл не создан.")

    if os.path.getsize(output_file) == 0:
        raise Exception("Аудиофайл пустой.")

    log.info("Audio download completed")


def split_audio(audio_file, output_dir):
    pattern = os.path.join(
        output_dir,
        "chunk_%04d.mp3"
    )

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
        glob.glob(
            os.path.join(output_dir, "chunk_*.mp3")
        )
    )

    if not chunks:
        raise Exception("Не удалось разделить аудио.")

    return chunks


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

    raise Exception(
        f"Ошибка расшифровки: {last_error}"
    )


def send_callback(payload):
    response = requests.post(
        CALLBACK_URL,
        json=payload,
        headers={
            "Content-Type": "application/json"
        },
        timeout=120
    )

    response.raise_for_status()
    log.info("Callback delivered")


def process_job(url, chat_id, job_id):
    temp_dir = tempfile.mkdtemp(
        prefix="stream_finder_"
    )

    try:
        log.info("Started job %s", job_id)

        video_id = extract_video_id(url)

        # Получаем ссылку на запись Kick.
        playlist_url = get_kick_video_url(video_id)

        # Скачиваем аудио.
        audio_file = os.path.join(
            temp_dir,
            "full_audio.mp3"
        )

        download_audio(
            playlist_url,
            audio_file
        )

        # Делим аудио на части.
        chunks_dir = os.path.join(
            temp_dir,
            "chunks"
        )

        os.makedirs(
            chunks_dir,
            exist_ok=True
        )

        chunks = split_audio(
            audio_file,
            chunks_dir
        )

        log.info("Total chunks: %s", len(chunks))

        all_segments = []

        # Расшифровываем все части.
        for index, chunk in enumerate(chunks):
            offset = index * CHUNK_SECONDS

            log.info(
                "Transcribing chunk %s/%s",
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

        # Отправляем результат в Cloudflare.
        send_callback({
            "job_id": job_id,
            "chat_id": str(chat_id),
            "status": "completed",
            "segments": all_segments,
            "transcript": transcript
        })

        log.info(
            "Job completed: %s segments",
            len(all_segments)
        )

    except Exception as error:
        log.exception("Processing failed")

        try:
            send_callback({
                "job_id": job_id,
                "chat_id": str(chat_id),
                "status": "error",
                "error": str(error)
            })

        except Exception:
            log.exception("Error callback failed")

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
        "job_id": job_id
    }), 202


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
