
import os
import glob
import time
import shutil
import tempfile
import threading
import subprocess
import requests

from flask import Flask, request, jsonify

app = Flask(__name__)

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
CALLBACK_URL = "https://stream-finder-bot.stfualmceo.workers.dev/processor-result"

CHUNK_SECONDS = 600
GROQ_URL = "https://api.groq.com/openai/v1/audio/transcriptions"


def send_callback(job_id, chat_id, payload):
    data = {
        "job_id": job_id,
        "chat_id": chat_id,
        **payload
    }

    response = requests.post(
        CALLBACK_URL,
        json=data,
        timeout=120
    )
    response.raise_for_status()


def get_kick_vod_url(video_url):
    vod_id = video_url.rstrip("/").split("/")[-1]

    api_url = f"https://kick.com/api/v1/stream/{vod_id}/playback"

    response = requests.get(
        api_url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        },
        timeout=30
    )

    response.raise_for_status()
    data = response.json()

    playback = data.get("playback_url", {})

    if isinstance(playback, dict):
        result = playback.get("vod") or playback.get("dvr")
        if result:
            return result

    if isinstance(playback, str) and playback:
        return playback

    raise Exception("Kick не вернул ссылку на видео")


def format_time(seconds):
    seconds = int(seconds)

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    return f"{hours:02}:{minutes:02}:{secs:02}"


def transcribe_chunk(path, offset):
    if not GROQ_API_KEY:
        raise Exception("GROQ_API_KEY отсутствует")

    for attempt in range(3):
        try:
            with open(path, "rb") as audio:
                response = requests.post(
                    GROQ_URL,
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}"
                    },
                    files={
                        "file": ("audio.mp3", audio, "audio/mpeg")
                    },
                    data={
                        "model": "whisper-large-v3-turbo",
                        "response_format": "verbose_json",
                        "timestamp_granularities[]": "segment"
                    },
                    timeout=300
                )

            response.raise_for_status()
            result = response.json()

            segments = []

            for item in result.get("segments", []):
                start = float(item.get("start", 0)) + offset
                text = item.get("text", "").strip()

                if not text:
                    continue

                segments.append({
                    "start": start,
                    "time": format_time(start),
                    "text": text
                })

            return segments

        except Exception:
            if attempt == 2:
                raise

            time.sleep(5 * (attempt + 1))


def process_job(job_id, chat_id, video_url):
    workdir = tempfile.mkdtemp(prefix="stream_")

    try:
        print(f"Starting job: {job_id}")

        video = get_kick_vod_url(video_url)

        audio_file = os.path.join(workdir, "full_audio.mp3")

        # Извлекаем аудио из всего видео
        command = [
            "ffmpeg",
            "-y",
            "-i", video,
            "-vn",
            "-ac", "1",
            "-ar", "16000",
            "-b:a", "48k",
            audio_file
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=3600
        )

        if result.returncode != 0:
            raise Exception(
                "FFmpeg extraction error: " + result.stderr[-1500:]
            )

        # Разбиваем аудио на части по 10 минут
        pattern = os.path.join(workdir, "part_%04d.mp3")

        command = [
            "ffmpeg",
            "-y",
            "-i", audio_file,
            "-f", "segment",
            "-segment_time", str(CHUNK_SECONDS),
            "-reset_timestamps", "1",
            "-c", "copy",
            pattern
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=3600
        )

        if result.returncode != 0:
            raise Exception(
                "FFmpeg splitting error: " + result.stderr[-1500:]
            )

        chunks = sorted(
            glob.glob(os.path.join(workdir, "part_*.mp3"))
        )

        if not chunks:
            raise Exception("Не удалось создать аудиофрагменты")

        all_segments = []

        for index, chunk in enumerate(chunks):
            offset = index * CHUNK_SECONDS

            print(
                f"Processing chunk {index + 1}/{len(chunks)}"
            )

            segments = transcribe_chunk(chunk, offset)
            all_segments.extend(segments)

            print(
                f"Chunk {index + 1} completed. "
                f"Segments: {len(segments)}"
            )

        if not all_segments:
            raise Exception("Groq не вернул текст")

        all_segments.sort(
            key=lambda item: item["start"]
        )

        transcript = "\n".join(
            f"[{item['time']}] {item['text']}"
            for item in all_segments
        )

        send_callback(
            job_id,
            chat_id,
            {
                "status": "completed",
                "segments": all_segments,
                "transcript": transcript
            }
        )

        print(
            f"Job {job_id} completed. "
            f"Total segments: {len(all_segments)}"
        )

    except Exception as error:
        print(f"JOB ERROR: {error}")

        try:
            send_callback(
                job_id,
                chat_id,
                {
                    "status": "error",
                    "error": str(error)[:2000]
                }
            )
        except Exception as callback_error:
            print(f"CALLBACK ERROR: {callback_error}")

    finally:
        shutil.rmtree(workdir, ignore_errors=True)


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Stream Finder Processor is online",
        "groq_configured": bool(GROQ_API_KEY)
    })


@app.route("/process", methods=["POST"])
def process():
    data = request.get_json(silent=True) or {}

    video_url = data.get("url")
    chat_id = data.get("chat_id")
    job_id = data.get("job_id")

    if not video_url or not chat_id or not job_id:
        return jsonify({
            "error": "Missing url, chat_id or job_id"
        }), 400

    thread = threading.Thread(
        target=process_job,
        args=(str(job_id), str(chat_id), str(video_url)),
        daemon=True
    )

    thread.start()

    return jsonify({
        "status": "queued",
        "job_id": job_id
    }), 202


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
