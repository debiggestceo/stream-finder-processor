
from flask import Flask, request, jsonify
import os
import uuid
import threading
import subprocess
import tempfile
import requests

app = Flask(__name__)

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
CALLBACK_URL = "https://stream-finder-bot.stfualmceo.workers.dev/processor-result"

jobs = {}


def send_callback(job_id, chat_id, payload):
    data = {
        "job_id": job_id,
        "chat_id": str(chat_id),
        **payload
    }
    response = requests.post(CALLBACK_URL, json=data, timeout=60)
    response.raise_for_status()


def get_kick_vod_url(stream_url):
    if "/videos/" not in stream_url:
        raise Exception("Нужна ссылка на Kick VOD")

    vod_id = stream_url.split("/videos/")[1].split("?")[0].strip("/")

    response = requests.post(
        f"https://web.kick.com/api/v1/stream/{vod_id}/playback",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        json={
            "video_player": {"player": {}},
            "video_session": {},
            "user_session": {"non_personalised_ads": True}
        },
        timeout=30
    )

    if not response.ok:
        raise Exception(f"Kick API error: {response.status_code}")

    data = response.json()
    playback = data.get("playback_url", {})
    vod_url = playback.get("vod") or playback.get("dvr")

    if not vod_url:
        raise Exception("Kick не выдал ссылку на запись VOD")

    return vod_url


def process_job(job_id, chat_id, stream_url):
    try:
        if not GROQ_API_KEY:
            raise Exception("На Render не задан GROQ_API_KEY")

        jobs[job_id] = {"status": "processing"}

        vod_url = get_kick_vod_url(stream_url)

        with tempfile.TemporaryDirectory() as temp_dir:
            audio_path = os.path.join(temp_dir, "audio.mp3")

            command = [
                "ffmpeg", "-y",
                "-i", vod_url,
                "-t", "600",
                "-vn",
                "-ac", "1",
                "-ar", "16000",
                "-b:a", "64k",
                audio_path
            ]

            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=900
            )

            if result.returncode != 0:
                raise Exception("FFmpeg не смог скачать аудио. Возможно, Kick не отдал запись.")

            with open(audio_path, "rb") as audio_file:
                groq_response = requests.post(
                    "https://api.groq.com/openai/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                    files={
                        "file": ("audio.mp3", audio_file, "audio/mpeg")
                    },
                    data={
                        "model": "whisper-large-v3-turbo",
                        "language": "ru",
                        "response_format": "verbose_json",
                        "timestamp_granularities[]": "segment"
                    },
                    timeout=300
                )

            if not groq_response.ok:
                raise Exception(f"Groq error: {groq_response.text[:500]}")

            result_data = groq_response.json()

            segments = []
            for segment in result_data.get("segments", []):
                start = int(segment.get("start", 0))
                segments.append({
                    "time": f"{start // 60}:{start % 60:02d}",
                    "start": start,
                    "text": segment.get("text", "").strip()
                })

            jobs[job_id] = {
                "status": "completed",
                "segments": segments
            }

            send_callback(job_id, chat_id, {
                "status": "completed",
                "segments": segments,
                "transcript": result_data.get("text", "")
            })

    except Exception as error:
        message = str(error)
        jobs[job_id] = {
            "status": "error",
            "message": message
        }

        try:
            send_callback(job_id, chat_id, {
                "status": "error",
                "error": message
            })
        except Exception as callback_error:
            print("Callback failed:", str(callback_error))


@app.get("/")
def home():
    return jsonify({"status": "Stream Finder Processor is online"})


@app.post("/process")
def start_process():
    data = request.get_json(silent=True) or {}

    stream_url = data.get("url", "").strip()
    chat_id = data.get("chat_id")
    job_id = data.get("job_id")

    if not stream_url or not chat_id or not job_id:
        return jsonify({
            "status": "error",
            "message": "Не хватает url, chat_id или job_id"
        }), 400

    if "kick.com/" not in stream_url or "/videos/" not in stream_url:
        return jsonify({
            "status": "error",
            "message": "Пока поддерживаются только Kick VOD"
        }), 400

    thread = threading.Thread(
        target=process_job,
        args=(job_id, chat_id, stream_url),
        daemon=True
    )
    thread.start()

    return jsonify({
        "status": "queued",
        "job_id": job_id
    }), 202


@app.get("/status/<job_id>")
def get_status(job_id):
    job = jobs.get(job_id)

    if not job:
        return jsonify({"status": "not_found"}), 404

    return jsonify(job)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
