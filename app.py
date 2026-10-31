from flask import Flask, request, jsonify

app = Flask(__name__)

@app.get("/")
def home():
    return jsonify({
        "status": "Stream Finder Processor is online"
    })

@app.post("/process")
def process():
    data = request.get_json(silent=True) or {}

    return jsonify({
        "status": "queued",
        "message": "Processor received the job",
        "data": data
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
