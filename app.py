from flask import Flask, send_from_directory, jsonify
import os

app = Flask(__name__)
DATA_DIR = os.environ.get("DATA_DIR", "/data")

@app.get("/")
def root():
    # 保存ファイルの一覧表示
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".csv")])
    return jsonify({
        "message": "CSV files ready",
        "files": files,
        "hint": "GET /files/<name> to download. Example: /files/races_2024_all_local_ex_obihiro.csv"
    })

@app.get("/files/<path:name>")
def files(name):
    return send_from_directory(DATA_DIR, name, as_attachment=True)