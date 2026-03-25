from flask import Flask
from flask_cors import CORS
import redis

app = Flask(__name__)
CORS(app)
cache = redis.Redis(host="localhost", port=6379)

@app.route("/")
def index():
    return cache.get("greeting") or "hello"
