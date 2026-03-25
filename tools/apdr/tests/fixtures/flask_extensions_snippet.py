from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_cors import CORS
import redis

app = Flask(__name__)
db = SQLAlchemy(app)
login_manager = LoginManager(app)
CORS(app)
cache = redis.Redis()
