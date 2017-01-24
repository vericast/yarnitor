from flask import Flask
from flask.ext.redis import FlaskRedis
from flask.ext.cache import Cache
from .ui import ui_bp
from .api import api_bp
import os

app = Flask(__name__)

# Whether we're testing flask or not
app.config['FLASK_TESTING'] = os.getenv('FLASK_TESTING')
# Which redis to talk to
app.config["REDIS_URL"] = "redis://" + os.getenv("REDIS_ENDPOINT", "localhost:6379")

# Register blueprints for APIs
app.register_blueprint(api_bp)
app.register_blueprint(ui_bp)

redis_store = FlaskRedis(app)
cache = Cache(app, config={"CACHE_TYPE": "simple"})
