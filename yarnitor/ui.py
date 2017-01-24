"""YARN monitoring built-in UI."""

from flask import Blueprint, render_template

ui_bp = Blueprint('ui', __name__, static_folder='static')


@ui_bp.route('/')
def index():
    return render_template('index.html')
