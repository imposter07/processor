from flask import Blueprint

bp = Blueprint('tutorials', __name__, static_folder='../static')

from app.tutorials import routes
