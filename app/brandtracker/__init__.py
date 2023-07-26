from flask import Blueprint

bp = Blueprint('brandtracker', __name__, static_folder='../static')

from app.brandtracker import routes
