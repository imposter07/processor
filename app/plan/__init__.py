from flask import Blueprint

bp = Blueprint('plan', __name__, static_folder='../static')

from app.plan import routes
