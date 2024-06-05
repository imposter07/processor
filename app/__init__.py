import os
import rq
import sys
import logging
from logging.handlers import SMTPHandler, RotatingFileHandler
from flask import Flask, request, current_app, has_request_context
from flask_sqlalchemy import SQLAlchemy as _BaseSQLAlchemy
from sqlalchemy import MetaData
from flask_migrate import Migrate
from flask_login import LoginManager
from flask_mail import Mail
from flask_moment import Moment
from flask_babel import Babel, lazy_gettext as _l
from config import Config
from elasticsearch import Elasticsearch, RequestsHttpConnection
from redis import Redis
from rq_scheduler import Scheduler
import rq_dashboard
from requests_aws4auth import AWS4Auth


def get_locale():
    return request.accept_languages.best_match(current_app.config['LANGUAGES'])


class SQLAlchemy(_BaseSQLAlchemy):
    def apply_pool_defaults(self, app, options):
        super(SQLAlchemy, self).apply_pool_defaults(app, options)
        options["pool_pre_ping"] = True


class RequestFormatter(logging.Formatter):
    def format(self, record):
        if has_request_context():
            record.url = request.url
            record.remote_addr = request.remote_addr
        else:
            record.url = None
            record.remote_addr = None
        return super().format(record)


naming_convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(column_0_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
db = SQLAlchemy(metadata=MetaData(naming_convention=naming_convention))
migrate = Migrate(compare_type=True)
login = LoginManager()
login.login_view = 'auth.login'
login.login_message = _l('Please log in to access this page.')
mail = Mail()
moment = Moment()
babel = Babel()


def create_app(config_class=Config()):
    app = Flask(__name__)
    app.config.from_object(config_class)

    db.init_app(app)
    migrate.init_app(app, db)
    login.init_app(app)
    mail.init_app(app)
    moment.init_app(app)
    babel.init_app(app, locale_selector=get_locale)
    app.elasticsearch = Elasticsearch(
        hosts=[{'host': app.config['ELASTICSEARCH_URL'].replace('https://', ''),
                'port': 443}],
        http_auth=AWS4Auth(app.config['AWS_ACCESS_KEY_ID'],
                           app.config['AWS_SECRET_ACCESS_KEY'],
                           app.config['AWS_REGION_NAME'], 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    ) \
        if app.config['ELASTICSEARCH_URL'] else None
    app.redis = Redis.from_url(app.config['REDIS_URL'])
    app.task_queue = rq.Queue('lqapp-tasks', connection=app.redis,
                              default_timeout=18000)
    app.scheduler = Scheduler(queue=app.task_queue, connection=app.redis)

    from app.errors import bp as errors_bp
    app.register_blueprint(errors_bp)

    from app.auth import bp as auth_bp
    app.register_blueprint(auth_bp, url_prefix='/auth')

    from app.main import bp as main_bp
    app.register_blueprint(main_bp)

    from app.tutorials import bp as tutorials_bp
    app.register_blueprint(tutorials_bp)

    from app.plan import bp as plan_bp
    app.register_blueprint(plan_bp)

    app.config.from_object(rq_dashboard.default_settings)
    app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")

    formatter = RequestFormatter(
        '[%(asctime)s] %(remote_addr)s requested %(url)s\n'
        '%(levelname)s in %(module)s: %(message)s'
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    if not app.debug and not app.testing:
        if app.config['MAIL_SERVER']:
            auth = None
            if app.config['MAIL_USERNAME'] or app.config['MAIL_PASSWORD']:
                auth = (app.config['MAIL_USERNAME'],
                        app.config['MAIL_PASSWORD'])
            secure = None
            if app.config['MAIL_USE_TLS']:
                secure = ()
            mail_handler = SMTPHandler(
                mailhost=(app.config['MAIL_SERVER'], app.config['MAIL_PORT']),
                fromaddr=app.config['ADMINS'][0],
                toaddrs=app.config['ADMINS'], subject='LQA Data Failure',
                credentials=auth, secure=secure)
            mail_handler.setLevel(logging.ERROR)
            mail_handler.setFormatter(formatter)
            app.logger.addHandler(mail_handler)

        if not os.path.exists('logs'):
            os.mkdir('logs')
        file_handler = RotatingFileHandler('logs/logging.log',
                                           maxBytes=10240, backupCount=10)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        app.logger.addHandler(file_handler)
        app.logger.addHandler(console)
        app.logger.setLevel(logging.INFO)
        app.logger.info('LQA App startup')
    elif app.testing:
        app.logger.addHandler(console)
        app.logger.setLevel(logging.DEBUG)
    return app


from app import models
