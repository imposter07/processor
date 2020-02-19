import os
import json
import urllib
import logging
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    REDIS_URL = os.environ.get('REDIS_URL') or 'redis://'
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ADMINS = ['james@liquidadvertising.com']
    POSTS_PER_PAGE = 10
    LANGUAGES = ['en', 'es']
    MAIL_SERVER = None
    MAIL_PORT = 25
    MAIL_USE_TLS = None
    MAIL_USERNAME = None
    MAIL_PASSWORD = None
    AWS_REGION_NAME = None
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None
    ELASTICSEARCH_URL = None
    USER_EMAIL_DOMAIN = None
    BASE_PROCESSOR_PATH = None
    DATABASE_URL = None
    DATABASE_USER = None
    DATABASE_PW = None
    DATABASE_PORT = None
    DATABASE_NAME = None

    def __init__(self, config_file='app/config.json'):
        config = self.load_config_file(config_file)
        for k in config:
            setattr(self, k, config[k])
        if self.DATABASE_URL:
            db_items = [self.DATABASE_USER, self.DATABASE_PW, self.DATABASE_URL,
                        self.DATABASE_PORT, self.DATABASE_NAME]
            self.SQLALCHEMY_DATABASE_URI = \
                ('postgresql://{0}:{1}@{2}:{3}/{4}'.format(
                    *[urllib.parse.quote_plus(x) for x in db_items]))

    @staticmethod
    def load_config_file(config_file):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
        except IOError:
            logging.warning('{} not found.'.format(config_file))
            config = {}
        return config
