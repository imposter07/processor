import os
import pytest
from multiprocessing import Process
from flask_login import FlaskLoginClient
from app import create_app, db
from config import Config, basedir
import processor.reporting.utils as utl
from app.models import User


class TestConfig(Config):
    SQLALCHEMY_DATABASE_URI = ('sqlite:///'
                               + os.path.join(basedir, 'app-test.db'))
    TESTING = True


def run_server(with_run=True):
    app = create_app(TestConfig)
    app_context = app.app_context()
    app_context.push()
    db.create_all()
    if with_run:
        app.run(debug=True, use_reloader=False)
    return app, app_context


@pytest.fixture(scope='module')
def app_fixture():
    app, app_context = run_server(False)
    yield app
    db.session.remove()
    db.drop_all()
    app_context.pop()


@pytest.fixture(scope='module')
def client(app_fixture, user):
    app_fixture.test_client_class = FlaskLoginClient
    return app_fixture.test_client(user=user)


@pytest.fixture(scope='module')
def sw():
    sw = utl.SeleniumWrapper()
    p = Process(target=run_server)
    p.start()
    yield sw
    sw.quit()
    p.terminate()
    p.join()


@pytest.fixture(scope='module')
def user(app_fixture):
    u = User(username='test', email='test@test.com')  # type: ignore
    u.set_password('test')
    db.session.add(u)
    db.session.commit()
    yield u
    db.session.delete(u)
    db.session.commit()
