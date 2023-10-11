import os
import pytest
import ctypes
import threading
from sqlalchemy import create_engine, Sequence, text, inspect
from multiprocessing import Process
from flask_login import FlaskLoginClient
from app import create_app, db
from config import Config, basedir
import processor.reporting.utils as utl
from processor.reporting.export import ScriptBuilder, DB
from app.models import User
from rq import SimpleWorker
from rq.timeouts import BaseDeathPenalty, JobTimeoutException


class TestConfig(Config):
    SQLALCHEMY_DATABASE_URI = 'sqlite:///{}'.format(
        os.path.join(basedir, 'app-test.db'))
    TESTING = True
    EXP_DB = 'test_dbconfig.json'


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


class TimerDeathPenalty(BaseDeathPenalty):
    def __init__(self, timeout, exception=JobTimeoutException, **kwargs):
        super().__init__(timeout, exception, **kwargs)
        self._target_thread_id = threading.current_thread().ident
        self._timer = None

        # Monkey-patch exception with the message ahead of time
        # since PyThreadState_SetAsyncExc can only take a class
        def init_with_message(self, *args, **kwargs):  # noqa
            msg = "Task exceeded maximum timeout value ({0} seconds)".format(
                timeout)
            super(exception, self).__init__(msg)

        self._exception.__init__ = init_with_message

    def new_timer(self):
        """Returns a new timer since timers can only be used once."""
        return threading.Timer(self._timeout, self.handle_death_penalty)

    def handle_death_penalty(self):
        """Raises an asynchronous exception in another thread.

        Reference http://docs.python.org/c-api/init.html#PyThreadState_SetAsyncExc for more info.
        """
        ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(
                self._target_thread_id), ctypes.py_object(self._exception)
        )
        if ret == 0:
            raise ValueError(
                "Invalid thread ID {}".format(self._target_thread_id))
        elif ret > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(self._target_thread_id), 0)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def setup_death_penalty(self):
        """Starts the timer."""
        if self._timeout <= 0:
            return
        self._timer = self.new_timer()
        self._timer.start()

    def cancel_death_penalty(self):
        """Cancels the timer."""
        if self._timeout <= 0:
            return
        self._timer.cancel()
        self._timer = None


class WindowsSimpleWorker(SimpleWorker):
    death_penalty_class = TimerDeathPenalty


@pytest.fixture(scope="class")
def worker(app_fixture):
    queue = app_fixture.task_queue
    queue.empty()
    worker = WindowsSimpleWorker([queue], connection=queue.connection)
    yield worker
    queue.empty()


@pytest.fixture(scope='module')
def drop_views():
    def _drop_views(engine, schema):
        inspector = inspect(engine)
        view_names = inspector.get_view_names(schema=schema)
        with engine.connect() as connection:
            for view_name in view_names:
                drop_statement = "DROP VIEW {}.{};".format(schema, view_name)
                connection.execute(text(drop_statement))
            connection.commit()
    return _drop_views


@pytest.fixture(scope='module')
def reporting_db(drop_views):
    processor_path = os.path.join(basedir, 'processor')
    os.chdir(processor_path)
    test_db = DB(TestConfig.EXP_DB)
    if test_db.db in ['lqadb', 'app']:
        pytest.fail('Database configuration forbidden.')
    if 'test' not in test_db.db:
        pytest.fail('Database name must contain "test".')
    os.chdir(basedir)
    engine = create_engine(test_db.conn_string)
    sb = ScriptBuilder()
    Sequence('models_modelid_seq', metadata=sb.metadata,
             schema=test_db.schema)
    for table in sb.tables:
        for col in table.columns:
            if col.primary_key:
                seq_name = '_'.join([table.name, col.name, 'seq'])
                Sequence(seq_name, metadata=sb.metadata, schema=test_db.schema)
    sb.metadata.create_all(bind=engine)
    yield engine
    drop_views(engine, test_db.schema)
    sb.metadata.drop_all(bind=engine)
