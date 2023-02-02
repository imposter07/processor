import os
import time
import pytest
import processor.reporting.utils as utl
from datetime import datetime, timedelta
from app import create_app, db
from app.models import User, Post
from config import Config
from multiprocessing import Process


class TestConfig(Config):
    SQLALCHEMY_DATABASE_URI = 'sqlite://'


def run_server(with_run=True):
    app = create_app(TestConfig)
    app_context = app.app_context()
    app_context.push()
    db.create_all()
    if with_run:
        u = User(username='test', email='test@test.com')  # type: ignore
        u.set_password('test')
        db.session.add(u)
        db.session.commit()
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
def sw():
    sw = utl.SeleniumWrapper()
    p = Process(target=run_server)
    p.start()
    yield sw
    sw.quit()
    p.terminate()
    p.join()


@pytest.mark.usefixtures("app_fixture")
class TestUserModelCase:

    @staticmethod
    def test_password_hashing():
        u = User(username='susan')  # type: ignore
        u.set_password('cat')
        assert not u.check_password('dog')
        assert u.check_password('cat')

    @staticmethod
    def test_avatar():
        url = ('https://www.gravatar.com/avatar/'
               'd4c74594d841139328695756648b6bd6?d=identicon&s=128')
        u = User(username='john', email='john@example.com')  # type: ignore
        assert u.avatar(128) == url

    @staticmethod
    def test_follow():
        u1 = User(username='john', email='john@example.com')  # type: ignore
        u2 = User(username='susan', email='susan@example.com')  # type: ignore
        db.session.add(u1)
        db.session.add(u2)
        db.session.commit()
        assert u1.followed.all() == []
        assert u1.followers.all() == []

        u1.follow(u2)
        db.session.commit()
        assert u1.is_following(u2)
        assert u1.followed.count() == 1
        assert u1.followed.first().username == 'susan'
        assert u2.followers.count() == 1
        assert u2.followers.first().username == 'john'

        u1.unfollow(u2)
        db.session.commit()
        assert not u1.is_following(u2)
        assert u1.followed.count() == 0
        assert u2.followers.count() == 0

    @staticmethod
    def test_follow_posts():
        # create four users
        u1 = User.query.get(1)
        u2 = User.query.get(2)
        u3 = User(username='mary', email='mary@example.com')  # type: ignore
        u4 = User(username='david', email='david@example.com')  # type: ignore
        db.session.add_all([u3, u4])

        # create four posts
        now = datetime.utcnow()
        p1 = Post(body="post from john", author=u1,
                  timestamp=now + timedelta(seconds=1))
        p2 = Post(body="post from susan", author=u2,
                  timestamp=now + timedelta(seconds=4))
        p3 = Post(body="post from mary", author=u3,
                  timestamp=now + timedelta(seconds=3))
        p4 = Post(body="post from david", author=u4,
                  timestamp=now + timedelta(seconds=2))
        db.session.add_all([p1, p2, p3, p4])
        db.session.commit()

        # setup the followers
        u1.follow(u2)  # john follows susan
        u1.follow(u4)  # john follows david
        u2.follow(u3)  # susan follows mary
        u3.follow(u4)  # mary follows david
        db.session.commit()

        # check the followed posts of each user
        f1 = u1.followed_posts().all()
        f2 = u2.followed_posts().all()
        f3 = u3.followed_posts().all()
        f4 = u4.followed_posts().all()
        assert f1 == [p2, p4, p1]
        assert f2 == [p2, p3]
        assert f3 == [p3, p4]
        assert f4 == [p4]


@pytest.mark.usefixtures("sw")
class TestUserLogin:
    base_url = 'http://127.0.0.1:5000/'

    def test_login(self, sw):
        time.sleep(5)
        sw.go_to_url(self.base_url, 1)
        login_url = '{}auth/login?next=%2F'.format(self.base_url)
        assert sw.browser.current_url == login_url
        user_pass = [('test', 'username'), ('test', 'password')]
        sw.send_keys_from_list(user_pass)
        sw.xpath_from_id_and_click('submit', 1)
        assert sw.browser.current_url == self.base_url

    def test_create_processor(self, sw):
        create_url = '{}create_processor'.format(self.base_url)
        sw.go_to_url(create_url)
        assert sw.browser.current_url == create_url
        form_names = ['cur_client', 'cur_product', 'cur_campaign', 'description']
        elem_form = [('test', x) for x in form_names]
        elem_form += [('Base Processor', 'name')]
        sw.send_keys_from_list(elem_form)
        sw.xpath_from_id_and_click('loadContinue')
        sw.go_to_url('{}explore'.format(self.base_url))
        time.sleep(3)

    def test_processor_page(self, sw):
        proc_link = '//*[@id="navLinkProcessor"]'
        sw.click_on_xpath(proc_link, 1)
        assert sw.browser.current_url == '{}processor'.format(self.base_url)
