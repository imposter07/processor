import os
import time
import pytest
import urllib
from datetime import datetime, timedelta
from app import db
from app.models import User, Post, Processor, Client, Product, Campaign
from config import basedir


base_url = 'http://127.0.0.1:5000/'


@pytest.fixture(scope='module')
def login(sw, user):
    sw.go_to_url(base_url)
    login_url = '{}auth/login?next=%2F'.format(base_url)
    user_pass = [('test', 'username'), ('test', 'password')]
    sw.send_keys_from_list(user_pass)
    sw.xpath_from_id_and_click('submit', 1)


@pytest.fixture(scope='module')
def create_processor(app_fixture, user, tmp_path_factory):
    cur_path = os.getcwd()
    created_processors = []
    tmp_dir = tmp_path_factory.mktemp('test_processors')

    def _create_processor(name, create_files=False):
        client = Client(name='test').check_and_add()
        product = Product(name='test',
                          client_id=client.id).check_and_add()
        campaign = Campaign(name='test',
                            product_id=product.id).check_and_add()
        local_path = os.path.join(tmp_dir, name, 'processor')
        new_processor = Processor(name=name, user_id=user.id,
                                  created_at=datetime.utcnow(),
                                  start_date=datetime.today(),
                                  campaign_id=campaign.id,
                                  local_path=local_path)
        db.session.add(new_processor)
        db.session.commit()
        created_processors.append(new_processor)
        if create_files:
            new_processor.launch_task(
                '.create_processor',
                'Creating processor {}...'.format(name), user.id,
                base_path=os.path.join(basedir, 'processor')
            )
        return new_processor
    yield _create_processor

    for proc in created_processors:
        db.session.delete(proc)
    db.session.commit()
    os.chdir(cur_path)


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
        u1 = db.session.get(User, 1)
        u2 = db.session.get(User, 2)
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


class TestUserLogin:

    def test_login(self, sw, login):
        sw.go_to_url(base_url)
        assert sw.browser.current_url == base_url


class TestProcessor:

    @pytest.fixture(scope="class")
    def default_name(self):
        return 'Base Processor'

    @pytest.fixture(scope="class")
    def set_up(self, login, create_processor, default_name, worker):
        create_processor(default_name, create_files=True)
        worker.work(burst=True)

    def test_create_processor(self, sw, login):
        test_name = 'test'
        create_url = '{}create_processor'.format(base_url)

        sw.go_to_url(create_url)
        form_names = ['cur_client-selectized',
                      'cur_product-selectized',
                      'cur_campaign-selectized', 'description']
        elem_form = [('test', x) for x in form_names]
        elem_form += [(test_name, 'name')]
        sw.send_keys_from_list(elem_form)
        sw.xpath_from_id_and_click('loadContinue')
        time.sleep(3)
        assert sw.browser.current_url == (
            '{}processor/{}/edit/import'.format(
                base_url, urllib.parse.quote(test_name))
        )

    def test_processor_page(self, sw, set_up):
        proc_link = '//*[@id="navLinkProcessor"]'
        sw.click_on_xpath(proc_link, 1)
        assert sw.browser.current_url == '{}processor'.format(base_url)
