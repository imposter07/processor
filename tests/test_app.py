import os
import time
import pytest
import urllib
from datetime import datetime, timedelta
from app import db
from app.models import User, Post, Processor, Client, Product, Campaign, Task, \
    Project, ProjectNumberMax, Plan
import app.plan.routes as plan_routes
import app.plan.forms as plan_forms
from config import basedir
import pandas as pd
import processor.reporting.vmcolumns as vmc
import processor.reporting.calc as calc
import processor.reporting.expcolumns as exc
from processor.reporting.export import DB

base_url = 'http://127.0.0.1:5000/'


@pytest.fixture(scope='module')
def login(sw, user):
    sw.go_to_url(base_url)
    login_url = '{}auth/login?next=%2F'.format(base_url)
    user_pass = [('test', 'username'), ('test', 'password')]
    sw.send_keys_from_list(user_pass)
    sw.xpath_from_id_and_click('submit', 1)


@pytest.fixture(scope='class')
def create_processor(app_fixture, user, tmp_path_factory, worker):
    cur_path = os.getcwd()
    created_processors = []
    tmp_dir = tmp_path_factory.mktemp('test_processors')

    def _create_processor(name, client='test', product='test', campaign='test',
                          create_files=False):
        client = Client(name=client).check_and_add()
        product = Product(name=product,
                          client_id=client.id).check_and_add()
        campaign = Campaign(name=campaign,
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
            worker.work(burst=True)
            upload_id_path = os.path.join(new_processor.local_path, 'config',
                                          exc.upload_id_file)
            if os.path.isfile(upload_id_path):
                os.remove(upload_id_path)
        return new_processor

    yield _create_processor

    for proc in created_processors:
        db.session.delete(proc)
    db.session.commit()
    os.chdir(cur_path)


def submit_form(sw, form_names=None, select_form_names=None,
                submit_id='loadContinue', test_name='test'):
    if not form_names:
        form_names = []
    if not select_form_names:
        select_form_names = []
    select_str = '-selectized'
    elem_form = [(test_name, '{}{}'.format(x, select_str))
                 if 'cur' in x or 'Select' in x or x in select_form_names
                 else (test_name, x) for x in form_names + select_form_names]
    sw.send_keys_from_list(elem_form)
    sw.xpath_from_id_and_click(submit_id)


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


class TestPlan:
    test_name = 'test'

    def get_url(self, url_type=''):
        url = '{}{}'.format(base_url, plan_routes.plan.__name__)
        name_url = urllib.parse.quote(self.test_name)
        topline_route = plan_routes.topline.__name__
        sow_route = plan_routes.edit_sow.__name__
        url_dict = {}
        for url_route in [topline_route, sow_route]:
            url_str = url_route
            if url_route == sow_route:
                url_str = url_str.split('_')[1]
            url_dict[url_route] = url_str
        if url_type in url_dict:
            url += '/{}/{}'.format(name_url, url_dict[url_type])
        return url

    def test_create_plan(self, sw, login, worker):
        create_url = self.get_url()
        sw.go_to_url(create_url)
        form_names = ['cur_client', 'cur_product', 'cur_campaign',
                      'description', 'name']
        submit_form(sw, form_names, test_name=self.test_name)
        p = Plan.query.filter_by(name=self.test_name).first()
        assert p.name == self.test_name
        edit_url = self.get_url(plan_routes.topline.__name__)
        assert sw.browser.current_url == edit_url

    def test_topline(self, sw, login, worker):
        edit_url = self.get_url(plan_routes.topline.__name__)
        if not sw.browser.current_url == edit_url:
            self.test_create_plan(sw, login, worker)
        assert sw.browser.current_url == edit_url
        sel_id = 'partnerSelectAdd'
        submit_id = 'addRowsTopline'
        worker.work(burst=True)
        TestProject.wait_for_jobs_finish()
        sw.wait_for_elem_load(submit_id)
        part_name = 'Facebook'
        part_budget = '5000'
        submit_form(sw, form_names=[sel_id], submit_id=submit_id,
                    test_name=part_name)
        sw.xpath_from_id_and_click('tr0', sleep=0)
        submit_form(sw, ['total_budget0'], test_name=part_budget)
        sow_url = self.get_url(plan_routes.edit_sow.__name__)
        worker.work(burst=True)
        TestProject.wait_for_jobs_finish()
        p = Plan.query.filter_by(name=self.test_name).first()
        phase = p.get_current_children()
        assert len(phase) == 1
        part = phase[0].get_current_children()
        assert len(part) == 1
        part = part[0]
        assert part.name == part_name
        assert int(part.total_budget) == int(part_budget)
        assert sw.browser.current_url == sow_url


class TestProject:

    @staticmethod
    def wait_for_jobs_finish():
        for x in range(20):
            t = Task.query.filter_by(complete=False).first()
            if t:
                t.wait_and_get_job(loops=10)
            else:
                break
        return True

    def test_project_number_page(self, sw, login, worker):
        name = Product.get_default_name()[0]
        cli = Client(name=name).check_and_add()
        pro = Product(name=name, client_id=cli.id).check_and_add()
        cam = Campaign(name=name, product_id=pro.id).check_and_add()
        p = Project(project_number=name, campaign_id=cam.id,
                    flight_start_date=datetime.today(),
                    flight_end_date=datetime.today())
        db.session.add(p)
        db.session.commit()
        pn_url = '{}project_numbers'.format(base_url)
        sw.go_to_url(pn_url)
        assert sw.browser.current_url == pn_url
        pn_max = ProjectNumberMax(max_number=3600)
        db.session.add(pn_max)
        db.session.commit()
        task_name = Task.get_table_name_to_task_dict()['ProjectNumber']
        t = Task.query.filter_by(complete=False, name=task_name).first()
        assert t.name == task_name
        worker.work(burst=True)
        self.wait_for_jobs_finish()
        p = Project.query.all()
        assert len(p) > 10
        select_id = 'productProjectNumberFilterSelect'
        apply_id = 'ProjectNumberFilterButton'
        sw.wait_for_elem_load(apply_id)
        submit_form(sw, [select_id], submit_id=apply_id, test_name=name)
        t = Task.query.filter_by(complete=False, name=task_name).first()
        assert t.name == task_name
        worker.work(burst=True)
        self.wait_for_jobs_finish()
        time.sleep(3)
        sw.xpath_from_id_and_click('rowproject_number0')
        assert 'edit' in sw.browser.current_url


class TestReportingDBReadWrite:
    test_proc_name = "FFXIV Evergreen FY22 - March"
    agency = 'Liquid Advertising'
    client = "Square Enix"
    product = "Final Fantasy XIV"
    campaign = "March EVG FY22"

    @pytest.fixture(scope="class")
    def result_df(self):
        test_path = os.path.join(basedir, 'processor', exc.test_path)
        results_path = os.path.join(test_path, 'results.csv')
        rdf = pd.read_csv(results_path)
        return rdf

    @pytest.fixture(scope="class")
    def default_name(self):
        return self.test_proc_name

    @pytest.fixture(scope="class")
    def set_up(self, login, create_processor, default_name, worker):
        new_processor = create_processor(
            default_name, client=self.client, product=self.product,
            campaign=self.campaign, create_files=True)
        worker.work(burst=True)
        return new_processor

    @pytest.fixture(scope="class")
    def load_empty_model_table(self, reporting_db, app_fixture):
        data = {'modeltype': {0: 'None'},
                'modelname': {0: 'None'}, 'modelcoefa': {0: 0},
                'modelcoefb': {0: 0}, 'modelcoefc': {0: 0},
                'modelcoefd': {0: 0}, 'eventdate': {0: '2021-07-13'}}
        model_df = pd.DataFrame(data=data)
        test_db = DB(app_fixture.config['EXP_DB'])
        test_db.copy_from('model', model_df, model_df.columns)

    def test_postgres_setup(self, reporting_db):
        """Check main postgresql fixture."""
        with reporting_db.connect() as connection:
            product_query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'product';
            """
            product_columns = pd.read_sql(
                product_query, connection)['column_name'].tolist()
            expected_product_columns = ['productname', 'productdetail',
                                        'clientid']
            assert all(column in product_columns for column in
                       expected_product_columns)
            campaign_query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'campaign';
            """
            campaign_columns = pd.read_sql(
                campaign_query, connection)['column_name'].tolist()
            expected_campaign_columns = ['campaignname', 'campaigntype',
                                         'campaignphase']
            assert all(column in campaign_columns for column in
                       expected_campaign_columns)

    def test_export_py(self, set_up, user, worker, reporting_db,
                       load_empty_model_table, result_df):
        new_processor = set_up
        task = '.run_processor'
        msg = 'Running processor {}...'.format(self.test_proc_name)
        run_args = '--noprocess --exp test'
        rdf = result_df
        new_processor.launch_task(task, msg, user.id, run_args)
        worker.work(burst=True)
        sql = "SELECT * FROM lqadb.agency;"
        with reporting_db.connect() as connection:
            df = pd.read_sql(sql, connection, index_col='agencyid')
            assert (self.agency in df['agencyname'].to_list())
            sql = "SELECT * FROM lqadb.campaign;"
            df = pd.read_sql(sql, connection, index_col='campaignid')
            assert (all(item in df['campaignname'].to_list()
                        for item in ['Evergreen', '0']))
            sql = ("SELECT SUM(impressions) as total_impressions "
                   "FROM lqadb.event;")
            df = pd.read_sql(sql, connection)
            total_impressions_from_db = df.iloc[0]['total_impressions']
            assert (int(total_impressions_from_db) ==
                    int(rdf[vmc.impressions].sum()))
            sql = "SELECT SUM(clicks) as total_clicks FROM lqadb.event;"
            df = pd.read_sql(sql, connection)
            total_clicks_from_db = df.iloc[0]['total_clicks']
            total_clicks_from_csv = rdf[vmc.clicks].sum()
            assert total_clicks_from_db == total_clicks_from_csv

    def test_get_data_tables_from_db(self, set_up, user, worker, reporting_db):
        dimensions = ['productname']
        metrics = ['kpi']
        new_processor = set_up
        kwargs = {
            'dimensions': dimensions, 'metrics': metrics, 'filter_dict': None
        }
        task = new_processor.launch_task(
            '.get_data_tables_from_db',
            'Getting {} data by {} for processor {}'.format(
                metrics, dimensions, self.test_proc_name), user.id, **kwargs
        )
        worker.work(burst=True)
        job = task.wait_and_get_job(20)
        result_df = job.result[0]
        sql = ("SELECT SUM(impressions) AS {}, SUM(clicks) AS {}, "
               "SUM(netcost) AS \"{}\" FROM lqadb.event;".format(
            vmc.impressions, vmc.clicks, calc.NCF.lower()))
        with reporting_db.connect() as connection:
            db_df = pd.read_sql(sql, connection)
        assert (result_df[vmc.clicks].iloc[0] ==
                db_df[vmc.clicks.lower()].iloc[0])
        assert (result_df[vmc.impressions].iloc[0] ==
                db_df[vmc.impressions.lower()].iloc[0])

    def test_daily_chart(self, sw, worker):
        ffxiv_proc_url = '{}/processor/{}'.format(
            base_url, urllib.parse.quote(self.test_proc_name))
        sw.go_to_url(ffxiv_proc_url)
        worker.work(burst=True)
        time.sleep(3)
        assert sw.browser.find_element_by_id("getAllCharts")
        time.sleep(5)
        metric_select_path = "dailyMetricsChartPlaceholderSelect0"
        metric_select = sw.browser.find_element_by_id(metric_select_path)
        selectize = metric_select.find_element_by_xpath(
            "following-sibling::*[1]")
        values = selectize.find_elements_by_class_name('item')
        assert len(values) >= 1
