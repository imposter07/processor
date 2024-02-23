import os
import time
import json
import pytest
import urllib
from datetime import datetime, timedelta
from contextlib import contextmanager
from app import db
from app.models import User, Post, Processor, Client, Product, Campaign, Task, \
    Project, ProjectNumberMax, Plan, PlanEffectiveness, Tutorial, Uploader, \
    PartnerPlacements, PlanRule, ProcessorReports
import app.plan.routes as plan_routes
import app.main.routes as main_routes
from config import basedir
import pandas as pd
import processor.reporting.vmcolumns as vmc
import processor.reporting.vendormatrix as vm
import processor.reporting.utils as utl
import processor.reporting.calc as calc
import processor.reporting.expcolumns as exc
from processor.reporting.export import DB
import processor.reporting.gsapi as gsapi

base_url = 'http://127.0.0.1:5000/'


@pytest.fixture(scope='module')
def login(sw, user):
    submit_id = 'submit'
    sw.go_to_url(base_url, elem_id=submit_id)
    submit_form(sw, ['username', 'password'], submit_id=submit_id)


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
    sw.xpath_from_id_and_click(submit_id, .1)


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
        sw.go_to_url(base_url, elem_id='post')
        assert sw.browser.current_url == base_url


class TestProcessor:

    @pytest.fixture(scope="class")
    def default_name(self):
        return 'Base Processor'

    @pytest.fixture(scope="class")
    def set_up(self, login, create_processor, default_name, worker):
        new_proc = create_processor(default_name, create_files=True)
        worker.work(burst=True)
        return new_proc

    @staticmethod
    @contextmanager
    def adjust_path(path):
        cur_path = os.path.abspath(os.getcwd())
        os.chdir(path)
        yield path
        os.chdir(cur_path)

    def test_create_processor(self, sw, login, worker):
        test_name = 'test'
        create_url = '{}create_processor'.format(base_url)
        submit_id = 'loadContinue'
        sw.go_to_url(create_url, elem_id=submit_id)
        form = ['cur_client', 'cur_product', 'cur_campaign', 'description',
                'name']
        submit_form(sw, form_names=form, submit_id=submit_id)
        worker.work(burst=True)
        sw.wait_for_elem_load('refresh_imports')
        import_url = '{}processor/{}/edit/import'.format(
            base_url, urllib.parse.quote(test_name))
        assert sw.browser.current_url == import_url

    def test_processor_page(self, sw, set_up, worker):
        proc_link = '//*[@id="navLinkProcessor"]'
        sw.click_on_xpath(proc_link, 1)
        worker.work(burst=True)
        assert sw.browser.current_url == '{}processor'.format(base_url)

    def add_import_card(self, worker, sw, default_name, name):
        with self.adjust_path(basedir):
            proc_url = '{}/processor/{}/edit/import'.format(
                base_url, urllib.parse.quote(default_name))
            sw.go_to_url(proc_url, elem_id='add_child', sleep=.5)
            sw.xpath_from_id_and_click('add_child')
            sw.wait_for_elem_load('apis-0-key-selectized')
            import_form = [(name, 'apis-0-name'),
                           ('Rawfile', "apis-0-key-selectized", 'clear'),
                           ('11-16-2023', 'apis-0-start_date')]
            sw.send_keys_from_list(import_form)
            sw.browser.execute_script("window.scrollTo(0, 0)")
            sw.xpath_from_id_and_click('loadRefresh')
            sw.wait_for_elem_load('.alert.alert-info', selector=sw.select_css)
            worker.work(burst=True)

    def delete_import_card(self, worker, sw, default_name, name):
        with self.adjust_path(basedir):
            proc_url = '{}/processor/{}/edit/import'.format(
                base_url, urllib.parse.quote(default_name))
            sw.go_to_url(proc_url, elem_id='add_child', sleep=.5)
            import_card = sw.browser.find_element_by_xpath(
                '//div[@class="card col-" and .//input[@value="{}"]]'.format(
                    name))
            card_body = import_card.find_element_by_xpath('./*')
            api_id = card_body.get_attribute('id')
            sw.xpath_from_id_and_click('{}-delete'.format(api_id))
            sw.browser.execute_script("window.scrollTo(0, 0)")
            sw.xpath_from_id_and_click('loadRefresh')
            sw.wait_for_elem_load('.alert.alert-info', selector=sw.select_css)
            worker.work(burst=True)

    def test_add_delete_import(self, set_up, sw, worker, default_name):
        self.add_import_card(worker, sw, default_name, 'test')
        sw.browser.refresh()
        sw.wait_for_elem_load('base_form_id')
        with self.adjust_path(set_up.local_path):
            matrix = vm.VendorMatrix()
            assert 'API_Rawfile_test' in matrix.vm_df[vmc.vendorkey].to_list()
            self.delete_import_card(worker, sw, default_name, 'test')
            matrix = vm.VendorMatrix()
            assert 'API_Rawfile_test' not in matrix.vm_df[
                vmc.vendorkey].to_list()

    def test_raw_file_upload(self, set_up, sw, worker, default_name, user):
        test_name = 'test1'
        test_raw = 'rawfile_{}.csv'.format(test_name)
        self.add_import_card(worker, sw, default_name, test_name)
        sw.browser.refresh()
        sw.wait_for_elem_load('apis-0')
        place_col = 'Placement Name (From Ad Server)'
        placement = ('1074619450_Criteo_US_Best Buy_0_0_0_dCPM_5_45216_No '
                     'Tracking_0_0_CPV_Awareness_Launch_0_0_V_Cross Device_0'
                     '_eCommerce_eCommerce Ads_Best Buy')
        data = {place_col: [placement],
                'Date': ['11/16/2023'],
                'Impressions': ['1']}
        df = pd.DataFrame(data)
        file_name = 'test1.csv'
        raw1_path = os.path.join(basedir, 'tests', 'tmp')
        utl.dir_check(raw1_path)
        raw1_path = os.path.join(raw1_path, file_name)
        df.to_csv(raw1_path)
        with self.adjust_path(basedir):
            form_file = sw.browser.find_element_by_id('apis-0-raw_file')
            file_pond = form_file.find_element_by_class_name(
                'filepond--browser')
            file_path = raw1_path
            file_pond.send_keys(file_path)
            sw.wait_for_elem_load("loadingBtnrawFileTableBody")
            worker.work(burst=True)
            sw.wait_for_elem_load('00')
            sw.xpath_from_id_and_click('modalRawFileSaveButton')
            sw.wait_for_elem_load('.alert.alert-info', selector=sw.select_css)
            worker.work(burst=True)
            raw_path = os.path.join(set_up.local_path, utl.raw_path, test_raw)
            raw_df = pd.read_csv(raw_path)
            assert not raw_df.empty
            assert (df.iloc[0][place_col] == raw_df.iloc[0][place_col])


class TestPlan:
    test_name = 'test'

    def get_url(self, url_type='', plan_name=None):
        if not plan_name:
            plan_name = self.test_name
        url = '{}{}'.format(base_url, plan_routes.plan.__name__)
        name_url = urllib.parse.quote(plan_name)
        topline_route = plan_routes.topline.__name__
        sow_route = plan_routes.edit_sow.__name__
        calc_route = plan_routes.calc.__name__
        rfp_route = plan_routes.rfp.__name__
        rules_route = plan_routes.plan_rules.__name__
        place_route = plan_routes.plan_placements.__name__
        url_dict = {}
        url_routes = [topline_route, sow_route, calc_route, rfp_route,
                      rules_route, place_route]
        for url_route in url_routes:
            url_str = url_route
            if url_route == sow_route:
                url_str = url_str.split('_')[1]
            url_dict[url_route] = url_str
        if url_type in url_dict:
            url += '/{}/{}'.format(name_url, url_dict[url_type])
        return url

    def check_and_get_plan(self, sw, login, worker, url=''):
        cur_plan = Plan.query.filter_by(name=self.test_name).first()
        if not cur_plan:
            self.test_topline(sw, login, worker)
            cur_plan = Plan.query.filter_by(name=self.test_name).first()
        if url:
            elem_id = ''.join(x.capitalize() for x in url.split('_'))
            url = self.get_url(url)
            sw.go_to_url(url, elem_id='loadingBtn{}'.format(elem_id))
            worker.work(burst=True)
        return cur_plan

    def test_create_plan(self, sw, login, worker):
        create_url = self.get_url()
        form_names = ['cur_client', 'cur_product', 'cur_campaign',
                      'description', 'name']
        sw.go_to_url(create_url, elem_id=form_names[0])
        submit_form(sw, form_names, test_name=self.test_name)
        p = Plan.query.filter_by(name=self.test_name).first()
        assert p.name == self.test_name
        edit_url = self.get_url(plan_routes.topline.__name__)
        assert sw.browser.current_url == edit_url

    @staticmethod
    def set_topline_cost(worker, sw, part_budget='5000',
                         submit_id='loadContinue'):
        elem_id = 'tr0'
        sw.wait_for_elem_load(elem_id)
        wait_for_id = 'partnerSelect0-selectized'
        sw.xpath_from_id_and_click(elem_id, load_elem_id=wait_for_id)
        elem_id = 'total_budget0'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        elem.click()
        elem.clear()
        submit_form(sw, [elem_id], test_name=part_budget,
                    submit_id=submit_id)
        worker.work(burst=True)

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
        self.set_topline_cost(worker, sw, part_budget)
        sow_url = self.get_url(plan_routes.edit_sow.__name__)
        p = Plan.query.filter_by(name=self.test_name).first()
        phase = p.get_current_children()
        assert len(phase) == 1
        part = phase[0].get_current_children()
        assert len(part) == 1
        part = part[0]
        assert part.name == part_name
        assert int(part.total_budget) == int(part_budget)
        assert sw.browser.current_url == sow_url
        sw.go_to_url(edit_url, elem_id='loadingBtnTopline')
        worker.work(burst=True)
        phase_id = 'rowpartner_type-1'
        sw.wait_for_elem_load(phase_id)
        sw.xpath_from_id_and_click(phase_id, load_elem_id='total_budget-1')
        phase_id = 'phaseSelect-1'
        submit_form(sw, [phase_id])
        worker.work(burst=True)
        sw.wait_for_elem_load('project_name')
        p = Plan.query.filter_by(name=self.test_name).first()
        phase = p.get_current_children()
        assert phase[0].name == self.test_name

    def test_rules(self, sw, login, worker):
        url = plan_routes.plan_rules.__name__
        self.check_and_get_plan(sw, login, worker, url)
        elem_id = 'rowplace_col0'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        col_name = PartnerPlacements.environment.name
        assert elem.get_attribute('innerHTML').strip() == col_name
        sw.xpath_from_id_and_click(elem_id)
        elem_id = 'rule_info01SliderKey-selectized'
        second_name = '{}1'.format(self.test_name)
        add_row_id = 'rule_info0AddRow'
        submit_form(sw, [elem_id], submit_id=add_row_id)
        new_elem_id = elem_id.replace('01', '02')
        try:
            sw.browser.find_element_by_id(new_elem_id)
        except:
            sw.xpath_from_id_and_click('rule_info0AddRow')
        submit_form(sw, [new_elem_id], test_name=second_name,
                    submit_id='loadRefresh')
        worker.work(burst=True)
        elem_id = 'rule_info0Value'
        elem = sw.browser.find_element_by_id(elem_id)
        elem.clear()
        submit_form(sw, [elem_id], test_name='50')
        worker.work(burst=True)
        env_rule = PlanRule.query.filter_by(place_col=col_name).all()
        assert len(env_rule) == 1
        data = json.loads(env_rule[0].rule_info)
        assert data[self.test_name] == .5
        assert data[second_name] == .5

    def test_rules_lookup(self, sw, login, worker):
        url = plan_routes.plan_rules.__name__
        self.check_and_get_plan(sw, login, worker, url)

    def test_placements(self, sw, login, worker):
        url = plan_routes.plan_placements.__name__
        cur_plan = self.check_and_get_plan(sw, login, worker, url)
        load_elem_id = 'budget0'
        elem_id = 'row{}'.format(load_elem_id)
        sw.wait_for_elem_load(elem_id)
        sw.xpath_from_id_and_click(elem_id, load_elem_id=load_elem_id)
        submit_form(sw, [load_elem_id])
        worker.work(burst=True)
        cur_place = cur_plan.get_placements()
        cur_place = cur_place[0]
        assert cur_place.budget == self.test_name
        budget_col = PartnerPlacements.budget.name
        new_rule = PlanRule.query.filter_by(
            plan_id=cur_plan.id, type='update', place_col=budget_col).first()
        assert new_rule.place_col == budget_col
        assert new_rule.rule_info['val'] == self.test_name

    def test_add_plan(self, sw, login, worker, check_for_plan=False,
                      app_cols=False):
        url = plan_routes.plan_placements.__name__
        cur_plan = self.check_and_get_plan(sw, login, worker, url)
        df = TestUploader().upload_plan(
            sw, worker, check_for_plan=check_for_plan, app_cols=app_cols)
        cur_places = cur_plan.get_placements()
        worker.work(burst=True)
        assert len(cur_places) == len(df)
        last_row = 'tr{}'.format(len(df) - 1)
        sw.wait_for_elem_load(last_row)
        elem = sw.browser.find_element_by_id(last_row)
        assert elem
        return df, cur_plan

    def test_add_plan_local(self, sw, login, worker):
        self.test_add_plan(sw, login, worker, check_for_plan=True)

    def test_add_plan_app_cols(self, sw, login, worker):
        self.test_add_plan(sw, login, worker, app_cols=True)

    def test_topline_after_plan(self, sw, login, worker):
        url = plan_routes.topline.__name__
        df, cur_plan = self.test_add_plan(sw, login, worker)
        self.check_and_get_plan(sw, login, worker, url)
        budget = '1000'
        self.set_topline_cost(worker, sw, part_budget=budget,
                              submit_id='loadRefresh')
        cur_places = cur_plan.get_placements()
        assert len(cur_places) == len(df)
        cur_partners = cur_plan.get_partners()
        assert len(cur_partners) == 1
        assert int(cur_partners[0].total_budget) == int(budget)
        assert int(sum(x.total_budget for x in cur_places)) == int(budget)

    def test_calc(self, sw, login, worker):
        url = plan_routes.calc.__name__
        p = self.check_and_get_plan(sw, login, worker, url)
        elem_id = 'rowHeaders0'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        assert elem.get_attribute('innerHTML') == 'Brand Factors'
        elem_id = 'rowValue50'
        elem = sw.browser.find_element_by_id(elem_id)
        elem.click()
        selected_val = '0.20'
        assert elem.get_attribute("class") == 'shadeCell0'
        assert elem.get_attribute('innerHTML') == selected_val
        sw.xpath_from_id_and_click('loadRefresh', sleep=.1)
        worker.work(burst=True)
        t = Task.query.filter_by(name='.write_plan_calc').first()
        assert t
        elem_id = 'totalCardValuecell_pick_col'
        elem = sw.browser.find_element_by_id(elem_id)
        default_val = 3
        for x in range(10):
            if elem.get_attribute('innerHTML') != f'{default_val}':
                break
            time.sleep(.1)
        num = float(selected_val) + default_val
        assert elem.get_attribute('innerHTML') == f'{num:.2f}'
        pe = PlanEffectiveness.query.filter_by(
            plan_id=p.id, factor_name=PlanEffectiveness.brand_low[0]).first()
        assert pe.selected_val == float(selected_val)
        sw.browser.refresh()
        worker.work(burst=True)
        elem_id = 'rowValue50'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        assert elem.get_attribute("class") == 'shadeCell0'
        assert elem.get_attribute('innerHTML') == selected_val

    def test_rate_card_rfp(self, sw, login, worker):
        plan_name = 'Rate Card Database'
        p = Processor.query.filter_by(name=self.test_name).first()
        if not p:
            p = Processor(name=self.test_name)
            db.session.add(p)
            db.session.commit()
        p.launch_task('.get_rate_cards', '', 1)
        worker.work(burst=True)
        edit_url = self.get_url(plan_routes.rfp.__name__, plan_name)
        sw.go_to_url(edit_url, elem_id='form_continue')
        worker.work(burst=True)
        elem_id = 'rowrfp_file_id0'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        assert elem.get_attribute('innerHTML').strip() == '1'


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
        sw.go_to_url(pn_url, elem_id='loadingBtn')
        assert sw.browser.current_url == pn_url
        pn_max = ProjectNumberMax(max_number=3700)
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
        elem_id = 'rowproject_number0'
        a_xpath = '//*[@id="{}"]/a'.format(elem_id)
        sw.wait_for_elem_load(a_xpath, selector=sw.select_xpath, visible=True)
        sw.click_on_xpath(a_xpath)
        sw.wait_for_elem_load('project_number')
        assert 'edit' in sw.browser.current_url


class TestTutorial:
    test_name = TestPlan.test_name

    def test_get_tutorial(self, sw, login, worker):
        p = Processor.query.filter_by(name=self.test_name).first()
        if not p:
            p = Processor(name=self.test_name)
            db.session.add(p)
            db.session.commit()
        task_names = ['.get_all_tutorials_from_google_doc']
        for task_name in task_names:
            p.launch_task(task_name, '', 1)
        worker.work(burst=True)
        sheets = Tutorial.get_all_tutorial_sheets()
        for sheet in sheets:
            sheet_name = sheet[2]
            t = Tutorial.query.filter_by(name=sheet_name).first()
            assert t.name == sheet_name
        all_tuts = Tutorial.query.all()
        assert len(all_tuts) == len(sheets)

    def test_tutorial(self, sw, login, worker):
        t = Tutorial.query.all()
        if not t:
            self.test_get_tutorial(sw, login, worker)
        elem_id = 'tutorialCardBody'
        sw.go_to_url(base_url, elem_id=elem_id)
        link_xpath = '//*[@id="{}"]/div/div/h5/a'.format(elem_id)
        sw.click_on_xpath(link_xpath, .1)
        elem_id = 'loadContinue'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        assert elem


class TestUploader:
    test_name = 'test'

    def get_url(self, url_type='', up_name=None):
        if not up_name:
            up_name = self.test_name
        url = '{}{}'.format(base_url, main_routes.uploader.__name__)
        name_url = urllib.parse.quote(up_name)
        c_route = main_routes.edit_uploader_campaign.__name__
        e_route = main_routes.edit_uploader.__name__
        url_dict = {}
        for url_route in [c_route, e_route]:
            url_split = url_route.split('_')
            url_str = '{}'.format(url_split[0])
            if len(url_split) > 2:
                url_str = '{}/{}'.format(url_str, url_split[2])
            url_dict[url_route] = url_str
        if url_type in url_dict:
            url += '/{}/{}'.format(name_url, url_dict[url_type])
        return url

    def test_create_uploader(self, sw, login, worker):
        c_url = '{}/{}'.format(base_url, main_routes.create_uploader.__name__)
        form_names = ['cur_client', 'cur_product', 'cur_campaign',
                      'description', 'name']
        sw.go_to_url(c_url, elem_id=form_names[0])
        submit_form(sw, form_names, test_name=self.test_name)
        worker.work(burst=True)
        cur_up = Uploader.query.filter_by(name=self.test_name).first()
        assert cur_up.name == self.test_name
        cam_url = self.get_url(main_routes.edit_uploader_campaign.__name__)
        assert sw.browser.current_url == cam_url
        up_main_file = os.path.join(cur_up.local_path, 'main.py')
        assert os.path.isfile(up_main_file)
        return cur_up

    def check_create_uploader(self, sw, login, worker):
        cur_up = Uploader.query.filter_by(name=self.test_name).first()
        if not cur_up:
            cur_up = self.test_create_uploader(sw, login, worker)
        e_url = self.get_url(main_routes.edit_uploader.__name__)
        sw.go_to_url(e_url, elem_id='loadContinue')
        return cur_up

    def test_edit_uploader(self, sw, login, worker):
        cur_up = self.check_create_uploader(sw, login, worker)
        new_camp = '{}1'.format(cur_up.campaign.name)
        form_names = ['cur_campaign']
        submit_form(sw, form_names, test_name=new_camp)
        worker.work(burst=True)
        cur_up = Uploader.query.filter_by(name=self.test_name).first()
        assert cur_up.campaign.name == new_camp

    @staticmethod
    def upload_plan(sw, worker, check_for_plan=False, app_cols=False):
        df = Plan.get_mock_plan(basedir, check_for_plan, app_cols)
        file_name = os.path.join(basedir, 'mediaplantmp.xlsx')
        if app_cols:
            df.to_csv(file_name)
        else:
            df.to_excel(file_name, sheet_name='Media Plan')
        fp = sw.browser.find_element_by_class_name('filepond--browser')
        fp.send_keys(file_name)
        elem_id = 'alertPlaceholder'
        for x in range(100):
            elem = sw.browser.find_element_by_id(elem_id)
            if 'File was saved.' in elem.get_attribute('innerHTML'):
                break
            time.sleep(.1)
        worker.work(burst=True)
        TestProject.wait_for_jobs_finish()
        os.remove(file_name)
        return df

    def test_add_plan_uploader(self, sw, login, worker):
        cur_up = self.check_create_uploader(sw, login, worker)
        self.upload_plan(sw, worker)
        assert os.path.isfile(os.path.join(cur_up.local_path, 'mediaplan.xlsx'))

    def test_campaign_uploader(self, sw, login, worker):
        cur_up = self.check_create_uploader(sw, login, worker)
        plan_path = os.path.join(cur_up.local_path, 'mediaplan.xlsx')
        if not os.path.isfile(plan_path):
            self.test_add_plan_uploader(sw, login, worker)
        c_url = self.get_url(main_routes.edit_uploader_campaign.__name__)
        sw.go_to_url(c_url, elem_id='loadContinue')
        worker.work(burst=True)
        edit_camp_id = 'editTableCampaign'
        drop_id = 'editTableDropdownButton'
        sw.xpath_from_id_and_click(drop_id, load_elem_id=edit_camp_id)
        load_id = 'loadingBtn{}'.format(edit_camp_id)
        sw.xpath_from_id_and_click(edit_camp_id, load_elem_id=load_id)
        worker.work(burst=True)
        table_id = 'modalTableUploaderCampaign_wrapper'
        sw.wait_for_elem_load(table_id, visible=True)
        rows = sw.count_rows_in_table(table_id)
        assert rows == 2


class TestReportingDBReadWrite:
    test_proc_name = "FFXIV Evergreen FY22 - March"
    agency = 'Liquid Advertising'
    client = "Square Enix"
    product = "Final Fantasy XIV"
    campaign = "March EVG FY22"
    dash = "dashTest"

    @pytest.fixture(scope="class")
    def result_df(self):
        test_path = os.path.join(basedir, 'processor', exc.test_path)
        results_path = os.path.join(test_path, 'results.csv')
        rdf = pd.read_csv(results_path, low_memory=False)
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

    @pytest.fixture(scope="class")
    def export_test_data(self, set_up, user, worker, load_empty_model_table,
                         reporting_db):
        new_processor = set_up
        task = '.run_processor'
        msg = 'Running processor {}...'.format(self.test_proc_name)
        run_args = '--noprocess --exp test'
        new_processor.launch_task(task, msg, user.id, run_args)
        worker.work(burst=True)

    @pytest.fixture(scope="class")
    def update_report_in_db(self, set_up, user, worker, export_test_data):
        new_processor = set_up
        task = '.update_report_in_db'
        msg = 'Updating report for processor {}...'.format(self.test_proc_name)
        new_processor.launch_task(task, msg, user.id)
        worker.work(burst=True)


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

    def test_export_py(self, reporting_db, result_df, export_test_data):
        rdf = result_df
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

    def test_get_data_tables_from_db(self, set_up, user, worker, reporting_db,
                                     export_test_data):
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

    def test_daily_chart(self, set_up, user, login, sw, worker,
                         export_test_data):
        ffxiv_proc_url = '{}/processor/{}'.format(
            base_url, urllib.parse.quote(self.test_proc_name))
        sw.go_to_url(ffxiv_proc_url)
        worker.work(burst=True)
        sw.wait_for_elem_load('getAllCharts')
        assert sw.browser.find_element_by_id("getAllCharts")
        metric_select_path = "dailyMetricsChartPlaceholderSelect0"
        sw.wait_for_elem_load(metric_select_path, selector=sw.select_xpath)
        metric_select = sw.browser.find_element_by_id(metric_select_path)
        selectize = metric_select.find_element_by_xpath(
            "following-sibling::*[1]")
        values = selectize.find_elements_by_class_name('item')
        assert len(values) >= 1

    def test_create_chart(self, set_up, user, login, sw, worker,
                          export_test_data):
        ffxiv_proc_url = '{}/processor/{}'.format(
            base_url, urllib.parse.quote(self.test_proc_name))
        sw.go_to_url(ffxiv_proc_url)
        worker.work(burst=True)
        add_dash_id = 'add-dash'
        sw.wait_for_elem_load(add_dash_id)
        sw.xpath_from_id_and_click(add_dash_id,
                                   load_elem_id='chart_type-selectized')
        dash_form_fill = [('Test', 'name'),
                          ('Lollipop', 'chart_type-selectized', 'clear'),
                          ('environmentname', 'dimensions-selectized', 'clear'),
                          ('impressions', 'metrics-selectized'),
                          ('Chart', 'default_view-selectized', 'clear'),
                          ('Topline', 'tab-selectized', 'clear')]
        sw.send_keys_from_list(dash_form_fill)
        save_id = 'saveDashButton'
        sw.wait_for_elem_load(save_id)
        sw.xpath_from_id_and_click(save_id)
        sw.wait_for_elem_load('.alert.alert-info', selector=sw.select_css)
        worker.work(burst=True)
        sw.wait_for_elem_load("getAllCharts")
        sw.xpath_from_id_and_click('getAllCharts')
        worker.work(burst=True)
        show_chart_id = "showChartBtndash1Metrics"
        sw.wait_for_elem_load(show_chart_id)
        assert sw.browser.find_element_by_id("dash1Metrics")
        assert sw.browser.find_element_by_id(show_chart_id)
        metric_selectize_path = (
            "//*[@id=\"dash1MetricsChartPlaceholderSelect\"]/option")
        sw.wait_for_elem_load(metric_selectize_path, selector=sw.select_xpath)
        metric_selectize = sw.browser.find_element_by_xpath(
            metric_selectize_path)
        assert metric_selectize.get_attribute('value') == 'Impressions'

    def test_partner_charts(self, set_up, user, login, sw, worker,
                            export_test_data):
        ffxiv_proc_url = '{}/processor/{}'.format(
            base_url, urllib.parse.quote(self.test_proc_name))
        if not sw.browser.current_url == ffxiv_proc_url:
            sw.go_to_url(ffxiv_proc_url)
            worker.work(burst=True)
        add_dash_id = 'add-dash'
        sw.wait_for_elem_load(add_dash_id)
        sw.xpath_from_id_and_click(add_dash_id,
                                   load_elem_id='chart_type-selectized')
        name_id = 'name'
        sw.wait_for_elem_load(name_id)
        test_partner = 'Test-partner'
        dash_form_fill = [(test_partner, name_id),
                          ('Bar', 'chart_type-selectized', 'clear'),
                          ('vendortypename', 'dimensions-selectized', 'clear'),
                          ('clicks', 'metrics-selectized'),
                          ('Table', 'default_view-selectized', 'clear'),
                          ('Partner', 'tab-selectized', 'clear')]
        sw.send_keys_from_list(dash_form_fill)
        sw.xpath_from_id_and_click('saveDashButton')
        sw.wait_for_elem_load('.alert.alert-info', selector=sw.select_css)
        worker.work(burst=True)
        partner_tab = 'nav-partner-tab'
        assert sw.browser.find_element_by_id(partner_tab)
        sw.xpath_from_id_and_click('nav-partner-tab')
        custom_charts = sw.browser.find_element_by_id('customChartsPartner')
        sw.scroll_to_elem(custom_charts)
        worker.work(burst=True)
        assert sw.browser.find_element_by_id('partnerSummaryMetrics')
        selector = '[data-name="{}"]'.format(test_partner)
        sw.wait_for_elem_load(selector, sw.select_css)
        partner_dash = sw.browser.find_element_by_css_selector(selector)
        assert partner_dash
        dash_id = partner_dash.get_attribute('id').replace('card', '')
        show_chart = "showChartBtn{}Metrics".format(dash_id)
        sw.wait_for_elem_load(show_chart)
        assert sw.browser.find_element_by_id(show_chart)

    @pytest.fixture(scope="class")
    def request_dashboard(self, sw, worker):
        req_dash_url = '/dashboard/create'
        ffxiv_proc_url = '{}/processor/{}/{}'.format(
            base_url, urllib.parse.quote(self.test_proc_name), req_dash_url)
        sw.go_to_url(ffxiv_proc_url)
        dash_form_fill = [(self.dash, 'name'),
                          ('Area', 'chart_type-selectized', 'clear'),
                          ('clicks', 'metrics-selectized'),
                          ('Table', 'default_view-selectized', 'clear')]
        sw.send_keys_from_list(dash_form_fill)
        sw.xpath_from_id_and_click('loadContinue')
        sw.wait_for_elem_load('.alert.alert-info', selector=sw.select_css)
        worker.work(burst=True)

    def search_for_dash(self, sw):
        all_dash_url = '/dashboard/all'
        ffxiv_proc_url = '{}/processor/{}/{}'.format(
            base_url, urllib.parse.quote(self.test_proc_name), all_dash_url)
        sw.go_to_url(ffxiv_proc_url, sleep=.5)
        sw.wait_for_elem_load("[data-name='{}']", selector=sw.select_css)
        return sw.browser.find_elements_by_css_selector(
            "[data-name='{}']".format(self.dash))

    def test_request_dashboard(self, set_up, user, login, sw, worker,
                               export_test_data, request_dashboard):
        new_charts = self.search_for_dash(sw)
        assert new_charts
        new_chart = new_charts[0]
        chart_id = new_chart.get_attribute("id").replace('card', '')
        sw.xpath_from_id_and_click('{}View'.format(chart_id))
        sw.wait_for_elem_load('spinner-grow spinner-grow-sm',
                              selector=sw.select_css)
        worker.work(burst=True)
        elem_id = 'showChartBtn{}Metrics'.format(chart_id)
        sw.wait_for_elem_load(elem_id)
        view_chart_btn = sw.browser.find_element_by_id(elem_id)
        assert view_chart_btn
        metric_selectize_path = (
            "//*[@id=\"{}MetricsChartPlaceholderSelect\"]/option".format(
                chart_id))
        sw.wait_for_elem_load(metric_selectize_path, selector=sw.select_xpath)
        metric_selectize = sw.browser.find_element_by_xpath(
            metric_selectize_path)
        assert metric_selectize.get_attribute('value') == 'Clicks'

    def test_delete_dashboard(self, set_up, user, login, sw, worker,
                              request_dashboard):
        new_chart = self.search_for_dash(sw)[0]
        chart_id = new_chart.get_attribute("id").replace('card', '')
        sw.xpath_from_id_and_click('{}Delete'.format(chart_id))
        new_chart = self.search_for_dash(sw)
        assert not new_chart

    def test_report_builder(self, set_up, user, login, sw, worker,
                            update_report_in_db):
        report_url = '{}/processor/{}/edit/report_builder'.format(
            base_url, urllib.parse.quote(self.test_proc_name))
        sw.go_to_url(report_url, elem_id='reportBuilder')
        worker.work(burst=True)
        chart_bullet_id = 'dailymetricsbullet'
        chart_bullet = sw.browser.find_element_by_id(chart_bullet_id)
        sw.scroll_to_elem(chart_bullet)
        sw.wait_for_elem_load('loadingBtndailyMetricsProgress', visible=True)
        topline = sw.browser.find_element_by_id('headertopline_metrics')
        assert topline
        topline_checkbox = topline.find_element_by_xpath(
            './/input[@type="checkbox"]')
        topline_checkbox.click()
        selected = topline.get_attribute('data-selected')
        assert selected == 'false'
        worker.work(burst=True)
        daily_chart_id = 'dailyMetricsChartCol'
        sw.wait_for_elem_load(daily_chart_id, visible=True)
        daily_chart = sw.browser.find_element_by_id(daily_chart_id)
        assert daily_chart

    def test_save_report(self, set_up, user, login, sw, worker,
                         update_report_in_db):
        report_url = '{}/processor/{}/edit/report_builder'.format(
            base_url, urllib.parse.quote(self.test_proc_name))
        if not sw.browser.current_url == report_url:
            sw.go_to_url(report_url, elem_id='reportBuilder')
            worker.work(burst=True)
        report_name = 'AutoTest'
        report_date = datetime.utcnow().date()
        dash_form_fill = [(report_name, 'name-selectized', 'clear')]
        sw.send_keys_from_list(dash_form_fill)
        sw.xpath_from_id_and_click('reportBuilderSaveButton')
        worker.work(burst=True)
        processor_reports = ProcessorReports.query.filter_by(
            report_name=report_name, report_date=report_date).all()
        assert processor_reports
        title = '-'.join([self.test_proc_name, report_name, str(report_date)])
        gs = gsapi.GsApi()
        gs.input_config(gs.default_config)
        gs.get_client()
        r = gs.get_file_by_name(title)
        doc_id = r.json()['files'][0]['id']
        assert doc_id
        gs.delete_file(doc_id)
