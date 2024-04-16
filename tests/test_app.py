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
    PartnerPlacements, PlanRule, ProcessorReports, Account, Dashboard, Partner
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
    elem_form = []
    for x in form_names + select_form_names:
        form_name = x
        form_val = test_name
        if 'cur' in x or 'Select' in x or x in select_form_names:
            form_name = '{}{}'.format(form_name, select_str)
        if 'date' in form_name:
            form_val = datetime.now().strftime('%m-%d-%Y')
        elem_form.append((form_val, form_name))
    if test_name:
        sw.send_keys_from_list(elem_form)
    sw.xpath_from_id_and_click(submit_id, .1)


def get_url(url_type, obj_name=None, obj_type=None):
    url_type = url_type.__name__
    if not obj_name:
        obj_name = TestUploader.test_name
    split_idx = [0]
    idx_one_route = [plan_routes.edit_sow]
    idx_all_route = [plan_routes.plan_rules, plan_routes.plan_placements]
    if url_type in [x.__name__ for x in idx_one_route]:
        split_idx = [1]
    elif url_type in [x.__name__ for x in idx_all_route]:
        split_idx = [0, 1]
    obj_type_name = obj_type.__name__ if obj_type else url_type
    url = '{}{}'.format(base_url, obj_type_name)
    name_url = urllib.parse.quote(obj_name)
    url_split = url_type.split('_')
    url_type = '_'.join(x for i, x in enumerate(url_split) if i in split_idx)
    if len(url_split) > 2:
        url_type = '{}/{}'.format(url_type, url_split[2])
    if obj_type:
        url += '/{}/{}'.format(name_url, url_type)
    return url


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
    test_name = 'test'
    request_test_name = 'test_request'

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

    @staticmethod
    def get_url(url_type=None, p_name=None, obj_type=main_routes.processor):
        if not p_name:
            p_name = TestProcessor.test_name
        url = get_url(url_type, obj_name=p_name, obj_type=obj_type)
        return url

    def check_and_get_proc(self, sw, login, worker, set_up, url=None):
        cur_proc = Plan.query.filter_by(name=self.test_name).first()
        if not cur_proc:
            self.test_create_processor(sw, login, worker, set_up)
            cur_proc = Plan.query.filter_by(name=self.test_name).first()
        if url:
            elem_id = ''.join(x.capitalize() for x in url.__name__.split('_'))
            url = self.get_url(url)
            sw.go_to_url(url, elem_id='loadingBtn{}'.format(elem_id))
            worker.work(burst=True)
        return cur_proc

    def test_create_processor(self, sw, login, worker, set_up):
        create_url = '{}create_processor'.format(base_url)
        submit_id = 'loadContinue'
        sw.go_to_url(create_url, elem_id=submit_id)
        form = ['cur_client', 'cur_product', 'cur_campaign', 'description',
                'name']
        submit_form(sw, form_names=form, submit_id=submit_id)
        worker.work(burst=True)
        sw.wait_for_elem_load('refresh_imports')
        import_url = self.get_url(main_routes.edit_processor_import)
        assert sw.browser.current_url == import_url
        cur_proc = Processor.query.filter_by(name=self.test_name).first()
        assert cur_proc

    def test_processor_page(self, sw, set_up, worker):
        proc_link = '//*[@id="navLinkProcessor"]'
        sw.click_on_xpath(proc_link, 1)
        worker.work(burst=True)
        assert sw.browser.current_url == '{}processor'.format(base_url)

    def test_request_processor(self, sw, set_up, worker):
        self.test_processor_page(sw, set_up, worker)
        request_id = 'requestProcessor'
        load_btn_id = 'loadingBtn{}'.format(request_id)
        sw.xpath_from_id_and_click(request_id, load_elem_id=load_btn_id)
        worker.work(burst=True)
        new_request_id = 'modalSubmitRequestProcessor'
        sw.wait_for_elem_load(new_request_id, visible=True)
        sw.xpath_from_id_and_click(new_request_id)
        sw.wait_for_elem_load('description')
        request_url = get_url(main_routes.request_processor)
        assert sw.browser.current_url == request_url
        form = ['name', 'description', 'plan_path', 'cur_client', 'cur_product',
                'cur_campaign', 'start_date', 'end_date', '']
        submit_form(sw, form_names=form, test_name=self.request_test_name)
        p = Processor.query.filter_by(name=self.request_test_name).first()
        assert p
        return p

    @staticmethod
    def add_account_card(sw, set_up, idx, cur_proc):
        account_types = ['Adwords', 'Facebook', 'DCM']
        test_name = account_types[idx]
        elem_id = 'add_child'
        accounts_id = 'accounts-{}'.format(idx)
        sw.xpath_from_id_and_click(elem_id, load_elem_id=accounts_id)
        form_names = ['account_id', 'campaign_id']
        select_form_names = ['{}-key'.format(accounts_id)]
        form_names = ['{}-{}'.format(accounts_id, x) for x in form_names]
        submit_form(sw, form_names=form_names,
                    select_form_names=select_form_names,
                    submit_id='loadRefresh', test_name=test_name)
        cur_acc = Account.query.filter_by(processor_id=cur_proc.id).all()
        assert len(cur_acc) == (idx + 1)
        cur_acc = Account.query.filter_by(
            processor_id=cur_proc.id, key=test_name).first()
        assert cur_acc
        assert cur_acc.account_id == test_name
        assert cur_acc.campaign_id == test_name

    def test_accounts(self, sw, set_up, worker):
        p = Processor.query.filter_by(name=self.request_test_name).first()
        if not p:
            p = self.test_request_processor(sw, set_up, worker)
        url = self.get_url(main_routes.edit_processor_accounts, p_name=p.name)
        sw.go_to_url(url, elem_id='add_child')
        for idx in range(3):
            self.add_account_card(sw, set_up, idx, p)

    def test_processor_plan(self, sw, login, worker, set_up):
        p = Processor.query.filter_by(name=self.request_test_name).first()
        if not p:
            p = self.test_request_processor(sw, set_up, worker)
        url = self.get_url(main_routes.edit_processor_plan, p_name=p.name)
        load_btn_elem_id = 'loadingBtnrowOne'
        sw.go_to_url(url, elem_id=load_btn_elem_id)
        worker.work(burst=True)
        TestUploader.upload_plan(sw, worker)
        sw.wait_for_elem_load(elem_id=load_btn_elem_id)
        for x in range(10):
            time.sleep(.1)
            t = worker.work(burst=True)
            if t:
                break
        sw.wait_for_elem_load(elem_id='newPlanResultsCardCol')

    @staticmethod
    def edit_fees(sw, worker, p, url):
        dig_fees = 'digital_agency_fees'
        elem_id = 'refresh_rate_card'
        load_elem_id = 'loadingBtn{}'.format(elem_id)
        sw.xpath_from_id_and_click(elem_id, load_elem_id=load_elem_id)
        worker.work(burst=True)
        elem_id = 'modalTablerate_cardvendorkey_info'
        sw.wait_for_elem_load(elem_id)
        save_btn = 'modalTableSaveButton'
        sw.xpath_from_id_and_click(save_btn, load_elem_id=dig_fees)
        worker.work(burst=True)
        sw.go_to_url(url, elem_id=dig_fees)
        form_names = [dig_fees, 'trad_agency_fees']
        fee_val = '.1'
        submit_form(sw, form_names=form_names, test_name='.1')
        assert float(p.digital_agency_fees) == float(fee_val)
        assert float(p.trad_agency_fees) == float(fee_val)

    def test_fees(self, sw, set_up, worker):
        p = Processor.query.filter_by(name=self.request_test_name).first()
        if not p:
            p = self.test_request_processor(sw, set_up, worker)
        url = self.get_url(main_routes.edit_processor_fees, p_name=p.name)
        dig_fees = 'digital_agency_fees'
        sw.go_to_url(url, elem_id=dig_fees)
        self.edit_fees(sw, worker, p, url)

    @staticmethod
    def click_to_plan(sw, cur_proc):
        c_url = '{}{}'.format(base_url, main_routes.clients.__name__)
        elem_id = 'headingUser1'
        sw.go_to_url(c_url, elem_id=elem_id)
        proc_elem_id = 'headingProcessor{}'.format(cur_proc.id)
        sw.xpath_from_id_and_click(elem_id, load_elem_id=proc_elem_id)
        plan_elem_id = 'createPlan{}'.format(cur_proc.id)
        sw.xpath_from_id_and_click(proc_elem_id, load_elem_id=plan_elem_id)
        sw.xpath_from_id_and_click(plan_elem_id, load_elem_id='client_requests')

    def test_create_plan_from_processor(self, sw, set_up, worker, login):
        p = Processor.query.filter_by(name=self.request_test_name).first()
        if not p:
            p = self.test_request_processor(sw, set_up, worker)
        self.click_to_plan(sw, p)
        TestPlan().test_topline(sw, login, worker, plan_name=p.name)
        self.click_to_plan(sw, p)

    def add_import_card(self, worker, sw, default_name, name):
        with self.adjust_path(basedir):
            proc_url = self.get_url(main_routes.edit_processor_import,
                                    p_name=default_name)
            add_child_id = 'add_child'
            sw.go_to_url(proc_url, elem_id=add_child_id)
            sw.xpath_from_id_and_click(add_child_id,
                                       load_elem_id='apis-0-key-selectized')
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
            proc_url = self.get_url(main_routes.edit_processor_import,
                                    p_name=default_name)
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
        test_name = 'Rawfile'
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

    def test_get_log(self, set_up, sw, worker, login, default_name):
        proc_url = self.get_url(main_routes.edit_processor_import,
                                p_name=default_name)
        sw.go_to_url(proc_url)
        TestUploader().test_get_log(sw, login, worker, cur_name=default_name)


class TestPlan:
    test_name = 'testPlan'

    def get_url(self, url_type=None, plan_name=None, obj_prefix=True):
        obj_type = plan_routes.plan if obj_prefix else None
        if not plan_name:
            plan_name = self.test_name
        url = get_url(url_type, obj_name=plan_name, obj_type=obj_type)
        return url

    def check_and_get_plan(self, sw, login, worker, url=None):
        cur_plan = Plan.query.filter_by(name=self.test_name).first()
        if not cur_plan:
            self.test_topline(sw, login, worker)
            cur_plan = Plan.query.filter_by(name=self.test_name).first()
        if url:
            elem_id = ''.join(x.capitalize() for x in url.__name__.split('_'))
            url = self.get_url(url)
            sw.go_to_url(url, elem_id='loadingBtn{}'.format(elem_id))
            worker.work(burst=True)
        return cur_plan

    def test_create_plan(self, sw, login, worker, plan_name=None):
        if not plan_name:
            plan_name = self.test_name
        create_url = self.get_url(url_type=plan_routes.plan, obj_prefix=False)
        form_names = ['cur_client', 'cur_product', 'cur_campaign',
                      'description', 'name']
        sw.go_to_url(create_url, elem_id=form_names[0])
        submit_form(sw, form_names, test_name=plan_name)
        p = Plan.query.filter_by(name=plan_name).first()
        assert p.name == plan_name
        edit_url = self.get_url(plan_routes.topline, plan_name=plan_name)
        assert sw.browser.current_url == edit_url

    @staticmethod
    def set_topline_cost(worker, sw, part_budget='5000',
                         submit_id='loadContinue', col_id='total_budget0'):
        idx = str(0)
        elem_id = 'tr{}'.format(idx)
        col_id = col_id.replace('0', idx)
        sw.wait_for_elem_load(elem_id)
        wait_for_id = 'partnerSelect{}-selectized'.format(idx)
        sw.xpath_from_id_and_click(elem_id, load_elem_id=wait_for_id)
        sw.wait_for_elem_load(col_id)
        elem = sw.browser.find_element_by_id(col_id)
        if [x for x in ['deleteRow', 'total_budget'] if x in col_id]:
            elem.click()
        if 'total_budget' in col_id:
            elem.clear()
        submit_form(sw, [col_id], test_name=part_budget,
                    submit_id=submit_id)
        worker.work(burst=True)

    @staticmethod
    def verify_plan_create(p, part_budget='5000', part_name=None):
        if not part_name:
            part_name = []
        phase = p.get_current_children()
        assert len(phase) == 1
        parts = phase[0].get_current_children()
        assert len(parts) == len(part_name)
        assert len(p.get_placements()) == len(part_name)
        for part in parts:
            assert part.name in part_name
            assert int(part.total_budget) == int(part_budget)
            assert int(part.estimated_cpc) == 1
            assert int(part.estimated_cpm) == 1000
            place = PartnerPlacements.query.filter_by(partner_id=part.id).all()
            assert len(place) == 1

    def topline_add_partner(self, sw, worker, part_name):
        sel_id = 'partnerSelectAdd'
        submit_id = 'addRowsTopline'
        part_budget = '5000'
        sw.wait_for_elem_load(submit_id)
        submit_form(sw, form_names=[sel_id], submit_id=submit_id,
                    test_name=part_name)
        self.set_topline_cost(worker, sw, part_budget)

    def test_topline(self, sw, login, worker, plan_name=None):
        if not plan_name:
            plan_name = self.test_name
        edit_url = self.get_url(plan_routes.topline, plan_name=plan_name)
        if not sw.browser.current_url == edit_url:
            self.test_create_plan(sw, login, worker, plan_name=plan_name)
        assert sw.browser.current_url == edit_url
        worker.work(burst=True)
        TestProject.wait_for_jobs_finish()
        part_name = 'Facebook'
        self.topline_add_partner(sw, worker, part_name=part_name)
        sow_url = self.get_url(plan_routes.edit_sow, plan_name=plan_name)
        p = Plan.query.filter_by(name=plan_name).first()
        self.verify_plan_create(p, part_name=[part_name])
        assert sw.browser.current_url == sow_url
        sw.go_to_url(edit_url, elem_id='loadingBtnTopline')
        worker.work(burst=True)
        phase_id = 'rowpartner_type-1'
        sw.wait_for_elem_load(phase_id)
        sw.xpath_from_id_and_click(phase_id, load_elem_id='total_budget-1')
        phase_id = 'phaseSelect-1'
        submit_form(sw, [phase_id], test_name=plan_name)
        worker.work(burst=True)
        sw.wait_for_elem_load('project_name')
        p = Plan.query.filter_by(name=plan_name).first()
        phase = p.get_current_children()
        assert phase[0].name == plan_name

    def test_topline_rename_partner(self, sw, login, worker):
        url = plan_routes.topline
        p = self.check_and_get_plan(sw, login, worker, url)
        new_part_name = 'FB'
        self.set_topline_cost(worker, sw, part_budget=new_part_name,
                              col_id='partnerSelect0')
        self.verify_plan_create(p, part_name=[new_part_name])
        df = pd.DataFrame([x.get_form_dict() for x in p.rules])
        tdf = df[df[Partner.__name__] == 'ALL']
        assert tdf.empty

    def test_rules(self, sw, login, worker):
        url = plan_routes.plan_rules
        p = self.check_and_get_plan(sw, login, worker, url)
        elem_id = 'rowplace_col0'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        col_name = PartnerPlacements.environment.name
        assert elem.get_attribute('innerHTML').strip() == col_name
        search_id = 'tableSearchInputPlanRulesTable'
        submit_form(sw, form_names=[search_id], submit_id=elem_id,
                    test_name=col_name)
        elem_id = 'rule_info01SliderKey-selectized'
        second_name = '{}1'.format(self.test_name)
        add_row_id = 'rule_info0AddRow'
        submit_form(sw, [elem_id], submit_id=add_row_id,
                    test_name=self.test_name)
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
        submit_form(sw, [elem_id], test_name='50',
                    submit_id='loadRefresh')
        worker.work(burst=True)
        env_rule = PlanRule.query.filter_by(place_col=col_name,
                                            plan_id=p.id).all()
        assert len(env_rule) == 1
        data = json.loads(env_rule[0].rule_info)
        assert data[self.test_name] == .5
        assert data[second_name] == .5

    def test_rules_lookup(self, sw, login, worker):
        url = plan_routes.plan_rules
        p = self.check_and_get_plan(sw, login, worker, url)
        col_name = PartnerPlacements.environment.name
        env_rule = PlanRule.query.filter_by(place_col=col_name,
                                            plan_id=p.id).all()
        data = env_rule[0].rule_info
        if self.test_name not in data:
            self.test_rules(sw, login, worker)
        col_name = PartnerPlacements.budget.name
        elem_id = 'rowplace_col1'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        assert elem.get_attribute('innerHTML').strip() == col_name
        search_id = 'tableSearchInputPlanRulesTable'
        elem = sw.browser.find_element_by_id(search_id)
        elem.clear()
        submit_form(sw, form_names=[search_id], submit_id=elem_id,
                    test_name=col_name)
        elem_id = 'typeSelect1-selectized'
        sw.wait_for_elem_load(elem_id)
        sw.send_keys_from_list([('Lookup', elem_id)])
        elem_id = 'rule_info1LookupCol-selectized'
        sw.wait_for_elem_load(elem_id)
        env_col = PartnerPlacements.environment.name
        sw.send_keys_from_list([(env_col, elem_id)])
        elem_id = 'rule_info1LookupContainer0-selectized'
        elem_two_id = elem_id.replace('0', '1')
        sw.wait_for_elem_load(elem_id)
        val_one = 'a'
        val_two = 'b'
        sw.send_keys_from_list([(val_one, elem_id), (val_two, elem_two_id)])
        submit_form(sw, form_names=[elem_id], test_name=self.test_name)
        worker.work(burst=True)
        cur_places = p.get_placements()
        poss_vals = [self.test_name, val_one, val_two]
        assert len(cur_places) == 3
        for cur_place in cur_places:
            val = cur_place.__dict__[col_name]
            assert val in poss_vals
            poss_vals = [x for x in poss_vals if x != val]
            if cur_place.__dict__[env_col] == self.test_name:
                assert val in [self.test_name, val_one]
            else:
                assert val == val_two

    def test_placements(self, sw, login, worker):
        url = plan_routes.plan_placements
        cur_plan = self.check_and_get_plan(sw, login, worker, url)
        cur_place = cur_plan.get_placements()
        cur_place = cur_place[0]
        clicks = cur_place.total_budget / cur_place.cpc
        load_elem_id = 'budget0'
        elem_id = 'row{}'.format(load_elem_id)
        sw.wait_for_elem_load(elem_id)
        click_cell_id = 'rowClicks0'
        elem = sw.browser.find_element_by_id(click_cell_id)
        elem_clicks = elem.get_attribute('innerHTML').replace(',', '')
        assert int(elem_clicks) == int(clicks)
        sw.xpath_from_id_and_click(elem_id, load_elem_id=load_elem_id)
        elem = sw.browser.find_element_by_id(load_elem_id)
        elem.clear()
        sw.xpath_from_id_and_click(elem_id, load_elem_id=load_elem_id)
        submit_form(sw, [load_elem_id], test_name=self.test_name)
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
        url = plan_routes.plan_placements
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
        url = plan_routes.topline
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
        url = plan_routes.calc
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
        edit_url = self.get_url(plan_routes.rfp, plan_name)
        sw.go_to_url(edit_url, elem_id='form_continue')
        worker.work(burst=True)
        elem_id = 'rowrfp_file_id0'
        sw.wait_for_elem_load(elem_id)
        elem = sw.browser.find_element_by_id(elem_id)
        assert elem.get_attribute('innerHTML').strip() == '1'

    @staticmethod
    def click_on_new_window_link(sw, elem_id, load_elem_id):
        a_xpath = '//*[@id="{}"]/a'.format(elem_id)
        sw.wait_for_elem_load(a_xpath, selector=sw.select_xpath, visible=True)
        sw.click_on_xpath(a_xpath)
        window_handles = sw.browser.window_handles
        sw.browser.close()
        sw.browser.switch_to.window(window_handles[1])
        sw.wait_for_elem_load(elem_id=load_elem_id)

    def test_checklist(self, sw, login, worker):
        url = plan_routes.checklist
        self.check_and_get_plan(sw, login, worker, url)
        elem_id = 'rowroute3'
        self.click_on_new_window_link(sw, elem_id, 'loadingBtnTopline')
        worker.work(burst=True)
        assert sw.browser.current_url == self.get_url(plan_routes.topline)

    def test_fees(self, sw, login, worker):
        url = plan_routes.fees
        p = self.check_and_get_plan(sw, login, worker, url)
        url = self.get_url(url)
        assert sw.browser.current_url == url
        TestProcessor.edit_fees(sw, worker, p, url)

    def test_add_delete_phase(self, sw, login, worker):
        url = plan_routes.topline
        p = self.check_and_get_plan(sw, login, worker, url)
        add_top_row_id = 'addTopRowTopline'
        submit_id = 'loadRefresh'
        sw.wait_for_elem_load(add_top_row_id)
        sw.xpath_from_id_and_click(add_top_row_id, load_elem_id='datePicker-2')
        sw.xpath_from_id_and_click(submit_id, load_elem_id=submit_id)
        worker.work(burst=True)
        assert len(p.get_current_children()) == 2
        top_row_elem_id = 'topRowHeaderTopline-2'
        delete_id = 'deleteRow-2'
        sw.wait_for_elem_load(top_row_elem_id)
        sw.xpath_from_id_and_click(top_row_elem_id, load_elem_id=delete_id)
        sw.xpath_from_id_and_click(delete_id, load_elem_id=add_top_row_id)
        sw.xpath_from_id_and_click(submit_id, load_elem_id=submit_id)
        worker.work(burst=True)
        assert len(p.get_current_children()) == 1

    def test_topline_delete_partner(self, sw, login, worker):
        url = plan_routes.topline
        p = self.check_and_get_plan(sw, login, worker, url)
        self.set_topline_cost(worker, sw, part_budget='',
                              submit_id='loadRefresh', col_id='deleteRow0')
        self.verify_plan_create(p)


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
        TestPlan.click_on_new_window_link(sw, elem_id, 'project_number')
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

    @staticmethod
    def get_url(url_type=None, up_name=None, obj_type=main_routes.uploader):
        if not up_name:
            up_name = TestUploader.test_name
        url = get_url(url_type, obj_name=up_name, obj_type=obj_type)
        return url

    def test_create_uploader(self, sw, login, worker):
        c_url = self.get_url(main_routes.create_uploader, obj_type=None)
        form_names = ['cur_client', 'cur_product', 'cur_campaign',
                      'description', 'name']
        sw.go_to_url(c_url, elem_id=form_names[0])
        submit_form(sw, form_names, test_name=self.test_name)
        worker.work(burst=True)
        cur_up = Uploader.query.filter_by(name=self.test_name).first()
        assert cur_up.name == self.test_name
        cam_url = self.get_url(main_routes.edit_uploader_campaign)
        assert sw.browser.current_url == cam_url
        up_main_file = os.path.join(cur_up.local_path, 'main.py')
        assert os.path.isfile(up_main_file)
        return cur_up

    def check_create_uploader(self, sw, login, worker):
        cur_up = Uploader.query.filter_by(name=self.test_name).first()
        if not cur_up:
            cur_up = self.test_create_uploader(sw, login, worker)
        e_url = self.get_url(main_routes.edit_uploader)
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
            if 'saved.' in elem.get_attribute('innerHTML'):
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
        c_url = self.get_url(main_routes.edit_uploader_campaign)
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

    def test_get_log(self, sw, login, worker, cur_name=''):
        if not cur_name:
            cur_up = self.check_create_uploader(sw, login, worker)
            cur_name = cur_up.name
        elem_id = 'getLog'
        label_id = 'logModalLabel'
        sw.wait_for_elem_load(elem_id)
        sw.xpath_from_id_and_click(elem_id, load_elem_id=label_id)
        worker.work(burst=True)
        elem = sw.browser.find_element_by_id(label_id)
        assert cur_name in elem.get_attribute('innerHTML')


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
        os.chdir(os.path.join(basedir, Processor.__name__))
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
        dash_name = 'Test'
        dash_form_fill = [(dash_name, 'name'),
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
        dash = Dashboard.query.filter_by(name=dash_name).all()
        assert dash
        custom_charts ='customChartsTopline'
        sw.wait_for_elem_load(custom_charts)
        chart = sw.browser.find_element_by_id(custom_charts)
        sw.scroll_to_elem(chart)
        dash_1 = 'dash1Metrics'
        sw.wait_for_elem_load(dash_1)
        worker.work(burst=True)
        show_chart_id = "showChartBtndash1Metrics"
        sw.wait_for_elem_load(show_chart_id)
        assert sw.browser.find_element_by_id(dash_1)
        assert sw.browser.find_element_by_id(show_chart_id)
        metric_selectize_path = (
            "//*[@id=\"dash1MetricsChartPlaceholderSelect\"]/option")
        sw.wait_for_elem_load(metric_selectize_path, selector=sw.select_xpath)
        metric_selectize = sw.browser.find_element_by_xpath(
            metric_selectize_path)
        assert metric_selectize.get_attribute('value') == 'Impressions'
        sw.xpath_from_id_and_click('dash1Delete')
        dash = Dashboard.query.filter_by(name=dash_name).all()
        assert not dash

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

    def request_dashboard(self, sw, worker, name, chart='Area',
                          metrics='clicks', default_view='Table',
                          include_in_report=False):
        req_dash_url = '/dashboard/create'
        ffxiv_proc_url = '{}/processor/{}/{}'.format(
            base_url, urllib.parse.quote(self.test_proc_name), req_dash_url)
        sw.go_to_url(ffxiv_proc_url)
        dash_form_fill = [(name, 'name'),
                          (chart, 'chart_type-selectized', 'clear'),
                          (metrics, 'metrics-selectized'),
                          (default_view, 'default_view-selectized', 'clear')]
        if include_in_report:
            dash_form_fill.append(('check', 'include_in_report'))
        sw.send_keys_from_list(dash_form_fill)
        sw.xpath_from_id_and_click('loadContinue')
        sw.wait_for_elem_load('.alert.alert-info', selector=sw.select_css)
        worker.work(burst=True)

    def search_for_dash(self, sw, name):
        all_dash_url = '/dashboard/all'
        ffxiv_proc_url = '{}/processor/{}/{}'.format(
            base_url, urllib.parse.quote(self.test_proc_name), all_dash_url)
        sw.go_to_url(ffxiv_proc_url, sleep=.5)
        sw.wait_for_elem_load("[data-name='{}']", selector=sw.select_css)
        return sw.browser.find_elements_by_css_selector(
            "[data-name='{}']".format(name))

    def test_request_dashboard(self, set_up, user, login, sw, worker,
                               export_test_data):
        dash_name = 'requestTest'
        self.request_dashboard(sw, worker, dash_name)
        new_charts = self.search_for_dash(sw, dash_name)
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

    def test_delete_dashboard(self, set_up, user, login, sw, worker):
        dash_name = 'deleteTest'
        self.request_dashboard(sw, worker, 'deleteTest')
        new_chart = self.search_for_dash(sw, dash_name)[0]
        chart_id = new_chart.get_attribute("id").replace('card', '')
        sw.xpath_from_id_and_click('{}Delete'.format(chart_id))
        new_chart = self.search_for_dash(sw, dash_name)
        assert not new_chart

    def test_report_builder(self, set_up, user, login, sw, worker,
                            update_report_in_db):
        custom_dash_name ='report_build_test'
        self.request_dashboard(sw, worker, custom_dash_name,
                               include_in_report=True)
        report_url = '{}/processor/{}/edit/report_builder'.format(
            base_url, urllib.parse.quote(self.test_proc_name))
        sw.go_to_url(report_url, elem_id='reportBuilder')
        sw.wait_for_elem_load('reportBuilder')
        worker.work(burst=True)
        topline = sw.browser.find_element_by_id('headertopline_metrics')
        assert topline
        sw.click_on_xpath(
            '//*[@id="headertopline_metrics"]//input[@type="checkbox"]')
        selected = topline.get_attribute('data-selected')
        assert selected == 'false'
        header1_id = 'headerdelivery,delivery_completion'
        header2_id = 'headerkpi_col'
        header1 = sw.browser.find_element_by_xpath(
            '//*[@id="{}"]//i'.format(header1_id))
        header2 = sw.browser.find_element_by_xpath(
            '//*[@id="{}"]//i'.format(header2_id))
        sw.drag_and_drop(header1, header2)
        assert sw.get_element_order(header2_id, header1_id)
        chart_bullet_id = 'dailymetricsbullet'
        chart_bullet = sw.browser.find_element_by_id(chart_bullet_id)
        sw.scroll_to_elem(chart_bullet)
        sw.wait_for_elem_load('loadingBtn{}Progress'.format(
            chart_bullet_id), visible=True)
        worker.work(burst=True)
        daily_chart_id = 'dailyMetricsChartCol'
        sw.wait_for_elem_load(daily_chart_id, visible=True)
        daily_chart = sw.browser.find_element_by_id(daily_chart_id)
        assert daily_chart
        custom_dash = Dashboard.query.filter_by(name=custom_dash_name).first()
        custom_dash_id = 'dash{}Metrics'.format(custom_dash.id)
        end_of_page = sw.browser.find_element_by_id('rowOne')
        sw.scroll_to_elem(end_of_page)
        sw.wait_for_elem_load('loadingBtn{}Progress'.format(
            custom_dash_id), visible=True)
        worker.work(burst=True)
        custom_chart_id = '{}ChartCol'.format(custom_dash_id)
        sw.wait_for_elem_load(custom_chart_id)
        custom_chart = sw.browser.find_element_by_id(custom_chart_id)
        assert custom_chart

    def test_save_report(self, set_up, user, login, sw, worker,
                         update_report_in_db):
        report_url = '{}/processor/{}/edit/report_builder'.format(
            base_url, urllib.parse.quote(self.test_proc_name))
        if not sw.browser.current_url == report_url:
            sw.go_to_url(report_url, elem_id='reportBuilder')
            worker.work(burst=True)
        report_name = 'AutoTest'
        report_date = datetime.utcnow().date()
        submit_form(sw, select_form_names=['name'], test_name=report_name,
                    submit_id='reportBuilderSaveButton')
        sw.wait_for_elem_load('alertPlaceholder')
        for x in range(50):
            report_saved = worker.work(burst=True)
            if report_saved:
                break
            time.sleep(.1)
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
