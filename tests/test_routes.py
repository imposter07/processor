import os
import pytest
import pandas as pd
import datetime as dt
from config import basedir
import processor.reporting.utils as utl
import processor.reporting.analyze as az
import processor.reporting.vmcolumns as vmc
import processor.reporting.models as prc_model
import app.main.routes as main_routes
import app.utils as app_utl
from app import db
from app.models import Conversation, Plan, PlanPhase, User, Partner, Task, \
    Chat, Uploader, Project, PartnerPlacements, Campaign, PlanRule, Client, \
    Product, Processor, Account, RequestLog, RateCard
import app.tasks as app_tasks


def test_index(client, user):
    response = client.get('/')
    assert response.status_code == 200
    assert 'Hi, {}'.format(user.username) in response.text


def test_health_check(client):
    expected_output = {'data': 'success'}
    response = client.get('/health_check')
    assert response.json == expected_output


def test_speed_test(client):
    test_url = '/speed_test'
    response = client.get(test_url)
    assert response.json['data'] < 1.1
    request_log = RequestLog.query.filter_by(url=test_url).first()
    assert test_url in request_log.url


class TestChat:

    @pytest.fixture(scope='class')
    def conversation(self, user, app_fixture):
        message = 'test message'
        conv = Conversation(name=message, user_id=user.id,
                            created_at=dt.datetime.utcnow())
        db.session.add(conv)
        db.session.commit()
        yield conv
        chats = Chat.query.filter_by(conversation_id=conv.id).all()
        for c in chats:
            db.session.delete(c)
            db.session.commit()
        db.session.delete(conv)
        db.session.commit()

    def test_get_conversation(self, client, conversation):
        expected_response = {'id': conversation.id, 'chats': []}
        form = {'conversation_id': conversation.id}
        response = client.post('/get_conversation', data=form)
        assert response.status_code == 200
        assert response.json == expected_response

    @staticmethod
    def wait_for_jobs_finish(p):
        for x in range(10):
            t = Task.query.filter_by(plan_id=p.id, complete=False).first()
            if t:
                t.wait_and_get_job(loops=10)
            else:
                break
        return True

    def verify_plan_create(self, conversation_id, prompt_dict):
        budget_col = Partner.total_budget.name
        cu = db.session.get(User, conversation_id)
        if Plan.name.name in prompt_dict:
            name = prompt_dict[Plan.name.name]
        else:
            name = cu.username
        p = Plan.query.filter_by(name=name).first()
        phase = p.get_current_children()[0]
        assert phase.name == PlanPhase.get_name_list()[0]
        total_budget = 0
        for part in phase.get_current_children():
            assert part.name in prompt_dict[Partner.__table__.name]
            idx = prompt_dict[Partner.__table__.name].index(part.name)
            total_budget = prompt_dict[budget_col][idx]
            total_budget = total_budget.lower().replace('k', '000')
            total_budget = int(total_budget.replace(',', '').replace('$', ''))
            assert int(part.total_budget) == total_budget
            for col in [Plan.start_date.name, Plan.end_date.name]:
                if col in prompt_dict:
                    val = part.__dict__[col]
                    new_val = utl.string_to_date(prompt_dict[col][0]).date()
                    assert val == new_val
        self.wait_for_jobs_finish(p)
        df = p.get_placements_as_df()
        o_keys = ['message', Partner.__table__.name, budget_col,
                  PartnerPlacements.end_date.name, PartnerPlacements.name.name]
        for k, v in prompt_dict.items():
            if k not in o_keys:
                budget = total_budget / len(v)
                tdf = df.groupby(k)[budget_col].sum()
                tdf = tdf.reset_index()
                tdf[budget_col] = tdf[budget_col].astype(float)
                tdf = tdf.sort_values(k).reset_index(drop=True)
                cdf = {k: v, Partner.total_budget.name: len(v) * [budget]}
                cdf = pd.DataFrame(cdf)
                cdf = cdf.sort_values(k).reset_index(drop=True)
                if 'date' in k:
                    cdf = utl.data_to_type(cdf, date_col=[k])
                    tdf = utl.data_to_type(tdf, date_col=[k])
                assert pd.testing.assert_frame_equal(tdf, cdf) is None

    def send_post_verify_response(self, client, conversation, msg,
                                  verify_success_msg=True):
        data = {Chat.conversation_id.name: conversation.id, 'message': msg}
        response = client.post('/post_chat', data=data)
        success_msg = az.AliChat.create_success_msg
        assert response.status_code == 200
        if verify_success_msg:
            assert response.json['response'][:len(success_msg)] == success_msg
        return response

    def test_plan_create(self, client, conversation, worker, prompt_dict=None,
                         name=None):
        if not prompt_dict:
            prompt_dict = Plan.get_large_create_prompt(prompt_dict=True)
        msg = prompt_dict['message']
        self.send_post_verify_response(client, conversation, msg)
        worker.work(burst=True)
        self.verify_plan_create(conversation.user_id, prompt_dict)
        if not name:
            cu = db.session.get(User, conversation.user_id)
            name = cu.username
        p = Plan.query.filter_by(name=name).first()
        assert p.name == name

    def test_plan_edit(self, client, conversation, worker):
        worker.work(burst=True)
        prompt_dict = Plan.get_large_create_prompt(prompt_dict=True)
        cu = db.session.get(User, conversation.user_id)
        p = Plan.query.filter_by(name=cu.username).first()
        if not p:
            self.test_plan_create(client, conversation, worker)
            p = Plan.query.filter_by(name=cu.username).first()
        assert p.name == cu.username
        prompts = p.get_create_prompt(wrap_html=False)
        partner_name = prompt_dict[Partner.__table__.name][0]
        new_budget = 5000
        new_env = 'Mobile'
        new_env_per = '75'
        for prompt in prompts:
            msg = prompt
            if 'partner_name' in prompt:
                msg = msg.replace('partner_name', partner_name)
                msg = msg.replace('new_budget', str(new_budget))
            if 'num' in prompt:
                msg = msg.replace('num', '{} {} {}%'.format(
                    partner_name, new_env, new_env_per))
            if Uploader.__table__.name in msg:
                continue
            self.send_post_verify_response(client, conversation, msg, False)
        parts = p.get_current_children()[0].get_current_children()
        part = parts[0]
        assert int(part.total_budget) == int(new_budget)
        prompt_dict[Partner.total_budget.name] = [str(new_budget)]
        worker.work(burst=True)
        self.wait_for_jobs_finish(p)
        env_rule = PlanRule.query.filter_by(
            place_col=PartnerPlacements.environment.name, partner_id=part.id,
            plan_id=p.id).first()
        rule_info = env_rule.rule_info
        assert rule_info[new_env] == int(new_env_per) / 100
        assert len(parts) == 1

    def test_create_project(self, client, conversation, worker):
        pn = '12345'
        msg = 'Create a {} with {} {}'.format(
            Project.__table__.name, Project.project_number.name, pn)
        self.send_post_verify_response(client, conversation, msg)
        p = Project.query.filter_by(project_number=pn).first()
        assert p.project_number == pn
        c = Campaign.query.filter_by(
            name=Campaign.get_default_name()[0]).first()
        assert p.campaign_id == c.id

    def test_plan_create_from_another(self, client, conversation, worker):
        prompt_dict = Plan().get_create_from_another_prompt()
        pn = '12345'
        p = Project.query.filter_by(project_number=pn).first()
        if not p:
            self.test_create_project(client, conversation, worker)
            p = Project.query.filter_by(project_number=pn).first()
        prompt_dict[Plan.name.name] = p.name
        self.test_plan_create(client, conversation, worker, prompt_dict, p.name)
        plan = Plan.query.filter_by(name=p.name).first()
        assert p.campaign_id == plan.campaign_id


class TestUtils:

    @staticmethod
    def check_and_add_parents():
        name = Client.get_default_name()[0]
        cli = Client(name=name).check_and_add()
        pro = Product(name=name, client_id=cli.id).check_and_add()
        cam = Campaign(name=name, product_id=pro.id).check_and_add()
        return name, cli, pro, cam

    def test_parse_filter_dict_from_clients(self, user, app_fixture):
        name, cli, pro, cam = self.check_and_add_parents()
        new_name = '{}0'.format(name)
        cam1 = Campaign(name=new_name, product_id=pro.id).check_and_add()
        for idx, cur_name in enumerate([name, new_name]):
            cam_ids = [cam.id]
            if idx > 0:
                cam_ids.append(cam1.id)
            for cam_id in cam_ids:
                p = Project(project_number=cur_name, campaign_id=cam_id)
                db.session.add(p)
                db.session.commit()
        p = Project.query.filter_by(project_number=name).first()
        assert p.project_number == name
        filter_dict = [{Project.__table__.name: [name]}]
        results = Project.query
        objs = app_utl.parse_filter_dict_from_clients(
            results, None, None, filter_dict, db_model=Project)
        objs = objs.all()
        assert len(objs) == 1
        assert objs[0].project_number == name
        filter_dict = [{Campaign.__table__.name: [cam.name]}]
        p = Project.query.filter_by(campaign_id=cam.id).all()
        objs = app_utl.parse_filter_dict_from_clients(
            results, None, None, filter_dict, db_model=Project)
        objs = objs.all()
        assert len(objs) == len(p)

    def test_get_next_route_from_buttons(self, user, app_fixture):
        prefix = os.path.dirname(main_routes.__file__).split('\\')[-1]
        cur_route = main_routes.edit_processor_accounts.__name__
        cur_route = '{}.{}'.format(prefix, cur_route)
        next_route = main_routes.edit_processor_fees.__name__
        next_route = '{}.{}'.format(prefix, next_route)
        edit_name = cur_route.split('_')[-1].capitalize()
        buttons = Processor().get_navigation_buttons('ProcessorRequest')
        cr, nr = app_utl.get_next_route_from_buttons(buttons, edit_name)
        assert cr == cur_route
        assert nr == next_route


class TestTasks:

    @staticmethod
    def create_test_processor(db_object=Processor):
        name, cli, pro, cam = TestUtils.check_and_add_parents()
        proc = db_object(name=name, campaign_id=cam.id)
        db.session.add(proc)
        db.session.commit()
        return proc

    def test_duplicate_in_db(self, user, app_fixture):
        old_proc = self.create_test_processor()
        name = old_proc.name
        new_name = '{}0'.format(name)
        form_data = {'new_name': new_name}
        for col in ['new_start_date', 'new_end_date']:
            form_data[col] = dt.datetime.today()
        new_id = app_tasks.duplicate_processor_in_db(old_proc.id, 1, form_data)
        new_proc = db.session.get(Processor, new_id)
        assert new_proc.name == new_name
        assert new_name in new_proc.local_path
        for cur_proc in [old_proc, new_proc]:
            db.session.delete(cur_proc)

    def test_set_db_values(self, user, app_fixture):
        old_proc = self.create_test_processor()
        new_acc = Account.query.filter_by(processor_id=old_proc.id).all()
        assert len(new_acc) == 0
        form_sources = [{'key': x} for x in range(3)]
        app_utl.set_db_values(old_proc.id, user.id, form_sources,
                              table=Account)
        new_acc = Account.query.filter_by(processor_id=old_proc.id).all()
        assert len(new_acc) == len(form_sources)
        form_sources = [x.get_form_dict() for x in new_acc[:2]]
        app_utl.set_db_values(old_proc.id, user.id, form_sources,
                              table=Account)
        new_acc = Account.query.filter_by(processor_id=old_proc.id).all()
        assert len(new_acc) == len(form_sources)
        new_key = 'new_key'
        changed_id = form_sources[0]['id']
        form_sources[0]['key'] = new_key
        app_utl.set_db_values(old_proc.id, user.id, form_sources,
                              table=Account)
        new_acc = Account.query.filter_by(processor_id=old_proc.id).all()
        assert len(new_acc) == len(form_sources)
        change_acc = db.session.get(Account, changed_id)
        assert change_acc.key == new_key

    def test_update_base_plan(self, user, app_fixture):
        ven_name = 'Test'
        max_cou = 'A'
        ven_col = prc_model.Vendor.vendorname.name
        cou_col = prc_model.Country.countryname.name
        data = {
            cou_col: [max_cou, max_cou, 'B', 'B'],
            ven_col: [ven_name] * 4,
            vmc.impressions: [10, 10, 5, 5]
        }
        df = pd.DataFrame(data)
        app_tasks.update_base_plan(ven_name, df, user.id)
        plan_name = 'Base Plan'
        cur_plan = Plan.query.filter_by(name=plan_name).first()
        assert cur_plan
        cur_place = cur_plan.get_placements()
        assert cur_place
        assert cur_place[0].country == max_cou

    def test_get_last_rate_card_for_client(self, user, app_fixture):
        old_proc = self.create_test_processor()
        rate_card = RateCard(name='test')
        db.session.add(rate_card)
        db.session.commit()
        old_proc.rate_card_id = rate_card.id
        db.session.commit()
        c_id, p_id = app_tasks.get_last_rate_card_for_client(old_proc.campaign)
        assert c_id == rate_card.id
        assert p_id == old_proc.id


class TestPlan:

    @staticmethod
    def check_plan(data, rule_info, cur_part, total_budget, place_col):
        assert len(data) == len(rule_info)
        places = PartnerPlacements.query.filter_by(partner_id=cur_part.id).all()
        assert len(places) == len(data)
        place_budget = 0
        for place in places:
            place_budget += place.total_budget
            cur_country = place.__dict__[place_col]
            assert cur_country in rule_info
            assert place.total_budget == rule_info[cur_country] * total_budget
        assert place_budget == total_budget

    def test_create_from_rules(self, user, app_fixture):
        cur_plan = TestTasks.create_test_processor(Plan)
        cur_phase = PlanPhase(name=cur_plan.name, plan_id=cur_plan.id)
        db.session.add(cur_phase)
        db.session.commit()
        total_budget = 100
        cur_part = Partner(name=cur_plan.name, plan_phase_id=cur_phase.id,
                           total_budget=total_budget)
        db.session.add(cur_part)
        db.session.commit()
        place_col = PartnerPlacements.country.name
        first_val = 'a'
        rule_info = {first_val: .5, 'b': .5}
        plan_rule = PlanRule(
            type='Create', plan_id=cur_plan.id, partner_id=cur_part.id,
            rule_info=rule_info, place_col=place_col)
        db.session.add(plan_rule)
        db.session.commit()
        data = PartnerPlacements().create_from_rules(parent_id=cur_part.id,
                                                     current_user_id=user.id)
        self.check_plan(data, rule_info, cur_part, total_budget, place_col)
        place = PartnerPlacements.query.filter_by(
            **{place_col: first_val}).first()
        new_val = 'c'
        update_rule_info = {'id': place.id, 'val': new_val}
        plan_rule = PlanRule(
            type='update', plan_id=cur_plan.id, partner_id=cur_part.id,
            rule_info=update_rule_info, place_col=place_col)
        db.session.add(plan_rule)
        db.session.commit()
        data = PartnerPlacements().create_from_rules(parent_id=cur_part.id,
                                                     current_user_id=user.id)
        rule_info[new_val] = rule_info.pop(first_val)
        self.check_plan(data, rule_info, cur_part, total_budget, place_col)

    def test_write_plan_placements(self, user, app_fixture,
                                   check_for_plan=False):
        cur_plan = TestTasks.create_test_processor(Plan)
        df = Plan.get_mock_plan(basedir, check_for_plan)
        file_type = '.csv'
        file_name = 'mediaplantmp{}'.format(file_type)
        df.to_csv(file_name)
        app_tasks.write_plan_placements(
            cur_plan.id, user.id, new_data=file_name, file_type=file_type)
        cur_places = cur_plan.get_placements()
        assert len(cur_places) == len(df)
        pdf = cur_plan.get_placements_as_df()
        pdf = PartnerPlacements.translate_plan_names(pdf)
        df = PartnerPlacements.translate_plan_names(df)
        cost_col = PartnerPlacements.total_budget.name
        assert float(pdf[cost_col].sum()) == float(df[cost_col].sum())
        col_order = PartnerPlacements.get_col_order()
        assert len(col_order) == len(cur_places[0].name.split('_'))
        os.remove(file_name)
        return cur_plan, df

    def test_write_plan_placements_local(self, user, app_fixture):
        self.test_write_plan_placements(
            user, app_fixture, check_for_plan=True)
