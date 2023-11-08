import os
import pytest
import pandas as pd
import datetime as dt
import processor.reporting.utils as utl
import processor.reporting.analyze as az
import app.utils as app_utl
from app import db
from app.models import Conversation, Plan, PlanPhase, User, Partner, Task, \
    Chat, Uploader, Project, PartnerPlacements, Campaign, PlanRule, Client, \
    Product


def test_index(client, user):
    response = client.get('/')
    assert response.status_code == 200
    assert 'Hi, {}'.format(user.username) in response.text


def test_health_check(client):
    expected_output = {'data': 'success'}
    response = client.get('/health_check')
    assert response.json == expected_output


class TestChat:

    @pytest.fixture(scope='class', autouse=True)
    def check_directory(self):
        if os.path.exists(os.path.basename(__file__)):
            os.chdir("..")

    @pytest.fixture(scope='class')
    def conversation(self, user, app_fixture):
        message = 'test message'
        conv = Conversation(name=message, user_id=user.id,
                            created_at=dt.datetime.utcnow())
        db.session.add(conv)
        db.session.commit()
        yield conv
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
        new_env = 'mobile'
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

    def test_parse_filter_dict_from_clients(self, user, app_fixture):
        name = Client.get_default_name()[0]
        cli = Client(name=name).check_and_add()
        pro = Product(name=name, client_id=cli.id).check_and_add()
        cam = Campaign(name=name, product_id=pro.id).check_and_add()
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
