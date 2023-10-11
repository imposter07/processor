import os
import pytest
import pandas as pd
import datetime as dt
import processor.reporting.utils as utl
import processor.reporting.analyze as az
from app import db
from app.models import Conversation, Plan, PlanPhase, User, Partner, Task, \
    Chat, Uploader, Project, PartnerPlacements, Campaign


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
    def verify_plan_create(conversation_id, prompt_dict):
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
        for x in range(10):
            t = Task.query.filter_by(plan_id=p.id, complete=False).first()
            if t:
                t.wait_and_get_job(loops=10)
            else:
                break
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

    def test_plan_create(self, client, conversation, worker, prompt_dict=None):
        if not prompt_dict:
            prompt_dict = Plan.get_large_create_prompt(prompt_dict=True)
        msg = prompt_dict['message']
        data = {Chat.conversation_id.name: conversation.id, 'message': msg}
        response = client.post('/post_chat', data=data)
        success_msg = az.AliChat.create_success_msg
        assert response.status_code == 200
        assert response.json['response'][:len(success_msg)] == success_msg
        worker.work(burst=True)
        self.verify_plan_create(conversation.user_id, prompt_dict)

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
        for prompt in prompts:
            msg = prompt
            if 'partner_name' in prompt:
                msg = msg.replace('partner_name', partner_name)
                msg = msg.replace('new_budget', str(new_budget))
            data = {Chat.conversation_id.name: conversation.id, 'message': msg}
            if Uploader.__table__.name in msg:
                continue
            client.post('/post_chat', data=data)
        part = p.get_current_children()[0].get_current_children()[0]
        assert int(part.total_budget) == int(new_budget)

    def test_create_project(self, client, conversation, worker):
        pn = '12345'
        msg = 'Create a {} with {} {}'.format(
            Project.__table__.name, Project.project_number.name, pn)
        data = {Chat.conversation_id.name: conversation.id, 'message': msg}
        response = client.post('/post_chat', data=data)
        success_msg = az.AliChat.create_success_msg
        assert response.status_code == 200
        assert response.json['response'][:len(success_msg)] == success_msg
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
        self.test_plan_create(client, conversation, worker, prompt_dict)
        plan = Plan.query.filter_by(name=p.name).first()
        assert p.campaign_id == plan.campaign_id
