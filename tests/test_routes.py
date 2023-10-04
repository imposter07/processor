import os
import pytest
import datetime as dt
import processor.reporting.analyze as az
from app import db
from app.models import Conversation, Plan, PlanPhase, User, Partner


def test_index(client, user):
    response = client.get('/')
    assert response.status_code == 200
    assert 'Hi, {}'.format(user.username) in response.text


def test_health_check(client):
    expected_output = {'data': 'success'}
    response = client.get('/health_check')
    assert response.json == expected_output


class TestChat:

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

    def test_plan_create(self, client, conversation):
        if os.path.exists(os.path.basename(__file__)):
            os.chdir("..")
        prompt_dict = Plan.get_large_create_prompt(prompt_dict=True)
        msg = prompt_dict['message']
        data = {'conversation_id': conversation.id, 'message': msg}
        response = client.post('/post_chat', data=data)
        success_msg = az.AliChat.create_success_msg
        assert response.status_code == 200
        assert response.json['response'][:len(success_msg)] == success_msg
        cu = db.session.get(User, conversation.user_id)
        p = Plan.query.filter_by(name=cu.username).first()
        phase = p.get_current_children()[0]
        assert phase.name == PlanPhase.get_name_list()[0]
        part = phase.get_current_children()[0]
        assert part.name == prompt_dict[Partner.__table__.name]
        total_budget = prompt_dict[Partner.total_budget.name]
        total_budget = int(total_budget.lower().replace('k', '000'))
        assert int(part.total_budget) == total_budget
