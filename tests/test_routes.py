import pytest
import datetime as dt
from app import db
from app.models import Conversation


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
