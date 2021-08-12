from app import db
import app.utils as utl
from flask_babel import _
from app.tutorials import bp
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, flash, request, jsonify
from app.tutorials.forms import TutorialUploadForm
from app.models import Tutorial, TutorialStage


@bp.route('/tutorial/edit', methods=['GET', 'POST'])
@login_required
def edit_tutorial():
    form = TutorialUploadForm()
    kwargs = {'form': form}
    return render_template('tutorials/tutorials.html', **kwargs)


@bp.route('/tutorial/edit/upload_file', methods=['GET', 'POST'])
@login_required
def edit_tutorial_upload_file():
    current_key, object_name, object_form, object_level =\
        utl.parse_upload_file_request(request)
    tutorial_name = [x['value'] for x in object_form if x['name'] ==
                     'tutorial_select'][0]
    mem, file_name = utl.get_file_in_memory_from_request(request, current_key)
    msg_text = 'Updating tutorial for {}'.format(tutorial_name)
    current_user.launch_task(
        '.update_tutorial', _(msg_text),
        running_user=current_user.id, tutorial_name=tutorial_name, new_data=mem)
    db.session.commit()
    return jsonify({'data': 'success: {}'.format(tutorial_name)})


@bp.route('/tutorial/get/<tutorial_name>/<tutorial_level>',
          methods=['GET', 'POST'])
@login_required
def get_tutorial(tutorial_name, tutorial_level=0):
    cur_tutorial = Tutorial.query.filter_by(name=tutorial_name).first_or_404()
    tutorial_stage = TutorialStage.query.filter_by(
        tutorial_id=cur_tutorial.id, tutorial_level=tutorial_level).first_or_404()
    kwargs = {'object_name': cur_tutorial.name,
              'title': 'Tutorial',
              'tutorial_stage': tutorial_stage}
    return render_template('tutorials/tutorials.html', **kwargs)
