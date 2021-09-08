from app import db
import app.utils as utl
from flask_babel import _
from app.tutorials import bp
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, flash, request, jsonify
from app.tutorials.forms import TutorialUploadForm, TutorialContinueForm
from app.models import Tutorial, TutorialStage, user_tutorial, User


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
        tutorial_id=cur_tutorial.id,
        tutorial_level=tutorial_level).first_or_404()
    form = TutorialContinueForm()
    tutorial_level = int(tutorial_level)
    tutorial_stage_num = len(cur_tutorial.tutorial_stage.all())
    edit_progress = ((tutorial_level + 1) / (tutorial_stage_num + 1)) * 100
    buttons = [
        {'lvl {}'.format(x): 'tutorials.get_tutorial'}
        for x in range(tutorial_stage_num)]
    kwargs = dict(object_name=cur_tutorial.name, title='Tutorial',
                  tutorial_stage=tutorial_stage,
                  edit_name='lvl {}'.format(tutorial_level),
                  form=form, edit_progress=edit_progress, buttons=buttons,
                  object_function_call={'tutorial_name': tutorial_name,
                                        'tutorial_level': tutorial_level})
    if request.method == 'POST':
        cur_user = User.query.filter_by(id=current_user.id).first_or_404()
        cur_user.complete_tutorial_stage(tutorial_stage)
        db.session.commit()
        if form.form_continue.data == 'continue':
            if tutorial_level + 1 >= tutorial_stage_num:
                return redirect(url_for('main.user',
                                        username=current_user.username))
            else:
                return redirect(url_for('tutorials.get_tutorial',
                                        tutorial_name=tutorial_name,
                                        tutorial_level=tutorial_level+1))
        else:
            return redirect(url_for('tutorials.get_tutorial',
                                    tutorial_name=tutorial_name,
                                    tutorial_level=tutorial_level))
    return render_template('tutorials/tutorials.html', **kwargs)
