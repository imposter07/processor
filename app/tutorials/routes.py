import math
from app import db
import app.utils as utl
from flask_babel import _
from app.tutorials import bp
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, request, jsonify
from app.tutorials.forms import TutorialUploadForm, TutorialContinueForm
from app.models import Tutorial, TutorialStage, User


@bp.route('/tutorial', methods=['GET', 'POST'])
@login_required
def tutorial():
    form_description = """
    Users ranked by tutorial completion.  If you feel you deserve to be ranked
    higher, there's a chance doing more tutorials will help.
    """
    u = User.query.all()
    t = Tutorial.query.all()
    total_stars = 5
    star_dict = {x + 1: [] for x in reversed(range(total_stars))}
    pro_den = ((len(t) * 100) / (total_stars - 1))
    for cu in u:
        if cu.password_hash:
            progress = 0
            for ct in t:
                progress += ct.get_progress(cu.id)
            idx = math.ceil(progress / pro_den) + 1
            user_dict = {'user': cu, 'progress': progress}
            star_dict[idx].append(user_dict)
    kwargs = dict(tutorial_progress=star_dict, title='Tutorial',
                  object_name='User Progress', form_title='TUTORIAL PROGRESS',
                  form_description=form_description, tutorials=t,
                  user=current_user)
    return render_template('tutorials/tutorials.html', **kwargs)


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
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
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
    all_tutorials = Tutorial.query.all()
    cur_tutorial = Tutorial.query.filter_by(name=tutorial_name).first_or_404()
    tutorial_stage = TutorialStage.query.filter_by(
        tutorial_id=cur_tutorial.id,
        tutorial_level=tutorial_level).first_or_404()
    form = TutorialContinueForm()
    tutorial_level = int(tutorial_level)
    tutorial_stage_num = len(cur_tutorial.tutorial_stage.all())
    edit_progress = ((tutorial_level + 1) / (tutorial_stage_num + 1)) * 100
    buttons = [x for x in range(tutorial_level - 2, tutorial_level + 3)
               if 0 < x < tutorial_stage_num]
    buttons = list(set([0] + buttons + [tutorial_stage_num]))
    if 1 not in buttons:
        buttons[1:1] = ['...']
    if tutorial_stage_num - 1 not in buttons:
        buttons[-1:-1] = ['...']
    buttons = [{'lvl {}'.format(x): 'tutorials.get_tutorial'} for x in buttons]
    kwargs = dict(object_name=cur_tutorial.name, title='Tutorial',
                  tutorial_stage=tutorial_stage, tutorials=all_tutorials,
                  edit_name='lvl {}'.format(tutorial_level), user=current_user,
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
