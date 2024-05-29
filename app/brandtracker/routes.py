import io
import json
from app import db
from flask_babel import _
from app.brandtracker import bp
import datetime as dt
from werkzeug.datastructures import MultiDict
from flask_login import current_user, login_required
from flask import render_template, request, jsonify, send_file
from app.brandtracker.forms import PlotCategoryForm, CategoryComponentForm, \
    BrandtrackerForm
from app.models import Brandtracker, BrandtrackerDimensions


@bp.route('/brandtracker', methods=['GET', 'POST'])
@login_required
def brandtracker():
    form = BrandtrackerForm()
    form.set_title_choices()
    brandtrackers = Brandtracker.query.filter_by(user_id=current_user.id).all()
    kwargs = {'form': form, 'brandtrackers': brandtrackers}
    return render_template('brandtracker/brandtracker.html', **kwargs)


@bp.route('/get_brandtracker', methods=['GET', 'POST'])
@login_required
def get_brandtracker():
    brandtracker_id = request.form['brandtracker_id']
    default_date = False
    if brandtracker_id == 'undefined':
        default_date = True
        cur_bt = Brandtracker.query.filter_by(name='Base Brandtracker').first()
    else:
        cur_bt = Brandtracker.query.filter_by(id=int(brandtracker_id)).first()
    form_kwargs = {}
    if cur_bt:
        form_kwargs = cur_bt.to_dict()
        if cur_bt.titles:
            form_kwargs['titles'] = cur_bt.titles.split('|')
    if default_date:
        form_kwargs['primary_date'] = dt.date.today()
        form_kwargs['comparison_date'] = (dt.date.today()
                                          - dt.timedelta(days=30))
    form = BrandtrackerForm(formdata=None, **form_kwargs)
    form.set_title_choices()
    new_form = render_template('_form.html', form=form)
    response = {'base_form_id': new_form}
    for dimension in [x[0] for x in form.categories.choices if x[0]]:
        components = []
        if cur_bt:
            components = BrandtrackerDimensions.query.filter_by(
                brandtracker_id=cur_bt.id, dimension=dimension).all()
        comp_dicts = [
            {'data_column': x.metric_column, 'weight': float(x.weight)}
            for x in components
        ]
        form = PlotCategoryForm(formdata=None, components=comp_dicts,
                                dimension_name=dimension)
        form.set_column_choices()
        form_id = '{}Form'.format(dimension)
        new_form = render_template('_form.html', form=form,
                                   form_id=form_id)
        response[form_id] = new_form
    return jsonify(response)


@bp.route('/get_brandtracker_tables', methods=['GET', 'POST'])
@login_required
def get_brandtracker_tables():
    download = (json.loads(request.form['download'])
                if 'download' in request.form else False)
    base_form_data = json.loads(request.form['base_form_id'])
    base_form = BrandtrackerForm(MultiDict(base_form_data))
    form_data = {'titles': base_form.titles.data,
                 'primary_date': base_form.primary_date.data,
                 'comparison_date': base_form.comparison_date.data}
    for dimension in [x[0] for x in base_form.categories.choices if x[0]]:
        form_id = '{}Form'.format(dimension)
        dimension_data = json.loads(request.form[form_id])
        form = PlotCategoryForm(MultiDict(dimension_data))
        form_data[dimension] = form.components.data
    if current_user.get_task_in_progress('.get_brandtracker_data'):
        message = 'Brand tracker calculations are already being refreshed.'
        data = {'data': 'fail', 'task': '', 'message': message, 'level': 'warning'}
        return jsonify(data)
    msg_text = 'Retrieving brand tracker data'
    task = current_user.launch_task(
        '.get_brandtracker_data', _(msg_text),
        running_user=current_user.id, form_data=form_data)
    db.session.commit()
    job = task.wait_and_get_job()
    if job and job.result and len(job.result) > 1:
        if download:
            df = job.result[0]
            mem = io.BytesIO()
            df.to_csv(mem, encoding='utf-8', index=False)
            mem.seek(0)
            return send_file(mem, as_attachment=True,
                             mimetype='text/csv', download_name='raw.csv')
        data = {
            'ChartData': job.result[0].to_dict(orient='records'),
            'InfluenceData': job.result[1],
            'EngagementData': job.result[2],
            'MomentumData': job.result[3]
        }
    else:
        data = {'level': 'danger', 'message': 'An unexpected error occurred'}
    return jsonify(data)
