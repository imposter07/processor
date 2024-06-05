import json
from app import db
from flask_babel import _
from app.plan import bp
from datetime import datetime
from werkzeug.datastructures import MultiDict
import datetime as dt
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, request, jsonify, flash
from app.plan.forms import PlanForm, EditPlanForm, PlanToplineForm, \
    CreateSowForm, RfpForm, PartnerPlacementForm, CompetitiveSpendForm, \
    PlotCategoryForm, BrandtrackerForm, CategoryComponentForm, ImpactScoreForm
from app.main.forms import FeeForm
from app.models import Client, Product, Campaign, Plan, Post, \
    PlanPhase, Sow, Processor, Brandtracker, BrandtrackerDimensions
import app.utils as app_utl


@bp.route('/plan', methods=['GET', 'POST'])
@login_required
def plan():
    kwargs = Plan().get_current_plan(current_page='create_plan',
                                     edit_progress=100, edit_name='Basic',
                                     buttons='Plan')
    form = PlanForm()
    form.set_choices()
    kwargs['form'] = form
    if request.method == 'POST':
        form.validate()
        new_plan = Plan.new_plan_from_form(form)
        db.session.add(new_plan)
        db.session.commit()
        creation_text = 'Plan was requested for creation.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    plan_id=new_plan.id)
        db.session.add(post)
        db.session.commit()
        if not new_plan.phases.all():
            phase = PlanPhase(
                name='Launch', plan_id=new_plan.id,
                start_date=new_plan.start_date,
                end_date=new_plan.end_date)
            db.session.add(phase)
            db.session.commit()
        if form.form_continue.data == 'continue':
            route_str = 'plan.topline'
        else:
            route_str = 'plan.edit_plan'
        return redirect(url_for(route_str, object_name=new_plan.name))
    return render_template('plan/plan.html', **kwargs)


@bp.route('/create_plan_from_processor', methods=['GET', 'POST'])
@login_required
def create_plan_from_processor():
    cur_processor = Processor.query.get(request.form['processor_id'])
    cur_plans = cur_processor.plans.all()
    if cur_plans:
        cur_plan = cur_plans[0]
        plan_url = url_for('plan.edit_plan', object_name=cur_plan.name)
    else:
        new_plan = Plan(
            name=cur_processor.name, description=cur_processor.description,
            start_date=cur_processor.start_date,
            end_date=cur_processor.end_date,
            user_id=current_user.id, created_at=datetime.utcnow(),
            campaign_id=cur_processor.campaign.id)
        db.session.add(new_plan)
        db.session.commit()
        cur_processor.plans.append(new_plan)
        db.session.commit()
        plan_url = url_for('plan.edit_plan', object_name=new_plan.name)
    return jsonify({'url': plan_url})


@bp.route('/plan/<object_name>', methods=['GET', 'POST'])
@login_required
def edit_plan(object_name):
    kwargs = Plan().get_current_plan(object_name, 'edit_plan',
                                     edit_progress=100,
                                     edit_name='Basic')
    current_plan = kwargs['object']
    form = EditPlanForm(original_name=current_plan.name)
    form.set_choices()
    if request.method == 'POST':
        form.validate()
        form_campaign = current_plan.get_parent_model_from_form(form)
        current_plan.set_from_form(form.data, form_campaign, current_user.id)
        db.session.commit()
        creation_text = 'Plan was edited.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    plan_id=current_plan.id)
        db.session.add(post)
        db.session.commit()
        if not current_plan.phases.all():
            phase = PlanPhase(
                name='Launch', plan_id=current_plan.id,
                start_date=current_plan.start_date,
                end_date=current_plan.end_date)
            db.session.add(phase)
            db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('plan.topline',
                                    object_name=current_plan.name))
        else:
            return redirect(url_for('plan.edit_plan',
                                    object_name=current_plan.name))
    elif request.method == 'GET':
        form.set_form(current_plan)
    kwargs['form'] = form
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/topline', methods=['GET', 'POST'])
@login_required
def topline(object_name):
    kwargs = Plan().get_current_plan(object_name, 'edit_plan',
                                     edit_progress=100,
                                     edit_name='Topline')
    kwargs['form'] = PlanToplineForm()
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/sow', methods=['GET', 'POST'])
@login_required
def edit_sow(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_sow', edit_progress=75, edit_name='SOW')
    form = CreateSowForm()
    cur_plan = kwargs['object']
    kwargs['form'] = form
    current_sow = Sow.query.filter_by(plan_id=cur_plan.id).first()
    if not current_sow:
        current_sow = Sow()
        current_sow.create_from_plan(cur_plan)
        db.session.add(current_sow)
        db.session.commit()
    if request.method == 'POST':
        form.validate()
        if not current_sow:
            current_sow = Sow()
            current_sow.create_from_plan(cur_plan)
            db.session.add(current_sow)
            db.session.commit()
        current_sow.project_name = form.project_name.data
        current_sow.project_contact = form.project_contact.data
        current_sow.date_submitted = form.date_submitted.data
        current_sow.liquid_contact = form.liquid_contact.data
        current_sow.liquid_project = form.liquid_project.data
        current_sow.start_date = form.start_date.data
        current_sow.end_date = form.end_date.data
        current_sow.client_name = form.client_name.data
        current_sow.address = form.address.data
        current_sow.phone = form.phone.data
        current_sow.fax = form.fax.data
        current_sow.ad_serving = form.ad_serving.data
        db.session.commit()
        creation_text = 'SOW was edited.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    plan_id=cur_plan.id)
        db.session.add(post)
        db.session.commit()
    elif request.method == 'GET':
        form.project_name.data = current_sow.project_name
        form.project_contact.data = current_sow.project_contact
        form.date_submitted.data = current_sow.date_submitted
        form.liquid_contact.data = current_sow.liquid_contact
        form.liquid_project.data = current_sow.liquid_project
        form.start_date.data = current_sow.start_date
        form.end_date.data = current_sow.end_date
        form.client_name.data = current_sow.client_name
        form.address.data = current_sow.address
        form.phone.data = current_sow.phone
        form.fax.data = current_sow.fax
        form.ad_serving.data = current_sow.ad_serving
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/plan_rules', methods=['GET', 'POST'])
@login_required
def plan_rules(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100, edit_name='PlanRules')
    kwargs['form'] = PlanToplineForm()
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/plan_placements/upload_file',
          methods=['GET', 'POST'])
@login_required
@app_utl.error_handler
def plan_placements_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        app_utl.parse_upload_file_request(request, object_name)
    cur_plan = Plan.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        app_utl.get_file_in_memory_from_request(request, current_key)
    msg_text = 'Adding placements for {}'.format(cur_plan.name)
    cur_plan.launch_task(
        '.write_plan_placements', _(msg_text),
        running_user=current_user.id, new_data=mem, file_type=file_type)
    db.session.commit()
    msg = 'File was saved.'
    return jsonify({'data': 'success', 'message': msg, 'level': 'success',
                    'table': 'PlanPlacements'})


@bp.route('/plan/<object_name>/plan_placements', methods=['GET', 'POST'])
@login_required
def plan_placements(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100, edit_name='PlanPlacements')
    kwargs['form'] = PartnerPlacementForm()
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/rfp', methods=['GET', 'POST'])
@login_required
def rfp(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100, edit_name='RFP')
    kwargs['form'] = RfpForm()
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/rfp/upload_file', methods=['GET', 'POST'])
@login_required
@app_utl.error_handler
def rfp_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        app_utl.parse_upload_file_request(request, object_name)
    cur_plan = Plan.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        app_utl.get_file_in_memory_from_request(request, current_key)
    msg_text = 'Adding RFP for {}'.format(cur_plan.name)
    cur_plan.launch_task(
        '.add_rfp_from_file', _(msg_text),
        running_user=current_user.id, new_data=mem)
    db.session.commit()
    msg = 'File was saved.'
    return jsonify({'data': 'success', 'message': msg, 'level': 'success'})


@bp.route('/plan/<object_name>/specs', methods=['GET', 'POST'])
@login_required
def specs(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100,
        edit_name='Specs')
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/contacts', methods=['GET', 'POST'])
@login_required
def contacts(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100,
        edit_name='Contacts')
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/calc', methods=['GET', 'POST'])
@login_required
def calc(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100,
        edit_name='Calc')
    kwargs['form'] = PlanToplineForm()
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/checklist', methods=['GET', 'POST'])
@login_required
def checklist(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100,
        edit_name='Checklist')
    kwargs['form'] = PlanToplineForm()
    return render_template('plan/plan.html', **kwargs)


@bp.route('/plan/<object_name>/fees', methods=['GET', 'POST'])
@login_required
def fees(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100,
        edit_name='Fees')
    kwargs['form'] = FeeForm()
    response = app_utl.obj_fees_route(
        object_name, current_user, object_type=Plan, kwargs=kwargs,
        template='plan/plan.html')
    return response


@bp.route('/plan/<object_name>/url_from_view_function', methods=['GET', 'POST'])
@login_required
@app_utl.error_handler
def url_from_view_function(object_name):
    view_function = request.form['object_name']
    url = url_for(str(view_function), object_name=object_name)
    return jsonify({'url': url})


@bp.route('/research', methods=['GET', 'POST'])
@login_required
def research():
    kwargs = Plan().get_current_plan(
        current_page='create_research', edit_progress=100, edit_name='Basic',
        buttons='Research')
    form = PlanForm()
    form.set_choices()
    kwargs['form'] = form
    if request.method == 'POST':
        form.validate()
        new_plan = Plan.new_plan_from_form(form)
        db.session.add(new_plan)
        db.session.commit()
        creation_text = 'Plan was requested for creation.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    plan_id=new_plan.id)
        db.session.add(post)
        db.session.commit()
        if not new_plan.phases.all():
            phase = PlanPhase(
                name='Launch', plan_id=new_plan.id,
                start_date=new_plan.start_date,
                end_date=new_plan.end_date)
            db.session.add(phase)
            db.session.commit()
        if form.form_continue.data == 'continue':
            route_str = 'plan.edit_competitive_spend'
        else:
            route_str = 'plan.research'
        return redirect(url_for(route_str, object_name=new_plan.name))
    return render_template('plan/plan.html', **kwargs)


@bp.route('/research/<object_name>', methods=['GET', 'POST'])
@login_required
def edit_research(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_research', edit_progress=100, edit_name='Basic',
        buttons='Research')
    current_plan = kwargs['object']
    form = EditPlanForm(original_name=current_plan.name)
    form.set_choices()
    if request.method == 'POST':
        form.validate()
        form_campaign = current_plan.get_parent_model_from_form(form)
        current_plan.set_from_form(form.data, form_campaign, current_user.id)
        db.session.commit()
        creation_text = 'Plan was edited.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    plan_id=current_plan.id)
        db.session.add(post)
        db.session.commit()
        if not current_plan.phases.all():
            phase = PlanPhase(
                name='Launch', plan_id=current_plan.id,
                start_date=current_plan.start_date,
                end_date=current_plan.end_date)
            db.session.add(phase)
            db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('plan.edit_competitive_spend',
                                    object_name=current_plan.name))
        else:
            return redirect(url_for('plan.edit_research',
                                    object_name=current_plan.name))
    elif request.method == 'GET':
        form.set_form(current_plan)
    kwargs['form'] = form
    return render_template('plan/plan.html', **kwargs)


@bp.route('/research/competitive_spend', methods=['GET', 'POST'])
@login_required
def competitive_spend():
    kwargs = Plan().get_current_plan(
        current_page='create_research', edit_progress=100,
        edit_name='CompetitiveSpend', buttons='Research')
    temp_proc = Processor()
    temp_proc.tableau_view = 'CompetitveSpend'
    temp_proc.tableau_workbook = 'RefreshedTableau'
    kwargs['processor'] = temp_proc
    kwargs['tableau_workbook'] = True
    return render_template('plan/plan.html', **kwargs)


@bp.route('/research/<object_name>/competitive_spend', methods=['GET', 'POST'])
@login_required
def edit_competitive_spend(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_competitive_spend', edit_progress=100,
        edit_name='CompetitiveSpend', buttons='Research')
    temp_proc = Processor()
    temp_proc.tableau_view = 'CompetitveSpend'
    temp_proc.tableau_workbook = 'RefreshedTableau'
    kwargs['processor'] = temp_proc
    kwargs['tableau_workbook'] = True
    form = CompetitiveSpendForm()
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('plan.edit_brandtracker',
                                    object_name=object_name))
        else:
            return redirect(url_for('plan.edit_competitive_spend',
                                    object_name=object_name))
    kwargs['form'] = form
    return render_template('plan/plan.html', **kwargs)


@bp.route('/research/<object_name>/brandtracker', methods=['GET', 'POST'])
@login_required
def edit_brandtracker(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_brandtracker', edit_progress=100,
        edit_name='Brandtracker', buttons='Research')
    form = BrandtrackerForm()
    form.set_title_choices()
    current_plan = kwargs['object']
    cur_bt = Brandtracker.query.filter_by(plan_id=current_plan.id).first()
    if cur_bt:
        form.primary_date.data = cur_bt.primary_date
        form.comparison_date.data = cur_bt.comparison_date
        form.titles.data = cur_bt.titles.split('|')
    else:
        cur_bt = Brandtracker.query.filter_by(name='Base Brandtracker').first()
        form.primary_date.data = dt.date.today()
        form.comparison_date.data = (dt.date.today()
                                     - dt.timedelta(days=30))
    kwargs['form'] = form
    kwargs['brandtrackers'] = cur_bt
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
        kwargs[form_id] = new_form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('plan.edit_impact_score',
                                    object_name=current_plan.name))
        else:
            return redirect(url_for('plan.edit_brandtracker',
                                    object_name=current_plan.name))
    return render_template('plan/plan.html', **kwargs)


@bp.route('/save_brandtracker', methods=['GET', 'POST'])
@login_required
def save_brandtracker():
    plan_name = request.form['object_name'].strip()
    cur_plan = Plan.query.filter_by(name=plan_name).first_or_404()
    cur_bt = Brandtracker.query.filter_by(plan_id=cur_plan.id).first()
    if not cur_bt:
        cur_bt = Brandtracker(plan_id=cur_plan.id, user_id=current_user.id)
        db.session.add(cur_bt)
    base_form_data = json.loads(request.form['base_form_id'])
    base_form = BrandtrackerForm(MultiDict(base_form_data))
    cur_bt.titles = '|'.join(base_form.titles.data)
    cur_bt.primary_date = base_form.primary_date.data
    cur_bt.comparison_date = base_form.comparison_date.data
    for dimension in [x[0] for x in base_form.categories.choices if x[0]]:
        form_id = '{}Form'.format(dimension)
        form_data = json.loads(request.form[form_id])
        form = PlotCategoryForm(MultiDict(form_data))
        components = form.components.data
        current_components = cur_bt.get_dimension_form_dicts()
        new_comps = []
        for comp_form in components:
            comp_form['dimension_name'] = dimension
            comp = BrandtrackerDimensions()
            comp.set_from_form(comp_form, cur_bt)
            new_comp = comp.get_form_dict()
            new_comps.append(new_comp)
            if new_comp not in current_components:
                db.session.add(comp)
        for old_comp in cur_bt.dimensions.filter_by(
                dimension=dimension).all():
            if old_comp.get_form_dict() not in new_comps:
                db.session.delete(old_comp)
    db.session.commit()
    return jsonify({
        'message': 'This brand tracker for plan {} has been saved '
                   'successfully'.format(cur_plan.name),
        'level': 'success'})


@bp.route('/add_brandtracker_component', methods=['GET', 'POST'])
@login_required
def add_brandtracker_componenet():
    orig_form = PlotCategoryForm(request.form)
    kwargs = orig_form.data
    new_component = CategoryComponentForm(formdata=None)
    kwargs['components'].insert(0, new_component.data)
    form = PlotCategoryForm(formdata=None, **kwargs)
    form.set_column_choices()
    form_id = '{}Form'.format(kwargs['dimension_name'])
    new_form = render_template('_form.html', form=form,
                               form_id=form_id)
    return jsonify({'form': new_form, 'form_id': form_id})


@bp.route('/delete_brandtracker_component', methods=['GET', 'POST'])
@login_required
def delete_brandtracker_component():
    delete_id = int(request.args.get('delete_id').split('-')[1])
    orig_form = PlotCategoryForm(request.form)
    kwargs = orig_form.data
    del kwargs['components'][delete_id]
    form = PlotCategoryForm(formdata=None, **kwargs)
    form.set_column_choices()
    form_id = '{}Form'.format(kwargs['dimension_name'])
    new_form = render_template('_form.html', form=form,
                               form_id=form_id)
    return jsonify({'form': new_form, 'form_id': form_id})


@bp.route('/research/<object_name>/impact_score', methods=['GET', 'POST'])
@login_required
def edit_impact_score(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_impact_score', edit_progress=100,
        edit_name='ImpactScore', buttons='Research')
    form = ImpactScoreForm()
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('plan.edit_impact_score',
                                    object_name=object_name))
        else:
            return redirect(url_for('plan.edit_impact_score',
                                    object_name=object_name))
    kwargs['form'] = form
    return render_template('plan/plan.html', **kwargs)
