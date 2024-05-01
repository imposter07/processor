from app import db
from flask_babel import _
from app.plan import bp
from datetime import datetime
import datetime as dt
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, request, jsonify, flash
from app.plan.forms import PlanForm, EditPlanForm, PlanToplineForm, \
    CreateSowForm, RfpForm, PartnerPlacementForm, CompetitiveSpendForm
from app.brandtracker.forms import BrandtrackerForm
from app.main.forms import FeeForm
from app.models import Client, Product, Campaign, Plan, Post, \
    PlanPhase, Sow, Processor, Brandtracker
import app.utils as app_utl


def set_plan_form(form):
    form.validate()
    form_client = Client(name=form.cur_client.data).check_and_add()
    form_product = Product(
        name=form.cur_product.data,
        client_id=form_client.id).check_and_add()
    form_campaign = Campaign(
        name=form.cur_campaign.data,
        product_id=form_product.id).check_and_add()
    sd = form.start_date.data
    sd = sd if sd else datetime.today()
    ed = form.end_date.data
    ed = ed if ed else datetime.today() + dt.timedelta(days=7)
    new_plan = Plan(
        name=form.name.data, description=form.description.data,
        client_requests=form.client_requests.data,
        restrictions=form.restrictions.data, objective=form.objective.data,
        start_date=sd, end_date=ed, total_budget=form.total_budget.data,
        campaign_id=form_campaign.id)
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
    return new_plan


def edit_plan(current_plan, form):
    form.validate()
    form_client = Client(name=form.cur_client.data).check_and_add()
    form_product = Product(name=form.cur_product.data,
                           client_id=form_client.id).check_and_add()
    form_campaign = Campaign(name=form.cur_campaign.data,
                             product_id=form_product.id).check_and_add()
    current_plan.name = form.name.data
    current_plan.client_requests = form.client_requests.data
    current_plan.restrictions = form.restrictions.data
    current_plan.objective = form.objective.data
    current_plan.start_date = form.start_date.data
    current_plan.end_date = form.end_date.data
    current_plan.total_budget = form.total_budget.data
    current_plan.campaign_id = form_campaign.id
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


def get_plan(form, current_plan):
    form.name.data = current_plan.name
    form.description.data = current_plan.description
    form.client_requests.data = current_plan.client_requests
    form.restrictions.data = current_plan.restrictions
    form.objective.data = current_plan.objective
    form.start_date.data = current_plan.start_date
    form.end_date.data = current_plan.end_date
    form.total_budget.data = current_plan.total_budget
    form_campaign = Campaign.query.filter_by(
        id=current_plan.campaign_id).first_or_404()
    form_product = Product.query.filter_by(
        id=form_campaign.product_id).first_or_404()
    form_client = Client.query.filter_by(
        id=form_product.client_id).first_or_404()
    form.cur_campaign.data = form_campaign.name
    form.cur_product.data = form_product.name
    form.cur_client.data = form_client.name
    return form


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
        new_plan = set_plan_form(form)
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
        edit_plan(current_plan, form)
        if form.form_continue.data == 'continue':
            return redirect(url_for('plan.topline',
                                    object_name=current_plan.name))
        else:
            return redirect(url_for('plan.edit_plan',
                                    object_name=current_plan.name))
    elif request.method == 'GET':
        form = get_plan(form, current_plan)
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
        new_plan = set_plan_form(form)
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
        edit_plan(current_plan, form)
        if form.form_continue.data == 'continue':
            return redirect(url_for('plan.edit_competitive_spend',
                                    object_name=current_plan.name))
        else:
            return redirect(url_for('plan.edit_research',
                                    object_name=current_plan.name))
    elif request.method == 'GET':
        form = get_plan(form, current_plan)
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
    brandtrackers = Brandtracker.query.filter_by(user_id=current_user.id).all()
    kwargs['form'] = form
    kwargs['brandtrackers'] = brandtrackers
    return render_template('plan/plan.html', **kwargs)
