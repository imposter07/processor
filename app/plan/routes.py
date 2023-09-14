import os
import json
from app import db
import app.utils as utl
from flask_babel import _
from app.plan import bp
from datetime import datetime
import processor.reporting.analyze as az
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, request, jsonify, flash
from app.plan.forms import PlanForm, EditPlanForm, PlanToplineForm, \
    CreateSowForm, RfpForm
from app.models import Client, Product, Campaign, Plan, Post, Partner, \
    PlanPhase, Sow, Processor, PartnerPlacements


@bp.route('/plan', methods=['GET', 'POST'])
@login_required
def plan():
    form = PlanForm()
    form.set_choices()
    kwargs = {'form': form}
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.cur_client.data).check_and_add()
        form_product = Product(
            name=form.cur_product.data,
            client_id=form_client.id).check_and_add()
        form_campaign = Campaign(
            name=form.cur_campaign.data,
            product_id=form_product.id).check_and_add()
        new_plan = Plan(
            name=form.name.data, description=form.description.data,
            client_requests=form.client_requests.data,
            restrictions=form.restrictions.data, objective=form.objective.data,
            start_date=form.start_date.data, end_date=form.end_date.data,
            total_budget=form.total_budget.data, campaign_id=form_campaign.id)
        db.session.add(new_plan)
        db.session.commit()
        creation_text = 'Plan was requested for creation.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    plan_id=new_plan.id)
        db.session.add(post)
        db.session.commit()
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
            start_date=cur_processor.start_date, end_date=cur_processor.end_date,
            user_id=current_user.id, created_at=datetime.utcnow(),
            campaign_id=cur_processor.campaign.id)
        db.session.add(new_plan)
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
        if form.form_continue.data == 'continue':
            return redirect(url_for('plan.topline',
                                    object_name=current_plan.name))
        else:
            return redirect(url_for('plan.edit_plan',
                                    object_name=current_plan.name))
    elif request.method == 'GET':
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


@bp.route('/save_topline', methods=['GET', 'POST'])
@login_required
def save_topline():
    obj_name = request.form['object_name']
    cur_plan = Plan.query.filter_by(name=obj_name).first_or_404()
    data = json.loads(request.form['placement_form'])
    topline_list = []
    phase_list = []
    for phase_idx in data:
        phase = data[phase_idx]
        phase_data = phase['Phase']
        phase_dict = {}
        for col in ['phaseSelect', 'total_budget', 'dates']:
            col_name = '{}{}'.format(col, phase_idx)
            new_data = [x['value'] for x in phase_data
                        if x['name'] == col_name][0]
            if col == 'dates':
                phase_dict = utl.get_sd_ed_in_dict(phase_dict, new_data)
            else:
                phase_dict[col] = new_data
        phase_list.append(phase_dict)
    old_phase = PlanPhase.query.filter_by(plan_id=cur_plan.id).all()
    utl.sync_new_form_data_with_database(
        form_dict=phase_list, old_db_items=old_phase, db_model=PlanPhase,
        relation_db_item=cur_plan, form_search_name='phaseSelect',
        delete_children=True)
    for phase_idx in data:
        phase_name = [
            x for x in data[phase_idx]['Phase'] if
            x['name'] == 'phaseSelect{}'.format(phase_idx)][0]['value']
        cur_phase = PlanPhase.query.filter_by(name=phase_name,
                                              plan_id=cur_plan.id).first()
        partner_data = data[phase_idx]['partner']
        part_num_list = [int(x['name'].replace('total_budget', ''))
                         for x in partner_data if 'total_budget' in x['name']]
        for x in part_num_list:
            tl_dict = {}
            for col in ['partner_typeSelect', 'partnerSelect',
                        'total_budget', 'cpm', 'cpc', 'cplpv', 'cpbc', 'cpv',
                        'cpcv', 'dates']:
                col_name = '{}{}'.format(col, x)
                new_data = [x['value'] for x in partner_data
                            if x['name'] == col_name][0]
                if col == 'dates':
                    tl_dict = utl.get_sd_ed_in_dict(tl_dict, new_data)
                else:
                    tl_dict[col] = new_data
            topline_list.append(tl_dict)
        old_part = Partner.query.filter_by(plan_phase_id=cur_phase.id).all()
        utl.sync_new_form_data_with_database(
            form_dict=topline_list, old_db_items=old_part, db_model=Partner,
            relation_db_item=cur_phase, form_search_name='partnerSelect',
            delete_children=True)
        new_part = Partner.query.filter_by(plan_phase_id=cur_phase.id).all()
        db_df = PartnerPlacements().get_reporting_db_df()
        config_path = os.path.join('processor', 'config')
        aly = az.Analyze(load_chat=True, chat_path=config_path)
        for part in new_part:
            aly.chat.check_gg_children([], part, db_df)
    return jsonify({'message': 'This data source {} was saved!'.format(
        obj_name), 'level': 'success'})


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
        current_sow.campaign = form.campaign.data
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


@bp.route('/plan/<object_name>/plan_placements', methods=['GET', 'POST'])
@login_required
def plan_placements(object_name):
    kwargs = Plan().get_current_plan(
        object_name, 'edit_plan', edit_progress=100, edit_name='PlanPlacements')
    kwargs['form'] = PlanToplineForm()
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
def rfp_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_plan = Plan.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
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
