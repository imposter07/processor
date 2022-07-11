from app import db
import numpy as np
import pandas as pd
import datetime as dt
import app.utils as utl
from flask_babel import _
from app.plan import bp
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, request, jsonify, flash
from app.plan.forms import PlanForm, EditPlanForm, PlanToplineForm, \
    CreateSowForm
from app.models import Client, Product, Campaign, Plan, Post, Partner, \
    ProcessorAnalysis, PlanPhase, Sow


@bp.route('/plan', methods=['GET', 'POST'])
@login_required
def plan():
    form = PlanForm()
    form.set_choices()
    kwargs = Plan.get_current_plan()
    kwargs['form'] = form
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


@bp.route('/plan/<object_name>', methods=['GET', 'POST'])
@login_required
def edit_plan(object_name):
    kwargs = Plan.get_current_plan(object_name, 'edit_plan', edit_progress=50,
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
            return redirect(url_for('plan.edit_plan',
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


@bp.route('/get_topline', methods=['GET', 'POST'])
@login_required
def get_topline():
    cur_plan = Plan.query.filter_by(
        name=request.form['object_name']).first_or_404()
    partners = Partner.query.filter(
        Partner.plan_phase_id.in_([x.id for x in cur_plan.phases])).all()
    partners = [x.get_form_dict() for x in partners]
    a = ProcessorAnalysis.query.filter_by(
        processor_id=23, key='database_cache',
        parameter='vendorname|vendortypename').first()
    df = pd.read_json(a.data)
    df = df[df['impressions'] > 0].sort_values('impressions', ascending=False)
    df['cpm'] = (df['netcost'] / (df['impressions'] / 1000)).round(2)
    df['cpc'] = (df['netcost'] / df['clicks']).round(2)
    df['cplpv'] = df['CPLPV'].round(2)
    df['Landing Page'] = df['landingpage']
    df['cpbc'] = df['CPBC'].round(2)
    df['Button Clicks'] = df['buttonclick']
    df['Views'] = df['videoviews']
    df['cpv'] = df['CPV'].round(2)
    df['Video Views 100'] = df['videoviews100']
    df['cpcv'] = (df['netcost'] / df['videoviews100']).round(2)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(0)
    df = df[['vendorname', 'vendortypename', 'cpm', 'cpc',
             'cplpv', 'cpbc', 'cpv', 'cpcv']]
    df = df.rename(columns={
        'vendorname': 'Partner', 'vendortypename': 'partner_type'})
    partner_list = df.to_dict(orient='records')
    sd = cur_plan.start_date
    ed = cur_plan.end_date
    weeks = [sd + dt.timedelta(days=x)
             for i, x in enumerate(range((ed-sd).days)) if i % 7 == 0]
    weeks_str = [dt.datetime.strftime(x, '%Y-%m-%d') for x in weeks]
    metric_cols = ['cpm', 'Impressions', 'cpc', 'Clicks',
                   'cplpv', 'Landing Page', 'cpbc', 'Button Clicks', 'Views',
                   'cpv', 'Video Views 100', 'cpcv']
    col_list = (['partner_type', 'Partner', 'total_budget', 'Phase'] +
                weeks_str + metric_cols)
    cols = []
    for x in col_list:
        cur_col = {'name': x, 'type': '', 'add_select_box': False,
                   'hidden': False, 'header': False}
        if x == 'Partner':
            cur_col['type'] = 'select'
            cur_col['values'] = partner_list
            cur_col['add_select_box'] = True
        if x == 'partner_type':
            cur_col['type'] = 'select'
            cur_col['values'] = pd.DataFrame(
                df['partner_type'].unique()).rename(
                    columns={0: 'partner_type'}).to_dict(orient='records')
        if x == 'Phase':
            cur_col['type'] = 'select'
            cur_col['values'] = ['Launch', 'Pre-Launch']
            cur_col['hidden'] = True
            cur_col['header'] = True
        if x in metric_cols:
            cur_col['type'] = 'metrics'
        cols.append(cur_col)
    phases = [x.get_form_dict() for x in cur_plan.phases.all()]
    return jsonify({'data': {'partners': partners,
                             'cols': cols, 'phases': phases}})


@bp.route('/save_topline', methods=['GET', 'POST'])
@login_required
def save_topline():
    obj_name = request.form['object_name']
    cur_plan = Plan.query.filter_by(name=obj_name).first_or_404()
    data = request.form.to_dict()
    topline_list = []
    num_partners = max([v.split('name')[1] for k, v in data.items() if
                        'name' in v and 'name' in k])
    for x in range(int(num_partners) + 1):
        tl_dict = {}
        for col in ['name', 'dates', 'total_budget', 'cpm', 'cpc']:
            col_name = '{}{}'.format(col, x)
            new_data = utl.get_col_from_serialize_dict(data, col_name)[0]
            if col == 'dates':
                date_list = new_data.split(' to ')
                sd = date_list[0]
                ed = date_list[1]
                tl_dict['start_date'] = dt.datetime.strptime(sd, '%Y-%m-%d')
                tl_dict['end_date'] = dt.datetime.strptime(ed, '%Y-%m-%d')
            else:
                tl_dict[col] = new_data
        topline_list.append(tl_dict)
    old_part = Partner.query.filter(
        Partner.plan_phase_id.in_([x.id for x in cur_plan.phases])).all()
    if old_part:
        for p in old_part:
            new_p = [x for x in topline_list if p.name == x['name']]
            if new_p:
                new_p = new_p[0]
                p.set_from_form(form=new_p, current_plan=cur_plan)
                db.session.commit()
                topline_list = [x for x in topline_list if p.name != x['name']]
            else:
                db.session.delete(p)
    for p in topline_list:
        new_p = Partner()
        new_p.set_from_form(form=p, current_plan=cur_plan)
        db.session.add(new_p)
        db.session.commit()
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
    if request.method == 'POST':
        form.validate()
        if not current_sow:
            current_sow = Sow(plan_id=cur_plan.id)
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
        current_sow.total_project_budget = form.total_project_budget.data
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
        form.campaign.data = current_sow.campaign
        form.address.data = current_sow.address
        form.phone.data = current_sow.phone
        form.fax.data = current_sow.fax
        form.total_project_budget.data = current_sow.total_project_budget
        form.ad_serving.data = current_sow.ad_serving
    return render_template('plan/plan.html', **kwargs)
