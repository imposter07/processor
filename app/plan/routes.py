from app import db
import numpy as np
import pandas as pd
import datetime as dt
import app.utils as utl
from flask_babel import _
from app.plan import bp
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, request, jsonify, flash
from app.plan.forms import PlanForm, EditPlanForm, PlanToplineForm
from app.models import Client, Product, Campaign, Plan, Post, Partner, \
    ProcessorAnalysis


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
    kwargs = Plan.get_current_plan(object_name, 'edit_plan', edit_progress=100,
                                   edit_name='Topline')
    kwargs['form'] = PlanToplineForm()
    return render_template('plan/plan.html', **kwargs)


@bp.route('/get_topline', methods=['GET', 'POST'])
@login_required
def get_topline():
    cur_plan = Plan.query.filter_by(
        name=request.form['object_name']).first_or_404()
    partners = Partner.query.filter_by(plan_id=cur_plan.id).all()
    partners = [x.get_form_dict() for x in partners]
    a = ProcessorAnalysis.query.filter_by(
        processor_id=23, key='database_cache', parameter='vendorname').first()
    df = pd.read_json(a.data)
    df = df[df['impressions'] > 0].sort_values('impressions', ascending=False)
    df['eCPM'] = (df['netcost'] / (df['impressions'] / 1000)).round(2)
    df['eCPC'] = (df['netcost'] / df['clicks']).round(2)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(0)
    df = df.rename(columns={'vendorname': 'name'})
    partner_list = df.to_dict(orient='records')
    sd = cur_plan.start_date
    ed = cur_plan.end_date
    weeks = [sd + dt.timedelta(days=x)
             for i, x in enumerate(range((ed-sd).days)) if i % 7 == 0]
    weeks_str = [dt.datetime.strftime(x, '%Y-%m-%d') for x in weeks]
    col_list = ['Partner', 'Cost'] + weeks_str + ['eCPM', 'eCPC']
    cols = []
    for x in col_list:
        cur_col = {'name': x, 'type': ''}
        if x == 'Partner':
            cur_col['type'] = 'select'
            cur_col['values'] = partner_list
        cols.append(cur_col)
    weeks = [
        [dt.datetime.strftime(x, '%Y-%m-%d'),
         dt.datetime.strftime(x + dt.timedelta(days=6), '%Y-%m-%d')]
        for x in weeks]
    return jsonify({'data': {'partners': partners,
                             'cols': cols,
                             'weeks': weeks}})


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
        for col in ['name', 'dates', 'total_budget', 'estimated_cpm',
                    'estimated_cpc']:
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
    old_part = Partner.query.filter_by(plan_id=cur_plan.id).all()
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
