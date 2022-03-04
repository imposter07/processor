from app import db
import app.utils as utl
from flask_babel import _
from app.plan import bp
from flask_login import current_user, login_required
from flask import render_template, redirect, url_for, request, jsonify, flash
from app.plan.forms import PlanForm, EditPlanForm
from app.models import User, Client, Product, Campaign, Plan, Post, Partner


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
    kwargs['partners'] = Partner.query.filter_by(
        plan_id=kwargs['object'].id).all()
    return render_template('plan/plan.html', **kwargs)
