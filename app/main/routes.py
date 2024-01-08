import io
import os
import sys
import html
import json
import zipfile
import pandas as pd
import app.utils as utl
import datetime as dt
from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, \
    jsonify, current_app, send_file
from flask_login import current_user, login_required
from flask_babel import _, get_locale
from guess_language import guess_language
from app import db
from app.main.forms import EditProfileForm, PostForm, SearchForm, MessageForm, \
    ProcessorForm, EditProcessorForm, ImportForm, APIForm, ProcessorCleanForm, \
    ProcessorExportForm, UploaderForm, EditUploaderForm, ProcessorRequestForm, \
    GeneralAccountForm, EditProcessorRequestForm, FeeForm, \
    GeneralConversionForm, ProcessorRequestFinishForm, \
    ProcessorContinueForm, ProcessorFixForm, ProcessorRequestCommentForm, \
    ProcessorDuplicateForm, EditUploaderMediaPlanForm, \
    EditUploaderNameCreateForm, EditUploaderCreativeForm, \
    UploaderDuplicateForm, ProcessorDashboardForm, ProcessorCleanDashboardForm, \
    PlacementForm, ProcessorDeleteForm, ProcessorDuplicateAnotherForm, \
    ProcessorNoteForm, ProcessorAutoAnalysisForm, ProcessorReportBuilderForm, \
    WalkthroughUploadForm, ProcessorPlanForm, UploadTestForm, ScreenshotForm, \
    BrandTrackerImportForm, EditProjectForm
from app.models import User, Post, Message, Notification, Processor, \
    Client, Product, Campaign, ProcessorDatasources, TaskScheduler, \
    Uploader, Account, RateCard, Conversion, Requests, UploaderObjects, \
    UploaderRelations, Dashboard, DashboardFilter, ProcessorAnalysis, Project, \
    Notes, ProcessorReports, Tutorial, TutorialStage, Task, Plan, Walkthrough, \
    Conversation, Chat, WalkthroughSlide, Rfp, Specs, Contacts

from functools import wraps
from app.translate import translate
from app.main import bp
import processor.reporting.vmcolumns as vmc
import processor.reporting.calc as cal
import processor.reporting.analyze as az
import app.utils as app_utl


@bp.before_app_request
def before_request():
    if current_user.is_authenticated:
        current_user.last_seen = datetime.utcnow()
        db.session.commit()
        g.search_form = SearchForm()
    g.locale = str(get_locale())


def error_handler(route_function):
    @wraps(route_function)
    def decorated_function(*args, **kwargs):
        try:
            result = route_function(*args, **kwargs)
            return result
        except:
            args = request.form.to_dict(flat=False)
            msg = 'Unhandled exception {}'.format(json.dumps(args))
            current_app.logger.error(msg, exc_info=sys.exc_info())
            data = {'data': 'error', 'task': '', 'level': 'error',
                    'args': request.form.to_dict(flat=False)}
            return jsonify(data)

    return decorated_function


@bp.route('/', methods=['GET', 'POST'])
@bp.route('/index', methods=['GET', 'POST'])
@login_required
def index():
    form = PostForm()
    tutorials = Tutorial.query.all()
    if form.validate_on_submit():
        language = guess_language(form.post.data)
        if language == 'UNKNOWN' or len(language) > 5:
            language = ''
        post = Post(body=form.post.data, author=current_user, language=language)
        db.session.add(post)
        db.session.commit()
        flash(_('Your post is now live!'))
        return redirect(url_for('main.index'))
    page = request.args.get('page', 1, type=int)
    posts = current_user.followed_posts().paginate(
        page=page, per_page=current_app.config['POSTS_PER_PAGE'],
        error_out=False)
    next_url = url_for('main.index', page=posts.next_num) \
        if posts.has_next else None
    prev_url = url_for('main.index', page=posts.prev_num) \
        if posts.has_prev else None
    return render_template('index.html', title=_('Home'), form=form,
                           posts=posts.items, next_url=next_url,
                           prev_url=prev_url, tutorials=tutorials,
                           user=current_user)


@bp.route('/health_check', methods=['GET'])
def health_check():
    return jsonify({'data': 'success'}), 200


@bp.route('/explore')
@login_required
def explore():
    tutorials = Tutorial.query.all()
    page = request.args.get('page', 1, type=int)
    posts = Post.query.order_by(Post.timestamp.desc()).paginate(
        page=page, per_page=current_app.config['POSTS_PER_PAGE'],
        error_out=False)
    next_url = url_for('main.explore', page=posts.next_num) \
        if posts.has_next else None
    prev_url = url_for('main.explore', page=posts.prev_num) \
        if posts.has_prev else None
    return render_template('index.html', title=_('Explore'),
                           posts=posts.items, next_url=next_url,
                           prev_url=prev_url, tutorials=tutorials,
                           user=current_user)


@bp.route('/get_processor_by_date', methods=['GET', 'POST'])
@login_required
def get_processor_by_date():
    processors = Processor.query.order_by(Processor.created_at)
    event_response = [{'title': 'Created: {}'.format(x.name),
                       'start': x.created_at.date().isoformat(),
                       'url': url_for(
                           'main.processor_page', object_name=x.name),
                       'color': 'LightGreen',
                       'textColor': 'Black',
                       'borderColor': 'DimGray'}
                      for x in processors]
    event_response.extend(
        [{'title': x.name,
          'start': x.start_date.isoformat(),
          'end': x.end_date.isoformat(),
          'url': url_for('main.processor_page', object_name=x.name),
          'color': 'LightSkyBlue',
          'textColor': 'Black',
          'borderColor': 'DimGray'}
         for x in processors if x.start_date and x.end_date])
    return jsonify(event_response)


@bp.route('/get_live_processors', methods=['GET', 'POST'])
@login_required
def get_live_processors():
    page_num = int(request.form['page'])
    page = request.args.get('page', page_num, type=int)
    if request.form['followed'] == 'true':
        processors = current_user.processor_followed
    else:
        processors = Processor.query
    processors = processors.filter(
        Processor.end_date > datetime.today().date()).order_by(
        Processor.created_at.desc())
    processors_all = [x.name for x in processors.all()]
    if 'filter_dict' in request.form and request.form['filter_dict'] != 'null':
        for d in json.loads(request.form['filter_dict']):
            for k, v in d.items():
                if k == "processors":
                    processors = processors.filter(Processor.name.in_(v))
    processors = processors.paginate(page=page, per_page=3, error_out=False)
    processor_html = [render_template('processor_popup.html', processor=x)
                      for x in processors.items]
    return jsonify({'items': processor_html,
                    'pages': processors.pages,
                    'has_next': processors.has_next,
                    'has_prev': processors.has_prev,
                    'processors': processors_all})


@bp.route('/get_open_processor_requests', methods=['GET', 'POST'])
@login_required
def get_open_processor_requests():
    if request.form['followed'] == 'true':
        processors = current_user.processor_followed
    else:
        processors = Processor.query
    processors = processors.filter(
        Processor.end_date > datetime.today().date()).order_by(
        Processor.created_at.desc())
    if 'filter_dict' in request.form and request.form['filter_dict'] != 'null':
        for d in json.loads(request.form['filter_dict']):
            for k, v in d.items():
                if k == "processors":
                    processors = processors.filter(Processor.name.in_(v))
    processor_html = render_template('all_open_requests.html',
                                     processors=processors.all())
    return jsonify({'items': processor_html})


@bp.route('/get_processor_body', methods=['GET', 'POST'])
@login_required
def get_processor_body():
    response = {}
    cur_processor = Processor.query.get(request.form['processor_id'])
    current_users = User.query.order_by(User.username).all()
    current_projects = Project.query.order_by(Project.project_name).all()
    proc_html = render_template(
        'processor_user_map_body.html', processor=cur_processor,
        current_users=current_users, project_numbers=current_projects)
    response['processor_body'] = proc_html
    return jsonify(response)


@bp.route('/get_processor_by_user', methods=['GET', 'POST'])
@login_required
def get_processor_by_user():
    response = {}
    processors = Processor.query.order_by(Processor.created_at)
    seven_days_ago = dt.datetime.today() - dt.timedelta(days=7)
    if 'filter_dict' in request.form:
        processors = utl.parse_filter_dict_from_clients(
            processors, seven_days_ago, request)
    if 'user_map' in request.form and request.form['user_map']:
        processor_html = utl.get_processor_user_map(processors)
        response['items'] = processor_html
    else:
        clients_html = utl.get_processor_client_directory(processors)
        response['client_directory'] = clients_html
    return jsonify(response)


@bp.route('/get_project_numbers', methods=['GET', 'POST'])
@login_required
def get_project_numbers():
    processors = Processor.query.order_by(Processor.created_at)
    seven_days_ago = dt.datetime.today() - dt.timedelta(days=7)
    if 'filter_dict' in request.form:
        processors = utl.parse_filter_dict_from_clients(
            processors, seven_days_ago, request)
    cur_processor = Processor.query.get(23)
    task = cur_processor.launch_task(
        '.get_project_numbers', _('Getting project numbers.'),
        running_user=current_user.id)
    db.session.commit()
    job = task.wait_and_get_job(loops=20)
    df = job.result[0]
    data = df_to_html(df, 'projectNumberTable')
    new_dict = {}
    for x in processors:
        client = x.campaign.product.client
        product = x.campaign.product
        campaign = x.campaign
        if client not in new_dict:
            new_dict[client] = {}
        if product not in new_dict[client]:
            new_dict[client][product] = {}
        if campaign not in new_dict[client][product]:
            new_dict[client][product][campaign] = []
        new_dict[client][product][campaign].append(x)
    new_dict = {key: new_dict[key] for
                key in sorted(new_dict.keys(), key=lambda y: y.name)}
    clients_html = render_template('_client_directory.html',
                                   client_dict=new_dict)
    return jsonify({'items': data['data'],
                    'client_directory': clients_html})


@bp.route('/processor_change_owner', methods=['GET', 'POST'])
@login_required
def processor_change_owner():
    object_name = request.form['object_name']
    new_owner = request.form['new_owner']
    cur_obj = Processor.query.filter_by(name=object_name).first_or_404()
    new_user = User.query.filter_by(username=new_owner).first_or_404()
    cur_obj.user_id = new_user.id
    msg = 'You have successfully assigned {} as the owner of {}'.format(
        new_user.username, cur_obj.name)
    post = Post(body=msg, author=current_user,
                processor_id=cur_obj.id)
    db.session.add(post)
    cur_obj.launch_task('.processor_assignment_email', _(msg),
                        current_user.id)
    db.session.commit()
    lvl = 'success'
    msg = '<strong>{}</strong>, {}'.format(current_user.username, msg)
    return jsonify({'data': 'success', 'message': msg, 'level': lvl})


@bp.route('/clients')
@login_required
def clients():
    current_clients = Client.query.order_by(Client.name).all()
    current_users = User.query.order_by(User.username).all()
    current_products = Product.query.order_by(Product.name).all()
    current_campaigns = Campaign.query.order_by(Campaign.name).all()
    current_processors = Processor.query.order_by(Processor.name).all()
    current_projects = Project.query.order_by(Project.project_name).all()
    view_selector = Client.get_client_view_selector('Clients')
    return render_template('clients.html', title=_('Clients'),
                           clients=current_clients,
                           current_users=current_users,
                           current_products=current_products,
                           current_campaigns=current_campaigns,
                           current_processors=current_processors,
                           current_projects=current_projects,
                           view_selector=view_selector)


@bp.route('/project_numbers')
@login_required
def project_numbers():
    current_clients = Client.query.order_by(Client.name).all()
    current_users = User.query.order_by(User.username).all()
    current_products = Product.query.order_by(Product.name).all()
    current_campaigns = Campaign.query.order_by(Campaign.name).all()
    current_processors = Processor.query.order_by(Processor.name).all()
    view_selector = Client.get_client_view_selector('Project Numbers')
    kwargs = Project.get_current_project(Project, edit_name='ProjectNumber')
    kwargs['title'] = 'ProjectNumber'
    return render_template(
        'clients.html',
        clients=current_clients, current_users=current_users,
        current_products=current_products, current_campaigns=current_campaigns,
        current_processors=current_processors, view_selector=view_selector,
        **kwargs)


@bp.route('/user/<username>')
@login_required
def user(username):
    user_page = User.query.filter_by(username=username).first_or_404()
    page = request.args.get('page', 1, type=int)
    posts = user_page.posts.order_by(Post.timestamp.desc()).paginate(
        page=page, per_page=current_app.config['POSTS_PER_PAGE'],
        error_out=False)
    processors = user_page.processor_followed.filter(
        Processor.end_date > datetime.today().date()).order_by(
        Processor.created_at.desc()).all()
    tutorials = Tutorial.query.all()
    next_url = url_for('main.user', username=user_page.username,
                       page=posts.next_num) if posts.has_next else None
    prev_url = url_for('main.user', username=user_page.username,
                       page=posts.prev_num) if posts.has_prev else None
    return render_template('user.html', user=user_page, posts=posts.items,
                           next_url=next_url, prev_url=prev_url,
                           title=_('User | {}'.format(username)),
                           tutorials=tutorials, processors=processors)


@bp.route('/edit_profile', methods=['GET', 'POST'])
@login_required
def edit_profile():
    form = EditProfileForm(current_user.username)
    if form.validate_on_submit():
        current_user.username = form.username.data
        current_user.about_me = form.about_me.data
        db.session.commit()
        flash(_('Your changes have been saved.'))
        return redirect(url_for('main.edit_profile'))
    elif request.method == 'GET':
        form.username.data = current_user.username
        form.about_me.data = current_user.about_me
    return render_template('edit_profile.html', title=_('Edit Profile'),
                           form=form)


@bp.route('/follow/<username>')
@login_required
def follow(username):
    user_follow = User.query.filter_by(username=username).first()
    if user_follow is None:
        flash(_('User %(username)s not found.', username=username))
        return redirect(url_for('main.index'))
    if user_follow == current_user:
        flash(_('You cannot follow yourself!'))
        return redirect(url_for('main.user', username=username))
    current_user.follow(user_follow)
    db.session.commit()
    flash(_('You are following %(username)s!', username=username))
    return redirect(url_for('main.user', username=username))


@bp.route('/unfollow/<username>')
@login_required
def unfollow(username):
    user_unfollow = User.query.filter_by(username=username).first()
    if user_unfollow is None:
        flash(_('User %(username)s not found.', username=username))
        return redirect(url_for('main.index'))
    if user_unfollow == current_user:
        flash(_('You cannot unfollow yourself!'))
        return redirect(url_for('main.user', username=username))
    current_user.unfollow(user_unfollow)
    db.session.commit()
    flash(_('You are not following %(username)s.', username=username))
    return redirect(url_for('main.user', username=username))


@bp.route('/follow_processor/<object_name>')
@login_required
def follow_processor(object_name):
    processor_follow = Processor.query.filter_by(name=object_name).first()
    if processor_follow is None:
        flash(_('Processor {} not found.'.format(object_name)))
        return redirect(url_for('main.index'))
    current_user.follow_processor(processor_follow)
    db.session.commit()
    flash(_('You are following {}!'.format(processor_follow.name)))
    return redirect(url_for('main.processor_page',
                            object_name=processor_follow.name))


@bp.route('/unfollow_processor/<object_name>')
@login_required
def unfollow_processor(object_name):
    processor_unfollow = Processor.query.filter_by(name=object_name).first()
    if processor_unfollow is None:
        flash(_('Processor {} not found.'.format(object_name)))
        return redirect(url_for('main.index'))
    current_user.unfollow_processor(processor_unfollow)
    db.session.commit()
    flash(_('You are no longer following {}!'.format(processor_unfollow.name)))
    return redirect(url_for('main.processor_page',
                            object_name=processor_unfollow.name))


@bp.route('/get_completed_task', methods=['GET', 'POST'])
@login_required
def get_completed_task():
    task = Task.query.get(request.form['task'])
    table_name, cur_proc, proc_arg, job_name = get_table_arguments()
    response = get_table_return(task, table_name, proc_arg, job_name)
    return response


@bp.route('/get_task_progress', methods=['GET', 'POST'])
@login_required
def get_task_progress():
    proc_arg = {'use_cache': (
            'args' in request.form and 'use_cache' in request.form['args'])}
    object_name = request.form['object_name']
    task_name = request.form['task_name']
    cur_obj = request.form['object_type']
    if cur_obj == 'Processor':
        cur_obj = Processor
    else:
        cur_obj = Uploader
    job_name, table_name, proc_arg = translate_table_name_to_job(
        task_name, proc_arg=proc_arg)
    if request.form['object_id'] != 'None':
        cur_obj = Processor.query.get(request.form['object_id'])
    else:
        cur_obj = cur_obj.query.filter_by(name=object_name).first()
    data = {'complete': False}
    if 'task' in request.form and request.form['task']:
        task = Task.query.get(request.form['task'])
        if task.complete:
            data['complete'] = True
            if 'table' not in request.form:
                job = task.get_rq_job()
                if job and job.result:
                    dfs = job.result[0]
                    data['data'] = dfs.reset_index().to_dict(
                        orient='records')
    else:
        task = cur_obj.get_task_in_progress(name=job_name)
    if task:
        percent = task.get_progress()
        if task.complete:
            data['complete'] = True
    else:
        percent = 90
    data['percent'] = percent
    return jsonify(data)


@bp.route('/translate', methods=['POST'])
@login_required
def translate_text():
    return jsonify({'text': translate(request.form['text'],
                                      request.form['source_language'],
                                      request.form['dest_language'])})


@bp.route('/search')
@login_required
def search():
    if not g.search_form.validate():
        return redirect(url_for('main.explore'))
    page = request.args.get('page', 1, type=int)
    posts, total = Post.search(g.search_form.q.data, page,
                               current_app.config['POSTS_PER_PAGE'])
    next_url = url_for('main.search', q=g.search_form.q.data, page=page + 1) \
        if total > page * current_app.config['POSTS_PER_PAGE'] else None
    prev_url = url_for('main.search', q=g.search_form.q.data, page=page - 1) \
        if page > 1 else None
    return render_template('search.html', title=_('Search'), posts=posts,
                           next_url=next_url, prev_url=prev_url)


@bp.route('/user/<username>/popup')
@login_required
def user_popup(username):
    user_for_popup = User.query.filter_by(username=username).first_or_404()
    return render_template('user_popup.html', user=user_for_popup)


@bp.route('/send_message/<recipient>', methods=['GET', 'POST'])
@login_required
def send_message(recipient):
    rec_user = User.query.filter_by(username=recipient).first_or_404()
    form = MessageForm()
    if form.validate_on_submit():
        msg = Message(author=current_user, recipient=rec_user,
                      body=form.message.data)
        db.session.add(msg)
        rec_user.add_notification('unread_message_count',
                                  rec_user.new_messages())
        db.session.commit()
        flash(_('Your message has been sent.'))
        return redirect(url_for('main.user', username=recipient))
    return render_template('send_message.html', title=_('Send Message'),
                           form=form, recipient=recipient)


@bp.route('/messages')
@login_required
def messages():
    current_user.last_message_read_time = datetime.utcnow()
    current_user.add_notification('unread_message_count', 0)
    db.session.commit()
    page = request.args.get('page', 1, type=int)
    user_messages = current_user.messages_received.order_by(
        Message.timestamp.desc()).paginate(
        page=page, per_page=current_app.config['POSTS_PER_PAGE'],
        error_out=False)
    next_url = url_for('main.messages', page=user_messages.next_num) \
        if user_messages.has_next else None
    prev_url = url_for('main.messages', page=user_messages.prev_num) \
        if user_messages.has_prev else None
    return render_template('messages.html', messages=user_messages.items,
                           next_url=next_url, prev_url=prev_url,
                           title=_('Messages'))


@bp.route('/notifications')
@login_required
def notifications():
    since = request.args.get('since', 0.0, type=float)
    user_notifications = current_user.notifications.filter(
        Notification.timestamp > since).order_by(Notification.timestamp.asc())
    return jsonify([{
        'name': n.name,
        'data': n.get_data(),
        'timestamp': n.timestamp
    } for n in user_notifications])


@bp.route('/export_posts')
@login_required
def export_posts():
    if current_user.get_task_in_progress('.export_posts'):
        flash(_('An export task is currently in progress'))
    else:
        current_user.launch_task('.export_posts', _('Exporting posts...'))
        db.session.commit()
    return redirect(url_for('main.user', username=current_user.username))


@bp.route('/processor')
@login_required
def processor():
    kwargs = Processor().get_current_processor('Base Processor', 'processor')
    return render_template('processor.html', **kwargs)


@bp.route('/create_processor', methods=['GET', 'POST'])
@login_required
def create_processor():
    form = ProcessorForm()
    form.set_choices()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.cur_client.data).check_and_add()
        form_product = Product(name=form.cur_product.data,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.cur_campaign.data,
                                 product_id=form_product.id).check_and_add()
        new_processor = Processor(
            name=form.name.data, description=form.description.data,
            user_id=current_user.id, created_at=datetime.utcnow(),
            local_path=form.local_path.data, start_date=form.start_date.data,
            end_date=form.end_date.data,
            tableau_workbook=form.tableau_workbook.data,
            tableau_view=form.tableau_view.data,
            tableau_datasource=form.tableau_datasource.data,
            campaign_id=form_campaign.id)
        db.session.add(new_processor)
        db.session.commit()
        cur_user.follow_processor(new_processor)
        db.session.commit()
        post_body = 'Create Processor {}...'.format(new_processor.name)
        new_processor.launch_task('.create_processor', _(post_body),
                                  current_user.id,
                                  current_app.config['BASE_PROCESSOR_PATH'])
        creation_text = 'Processor was requested for creation.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=new_processor.id)
        db.session.add(post)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_import',
                                    object_name=new_processor.name))
        else:
            return redirect(url_for('main.processor'))
    return render_template('create_processor.html', user=cur_user,
                           title=_('Processor'), form=form, edit_progress="25",
                           edit_name='Basic',
                           buttons=Processor().get_navigation_buttons())


def add_df_to_processor_dict(form_import, processor_dicts):
    for fi in form_import:
        if ('raw_file' in fi and fi['raw_file']
                and fi['raw_file'] != 'undefined'
                and '{"data":"success' not in fi['raw_file']):
            df = utl.convert_file_to_df(fi['raw_file'])
            new_dict = [x for x in processor_dicts
                        if x[vmc.vendorkey] == fi['vendor_key']][0]
            processor_dicts = [x for x in processor_dicts
                               if x[vmc.vendorkey] != fi['vendor_key']]
            new_dict['raw_file'] = df
            processor_dicts.append(new_dict)
    return processor_dicts


def set_processor_imports_in_db(processor_id, form_imports):
    cur_processor = Processor.query.get(processor_id)
    old_imports = ProcessorDatasources.query.filter_by(
        processor_id=cur_processor.id).all()
    proc_imports = []
    for imp in form_imports:
        proc_import = ProcessorDatasources()
        proc_import.set_from_form(imp, cur_processor)
        proc_import.get_full_dict()
        proc_imports.append(proc_import)
    proc_import_dicts = [x.form_dict for x in proc_imports]
    for imp in old_imports:
        imp.get_full_dict()
        if imp.key and imp.form_dict not in proc_import_dicts:
            db.session.delete(imp)
    old_import_dicts = [x.form_dict for x in old_imports]
    for imp in proc_imports:
        if imp.form_dict not in old_import_dicts:
            db.session.add(imp)
    db.session.commit()
    processor_dicts = [x.get_import_processor_dict() for x in proc_imports]
    processor_dicts = add_df_to_processor_dict(form_imports, processor_dicts)
    return processor_dicts


@bp.route('/processor/<object_name>/edit/import/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_import_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    if file_type not in ['.xlsx', '.csv']:
        msg = 'FAILED: File was not an xlsx or csv.  Only use those types.'
        return jsonify({'data': 'failed', 'message': msg, 'level': 'danger'})
    search_dict = {}
    for col in ['account_id', 'start_date', 'api_fields', 'key',
                'account_filter', 'name']:
        col_val = [x for x in object_form if x['name'] ==
                   current_key.replace('raw_file', col)][0]['value']
        if col_val:
            search_dict[col] = col_val
        else:
            if col != 'start_date':
                search_dict[col] = ''
    search_dict['processor_id'] = cur_proc.id
    ds = ProcessorDatasources.query.filter_by(**search_dict).first()
    if not ds:
        new_name = search_dict['name']
        msg_text = 'Adding new raw data file for {}'.format(new_name)
        cur_proc.launch_task(
            '.write_raw_data', _(msg_text),
            running_user=current_user.id, new_data=mem,
            vk=None, mem_file=True, new_name=new_name, file_type=file_type)
    else:
        msg_text = 'Adding raw data for {}'.format(ds.vendor_key)
        cur_proc.launch_task(
            '.write_raw_data', _(msg_text),
            running_user=current_user.id, new_data=mem,
            vk=ds.vendor_key, mem_file=True, file_type=file_type)
    db.session.commit()
    msg = 'File was saved.'
    return jsonify({'data': 'success', 'message': msg, 'level': 'success'})


@bp.route('/processor/<object_name>/edit/import', methods=['GET', 'POST'])
@login_required
def edit_processor_import(object_name):
    form_description = """
    Add new, or edit existing data sources for this processor.  New data sources
    may be added with 'Add API'.
    """
    base_template = 'create_processor.html'
    kwargs = Processor().get_current_processor(
        object_name, 'edit_processor_import', 50, 'Import', form_title='IMPORT',
        form_description=form_description)
    cur_proc = kwargs['processor']
    form_class = ImportForm
    if cur_proc.is_brandtracker():
        base_template = 'brandtracker/processor_brandtracker.html'
        form_class = BrandTrackerImportForm
    apis = form_class().set_apis(kwargs['processor'])
    form = form_class(apis=apis)
    form.set_vendor_key_choices(current_processor_id=cur_proc.id)
    kwargs['form'] = form
    for api in form.apis:
        if api.delete.data:
            if api.__dict__['object_data']:
                delete_name = api.__dict__['object_data']['name']
            else:
                delete_name = ''
            ds = ProcessorDatasources.query.filter_by(
                account_id=api.account_id.data, start_date=api.start_date.data,
                api_fields=api.api_fields.data, key=api.key.data,
                account_filter=api.account_filter.data,
                name=delete_name).first()
            if ds:
                db.session.delete(ds)
                db.session.commit()
            return redirect(url_for('main.edit_processor_import',
                                    object_name=cur_proc.name))
    form_imports = form.apis.data
    if request.method == 'POST':
        if cur_proc.is_brandtracker():
            if 'table_data' in request.form:
                table_imports = json.loads(request.form['table_data'])
                form_imports = form.make_brandtracker_sources(
                    table_imports, form.apis.data, cur_proc)
                old_imports = cur_proc.get_requests_processor_analysis(
                    az.Analyze.brandtracker_imports)
                if old_imports:
                    old_imports.data = pd.DataFrame(table_imports).to_dict()
                    old_imports.date = datetime.today().date()
                else:
                    new_imports = ProcessorAnalysis(
                        key=az.Analyze.brandtracker_imports,
                        data=pd.DataFrame(table_imports).to_dict(),
                        processor_id=cur_proc.id, date=datetime.today().date())
                    db.session.add(new_imports)
                db.session.commit()
        if cur_proc.get_task_in_progress('.set_processor_imports'):
            flash(_('The data sources are already being set.'))
        else:
            form_imports = set_processor_imports_in_db(
                processor_id=cur_proc.id, form_imports=form_imports)
            msg_text = ('Setting imports in '
                        'vendormatrix for {}').format(cur_proc.name)
            task = cur_proc.launch_task(
                '.set_processor_imports', _(msg_text),
                running_user=current_user.id, form_imports=form_imports,
                set_in_db=False)
            db.session.commit()
            task.wait_and_get_job(loops=20)
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_clean',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_import',
                                    object_name=cur_proc.name))
    return render_template(base_template, **kwargs)


@bp.route('/add_processor_import', methods=['GET', 'POST'])
@login_required
def add_processor_import():
    form_class = ImportForm
    if 'table_data' in request.form:
        form_class = BrandTrackerImportForm
    orig_form = form_class(request.form)
    kwargs = orig_form.data
    new_api = APIForm(formdata=None)
    kwargs['apis'].insert(0, new_api.data)
    form = form_class(formdata=None, **kwargs)
    import_form = render_template('_form.html', form=form)
    return jsonify({'form': import_form})


@bp.route('/delete_processor_import', methods=['GET', 'POST'])
@login_required
def delete_processor_import():
    delete_id = int(request.args.get('delete_id').split('-')[1])
    form_class = ImportForm
    if 'table_data' in request.form:
        form_class = BrandTrackerImportForm
    orig_form = form_class(request.form)
    kwargs = orig_form.data
    del kwargs['apis'][delete_id]
    form = form_class(formdata=None, **kwargs)
    import_form = render_template('_form.html', form=form)
    return jsonify({'form': import_form})


@bp.route('/get_import_placeholders', methods=['GET', 'POST'])
@login_required
def get_import_placeholders():
    api_details = {}
    api_slide = WalkthroughSlide.query.filter_by(
        walkthrough_id=5, slide_number=2).first()
    if api_slide:
        api_details = api_slide.get_data()
    return jsonify({'api_details': api_details})


@bp.route('/get_test_apis', methods=['GET', 'POST'])
@login_required
def get_test_apis():
    test_apis = vmc.test_apis
    return jsonify({'test_apis': test_apis})


@bp.route('/get_log', methods=['GET', 'POST'])
@login_required
def get_log():
    if request.form['object_type'] == 'Processor':
        item_model = Processor
        task = '.get_logfile'
    elif request.form['object_type'] == 'Uploader':
        item_model = Uploader
        task = '.get_logfile_uploader'
    else:
        return jsonify({'data': 'Could not recognize request.'})
    item_name = request.form['object_name']
    msg_text = 'Getting logfile for {}.'.format(item_name)
    cur_item = item_model.query.filter_by(name=item_name).first_or_404()
    task = cur_item.launch_task(task, _(msg_text),
                                {'running_user': current_user.id})
    db.session.commit()
    job = task.wait_and_get_job(force_return=True)
    log_list = job.result.split('\n')
    return jsonify({'data': json.dumps(log_list)})


@bp.route('/post_table', methods=['GET', 'POST'])
@login_required
def post_table():
    obj_name = request.form['object_name']
    proc_arg = {'running_user': current_user.id,
                'new_data': html.unescape(request.form['data'])}
    table_name = request.form['table']
    cur_obj = request.form['object_type']
    if cur_obj == 'Processor':
        cur_obj = Processor
    elif cur_obj == 'Plan':
        cur_obj = Plan
    else:
        cur_obj = Uploader
        proc_arg['object_level'] = request.form['object_level']
        proc_arg['uploader_type'] = request.form['uploader_type']
    if 'vendorkey' in table_name:
        split_table_name = table_name.split('vendorkey')
        table_name = split_table_name[0]
        vendor_key = split_table_name[1].replace('___', ' ')
        proc_arg['vk'] = vendor_key
    msg_text = 'Updating {} table for {}'.format(table_name, obj_name)
    cur_proc = cur_obj.query.filter_by(name=obj_name).first_or_404()
    for base_name in ['Relation', 'Uploader']:
        if base_name in table_name:
            proc_arg['parameter'] = table_name.replace(base_name, '')
            table_name = base_name
    arg_trans = {'Translate': '.write_translational_dict',
                 'Vendormatrix': '.write_vendormatrix',
                 'Constant': '.write_constant_dict',
                 'Relation': '.write_relational_config',
                 'dictionary': '.write_dictionary',
                 'rate_card': '.write_rate_card',
                 'edit_conversions': '.write_conversions',
                 'raw_data': '.write_raw_data',
                 'Uploader': '.write_uploader_file',
                 'import_config': '.write_import_config_file',
                 'raw_file_comparison': '.write_raw_file_from_tmp',
                 'get_plan_property': '.write_plan_property',
                 'change_dictionary_order': '.write_dictionary_order',
                 'billingTable': '.write_billing_table',
                 'reportBuilder': '.write_report_builder',
                 'PlanRules': '.write_plan_rules',
                 'PlanPlacements': '.write_plan_placements',
                 'Calc': '.write_plan_calc'}
    msg = '<strong>{}</strong>, {}'.format(current_user.username, msg_text)
    if table_name in ['delete_dict', 'imports', 'data_sources', 'OutputData',
                      'dictionary_order', 'modalTable']:
        msg_text = 'Saving table {} does not do anything.'.format(table_name)
        msg = '<strong>{}</strong>, {}'.format(current_user.username, msg_text)
        return jsonify({'data': 'success', 'message': msg, 'level': 'warning'})
    job_name = arg_trans[table_name]
    if job_name == '.write_dictionary':
        proc_arg['object_level'] = request.form['object_level']
    task = cur_proc.launch_task(job_name, _(msg_text), **proc_arg)
    db.session.commit()
    if table_name in ['Vendormatrix', 'Uploader']:
        task.wait_and_get_job(loops=20)
    return jsonify({'data': 'success', 'message': msg, 'level': 'success'})


def translate_table_name_to_job(table_name, proc_arg):
    for base_name in ['OutputData', 'Relation']:
        if base_name in table_name:
            proc_arg['parameter'] = table_name.replace(base_name, '')
            table_name = base_name
    if table_name in ['download_raw_data', 'download_pacing_data']:
        proc_arg['parameter'] = 'Download'
    if 'use_cache' in proc_arg and table_name != 'downloadTable':
        job_name = '.get_liquid_table_from_db'
    else:
        arg_trans = Task.get_table_name_to_task_dict()
        job_name = arg_trans[table_name]
    return job_name, table_name, proc_arg


def get_table_arguments():
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    proc_arg = {'running_user': cur_user.id}
    cur_obj = request.form['object_type']
    table_name = request.form['table']
    vk = request.form['vendorkey']
    form_dict = request.form.to_dict(flat=False)
    if form_dict['args'][0] != 'None':
        proc_arg = {**proc_arg, **json.loads(form_dict['args'][0])}
        proc_arg = utl.parse_additional_args(proc_arg)
    if cur_obj == Uploader.__name__.capitalize():
        proc_arg['parameter'] = table_name
        table_name = 'Uploader'
        proc_arg['uploader_type'] = request.form['uploader_type']
        proc_arg['object_level'] = request.form['object_level']
    elif cur_obj == Plan.__name__.capitalize():
        table_name = table_name.replace('OutputData', '')
    task_objects = [Uploader, Processor, Plan, Project]
    obj_dict = {x.__name__.capitalize(): x for x in task_objects}
    if 'proc_id' in proc_arg:
        proc_id = proc_arg.pop('proc_id')
        cur_proc = Processor.query.get(proc_id)
        cur_obj = Processor
    elif cur_obj in obj_dict and request.form['object_name'] != 'undefined':
        k = Processor.name.name
        if cur_obj == Project.__name__.capitalize():
            k = Project.project_number.name
        cur_obj = obj_dict[cur_obj]
        obj_name = request.form['object_name']
        cur_proc = cur_obj.query.filter_by(**{k: obj_name}).first_or_404()
    else:
        cur_obj = User
        cur_proc = current_user
        cur_proc.name = cur_proc.username
    if table_name == 'downloadTable':
        jn, tn, proc_arg = translate_table_name_to_job(
            table_name=vk.replace('downloadBtn', ''), proc_arg=proc_arg)
        proc_arg['function_name'] = jn
        vk = 'None'
    job_name, table_name, proc_arg = translate_table_name_to_job(
        table_name=table_name, proc_arg=proc_arg)
    if cur_obj.__table__.name == Project.__table__.name:
        proc_arg['function_name'] = job_name
        job_name = '.get_table_project'
    task_in_progress = cur_proc.get_task_in_progress(job_name)
    if task_in_progress and task_in_progress.name == table_name:
        if job_name == '.get_request_table':
            description = 'Getting {} table for {}'.format(
                table_name, request.form['fix_id'])
            if cur_proc.get_request_task_in_progress(
                    job_name, description):
                flash(_('This job: {} is already running!').format(table_name))
                return table_name, cur_proc, proc_arg, None
        else:
            flash(_('This job: {} is already running!').format(table_name))
            return table_name, cur_proc, proc_arg, None
    if request.form['fix_id'] != 'None':
        if job_name == '.apply_quick_fix':
            proc_arg['fix_id'] = request.form['fix_id']
            cur_fix = Requests.query.get(request.form['fix_id'])
            if cur_fix.fix_type in ['missing_metrics', 'unknown']:
                table_name = 'dictionary'
                proc_arg['vk'] = 'Plan Net'
                table_name = '{}vendorkey{}'.format(
                    table_name, 'Plan Net'.replace(' ', '___'))
            elif cur_fix.fix_type in ['raw_file_update', 'max_api_length',
                                      'placement_col', 'double_counting_all']:
                table_name = 'Vendormatrix'
            elif cur_fix.fix_type in ['missing_flat_costs']:
                table_name = 'Translate'
        if job_name == '.get_request_table':
            proc_arg['fix_id'] = request.form['fix_id']
            table_name = table_name + '-' + request.form['fix_id']
    if vk != 'None':
        proc_arg['vk'] = request.form['vendorkey']
        if not table_name in ['toplineMetrics']:
            table_name = '{}vendorkey{}'.format(
                table_name, request.form['vendorkey'].replace(' ', '___'))
    return table_name, cur_proc, proc_arg, job_name


def get_table_return(task, table_name, proc_arg, job_name,
                     force_return=False):
    if job_name in ['.get_processor_sources']:
        job = task.wait_and_get_job(loops=20)
        if job:
            df = pd.DataFrame([{'Result': 'DATA WAS REFRESHED.'}])
        else:
            df = pd.DataFrame([{'Result': 'DATA IS REFRESHING.'}])
    else:
        job = task.wait_and_get_job(force_return=True)
        if job and job.result:
            df = job.result[0]
        else:
            df = pd.DataFrame([{'Result': 'AN UNEXPECTED ERROR OCCURRED.'}])
    dl_table = table_name in ['SOW', 'ToplineDownload', 'downloadTable']
    dl_table_1 = (
            'parameter' in proc_arg and (
            proc_arg['parameter'] == 'RawDataOutput' or
            proc_arg['parameter'] == 'Download'))
    dl_table_2 = 'billingInvoice' in table_name
    if dl_table or dl_table_1 or dl_table_2:
        z = zipfile.ZipFile(df)
        if table_name == 'SOW' or dl_table_2:
            file_name = 'sow.pdf'
            mime_type = 'application/pdf'
        elif table_name == 'ToplineDownload':
            file_name = 'topline.xlsx'
            mime_type = 'data:application/vnd.ms-excel'
        else:
            file_name = 'raw.csv'
            mime_type = 'application/pdf'
        df = z.read(file_name)
        z.close()
        mem = io.BytesIO()
        mem.write(df)
        mem.seek(0)
        return send_file(mem, as_attachment=True,
                         download_name=file_name, mimetype=mime_type)
    if 'screenshotImage' in table_name:
        return send_file(
            io.BytesIO(df),
            download_name='screenshot.png',
            mimetype='image/png',
            as_attachment=True,
        )
    for base_name in ['Relation', 'Uploader']:
        if base_name in table_name:
            table_name = '{}{}'.format(base_name, proc_arg['parameter'])
            if 'vk' in proc_arg and job_name not in [
                '.check_processor_plan' '.apply_processor_plan']:
                table_name = '{}vendorkey{}'.format(
                    table_name, request.form['vendorkey'].replace(' ', '___'))
    table_name = "modalTable{}".format(table_name)
    if job_name in ['.get_raw_file_comparison', '.check_processor_plan']:
        data = {'data': {'data': df, 'name': table_name}}
    elif job_name == '.get_request_table':
        table_name = table_name.replace('modalTable', '')
        msg = job.result[1]
        table_data = df_to_html(df, table_name, job_name)
        data = {'html_data': table_data, 'msg': msg}
    elif job_name == '.get_daily_pacing':
        plan_cols = job.result[1]
        final_cols = plan_cols + [
            vmc.date, cal.NCF, 'Daily Spend Goal', 'Day Pacing']
        html_dfs = []
        for tmp_df in df:
            if isinstance(tmp_df, type(pd.DataFrame())):
                row_names = [str(tmp_df[x].iloc[0]) for x in plan_cols]
                row_names = ''.join(row_names)
                row_names = utl.remove_special_characters(row_names)
                final_cols = [
                    col for col in final_cols if col in tmp_df.columns]
                tmp_df = tmp_df[final_cols]
                tmp_df = df_to_html(tmp_df, row_names)
                html_dfs.append(tmp_df)
        data = {'data': {'data': html_dfs, 'plan_cols': plan_cols}}
    elif utl.LiquidTable.id_col in df and df[utl.LiquidTable.id_col]:
        data = {'data': df}
    elif 'return_func' in proc_arg and 'table_name' not in proc_arg:
        df = df.rename(columns=str)
        df = df.reset_index().to_dict(orient='records')
        data = {'data': {'data': df, 'args': proc_arg}}
    else:
        to_html = True
        cols_to_json = True
        data = df_to_html(df, table_name, job_name, to_html, cols_to_json)
        if job_name == '.get_change_dict_order':
            data['dict_cols'] = json.dumps(job.result[1])
            data['relational_cols'] = json.dumps(job.result[2])
        if job_name == '.get_processor_pacing_metrics':
            data['plan_cols'] = job.result[1]
    return jsonify(data)


@bp.route('/get_table', methods=['GET', 'POST'])
@login_required
def get_table():
    try:
        table_name, cur_proc, proc_arg, job_name = get_table_arguments()
        if not job_name:
            data = {'data': 'fail', 'task': '', 'level': 'fail'}
            return data
        if job_name == '.get_request_table':
            msg_text = 'Getting {} table for {}'.format(
                table_name, proc_arg['fix_id'])
        else:
            msg_text = 'Getting {} table for {}'.format(table_name,
                                                        cur_proc.name)
        task = cur_proc.launch_task(job_name, _(msg_text), **proc_arg)
        db.session.commit()
        if ('force_return' in request.form and
                request.form['force_return'] == 'false'):
            data = {'data': 'success', 'task': task.id, 'level': 'success'}
        else:
            data = get_table_return(task, table_name, proc_arg, job_name,
                                    force_return=True)
    except:
        args = request.form.to_dict(flat=False)
        msg = 'Unhandled exception {}'.format(json.dumps(args))
        current_app.logger.error(msg, exc_info=sys.exc_info())
        data = {'data': 'error', 'task': '', 'level': 'error',
                'args': request.form.to_dict(flat=False)}
    return data


@bp.context_processor
def utility_functions():
    def print_in_console(message):
        print(str(message))

    return dict(mdebug=print_in_console)


def df_to_html(df, name, job_name='', to_html=True, cols_to_json=True):
    pd.set_option('display.max_colwidth', None)
    set_index = True
    if 'change_dictionary_order' not in name:
        set_index = False
        df = df.reset_index()
        if 'index' in df.columns and job_name != '.get_import_config_file':
            df = df[[x for x in df.columns if x != 'index'] + ['index']]
    if to_html:
        data = df.to_html(
            index=set_index, table_id=name,
            classes='table table-striped table-responsive-sm small',
            border=0)
    else:
        data = df.to_dict(orient='records')
    cols = df.columns.tolist()
    if cols_to_json:
        cols = json.dumps(cols)
    response = {'data': {'data': data, 'cols': cols, 'name': name}}
    return response


def get_placement_form(data_source):
    form = PlacementForm()
    ds_dict = data_source.get_form_dict_with_split()
    auto_order_cols = list(
        utl.rename_duplicates(ds_dict['auto_dictionary_order']))
    ds_dict['auto_dictionary_order'] = auto_order_cols
    form.set_column_choices(data_source.id, ds_dict)
    form.full_placement_columns.data = ds_dict['full_placement_columns']
    form.placement_columns.data = ds_dict['placement_columns']
    form.auto_dictionary_placement.data = ds_dict['auto_dictionary_placement']
    form.auto_dictionary_order.data = ds_dict['auto_dictionary_order']
    form = render_template('_form.html', form=form, form_id='formPlacement')
    return form


@bp.route('/get_datasource', methods=['GET', 'POST'])
@login_required
def get_datasource():
    obj_name = request.form['object_name']
    datasource_name = request.form['datasource']
    cur_proc = Processor.query.filter_by(name=obj_name).first_or_404()
    ds = ProcessorCleanForm().set_datasources(ProcessorDatasources, cur_proc)
    ds = [x for x in ds if x['vendor_key'] == datasource_name]
    form = ProcessorCleanForm(datasources=ds)
    form.set_vendor_key_choices(current_processor_id=cur_proc.id)
    form.data_source_clean.data = datasource_name
    form = render_template('_form.html', form=form)
    dash_form = ProcessorCleanDashboardForm()
    dash_form = render_template('_form.html', form=dash_form,
                                form_id="formDash")
    ds = cur_proc.processor_datasources.filter_by(
        vendor_key=datasource_name).first()
    if not ds:
        return jsonify({'datasource_form': form,
                        'dashboard_form': dash_form,
                        'metrics_table': {},
                        'rules_table': {},
                        'placement_form': {}})
    metrics = ds.get_datasource_for_processor()['active_metrics']
    metrics = {k: ['|'.join(v)] for k, v in metrics.items()}
    df = pd.DataFrame(metrics).T.reset_index()
    df = df.rename({'index': 'Metric Name', 0: 'Metric Value'}, axis=1)
    data = df_to_html(df, 'metrics_table')
    data['select_cols'] = {}
    ds_raw_cols = get_datasource_raw_columns(obj_name, datasource_name)
    ds_raw_cols = json.dumps(ds_raw_cols)
    data['select_cols']['ds_raw_cols'] = ds_raw_cols
    vmc_cols = json.dumps(vmc.datacol)
    data['select_cols']['vmc_cols'] = vmc_cols
    rules = ds.get_datasource_for_processor()['vm_rules']
    df = pd.DataFrame(rules).T
    rules_data = df_to_html(df, 'rules_table')
    placement_form = get_placement_form(data_source=ds)
    return jsonify({'datasource_form': form,
                    'dashboard_form': dash_form,
                    'metrics_table': data,
                    'rules_table': rules_data,
                    'placement_form': placement_form})


@bp.route('/save_datasource', methods=['GET', 'POST'])
@login_required
def save_datasource():
    obj_name = request.form['object_name']
    datasource_name = request.form['datasource']
    cur_proc = Processor.query.filter_by(name=obj_name).first_or_404()
    data = request.form.to_dict()
    df = pd.read_json(html.unescape(data['metrics_table']))
    if 'index' in df.columns:
        metric_dict = df.drop('index', axis=1).to_dict(orient='index')
        metric_dict = {v['Metric Name']: [v['Metric Value']] for k, v in
                       metric_dict.items()}
    else:
        metric_dict = {}
    ds_dict = {'original_vendor_key': datasource_name,
               'vendor_key': datasource_name,
               'active_metrics': json.dumps(metric_dict),
               'vm_rules': request.form['rules_table']}
    for col in ['full_placement_columns', 'placement_columns',
                'auto_dictionary_placement', 'auto_dictionary_order']:
        new_data = utl.get_col_from_serialize_dict(data, col)
        new_data = '\r\n'.join(new_data)
        ds_dict[col] = new_data
    msg_text = ('Setting data source {} in vendormatrix for {}'
                '').format(datasource_name, obj_name)
    task = cur_proc.launch_task('.set_data_sources', _(msg_text),
                                running_user=current_user.id,
                                form_sources=[ds_dict])
    db.session.commit()
    task.wait_and_get_job(loops=20)
    return jsonify({'message': 'This data source {} was saved!'.format(
        datasource_name), 'level': 'success'})


def get_datasource_raw_columns(obj_name, datasource_name):
    cur_proc = Processor.query.filter_by(name=obj_name).first_or_404()
    ds = cur_proc.processor_datasources.filter_by(
        vendor_key=datasource_name).first()

    import processor.reporting.analyze as az
    all_analysis = ProcessorAnalysis.query.filter_by(
        processor_id=ds.processor_id, key=az.Analyze.raw_columns).first()
    if all_analysis and all_analysis.data:
        df = pd.DataFrame(all_analysis.data)
        raw_cols = df[df[vmc.vendorkey] == ds.vendor_key][
            az.Analyze.raw_columns]
    else:
        raw_cols = []
    if len(raw_cols) > 0:
        raw_cols = raw_cols[0]
    else:
        raw_cols = []
    return raw_cols


@bp.route('/processor/<object_name>/edit/clean/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_clean_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    ds = [x for x in object_form if x['name'] == 'data_source_clean']
    if ds:
        ds = ds[0]['value']
    search_dict = {'processor_id': cur_proc.id, 'vendor_key': ds}
    ds = ProcessorDatasources.query.filter_by(**search_dict).first()
    msg_text = 'Adding raw data for {}'.format(ds.vendor_key)
    cur_proc.launch_task(
        '.write_raw_data', _(msg_text),
        running_user=current_user.id, new_data=mem,
        vk=ds.vendor_key, mem_file=True, file_type=file_type)
    db.session.commit()
    return jsonify({'data': 'success', 'message': msg_text, 'level': 'success'})


@bp.route('/processor/<object_name>/edit/clean', methods=['GET', 'POST'])
@login_required
def edit_processor_clean(object_name):
    form_description = """
    Configure the properties of each data source to properly clean it.  
    Select the data source using the dropdown.  
    """
    kwargs = Processor().get_current_processor(
        object_name, 'edit_processor_clean', edit_progress=75,
        edit_name='Clean', form_title='CLEAN',
        form_description=form_description)
    cur_proc = kwargs['processor']
    form = ProcessorCleanForm()
    form.set_vendor_key_choices(current_processor_id=cur_proc.id)
    kwargs['form'] = form
    return render_template('create_processor.html', **kwargs)


def cancel_schedule(scheduled_task):
    if scheduled_task:
        if scheduled_task.id in current_app.scheduler:
            current_app.scheduler.cancel(scheduled_task.id)
        db.session.delete(scheduled_task)
        db.session.commit()


@bp.route('/processor/<object_name>/edit/export', methods=['GET', 'POST'])
@login_required
def edit_processor_export(object_name):
    form_description = """
    Configure the schedule for this processor to run.
    """
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_export',
        edit_progress=100, edit_name='Export', form_title='EXPORT',
        form_description=form_description)
    cur_proc = kwargs['processor']
    form = ProcessorExportForm()
    sched = TaskScheduler.query.filter_by(processor_id=cur_proc.id).first()
    kwargs['form'] = form
    if request.method == 'GET':
        form.tableau_workbook.data = cur_proc.tableau_workbook
        form.tableau_view.data = cur_proc.tableau_view
        form.tableau_datasource.data = cur_proc.tableau_datasource
        if sched:
            form.schedule.data = True
            form.schedule_start.data = sched.start_date
            form.schedule_end.data = sched.end_date
            form.run_time.data = sched.scheduled_time
            form.interval.data = str(sched.interval)
    elif request.method == 'POST':
        form.validate()
        cur_proc.tableau_workbook = form.tableau_workbook.data
        cur_proc.tableau_view = form.tableau_view.data
        cur_proc.tableau_datasource = form.tableau_datasource.data
        cancel_schedule(sched)
        if form.schedule.data:
            msg_text = 'Scheduling processor: {}'.format(cur_proc.name)
            cur_proc.schedule_job('.full_run_processor', msg_text,
                                  start_date=form.schedule_start.data,
                                  end_date=form.schedule_end.data,
                                  scheduled_time=form.run_time.data,
                                  interval=form.interval.data)
        if form.tableau_datasource.data:
            msg_text = 'Adding tableau export file: {}'.format(cur_proc.name)
            cur_proc.launch_task('.write_tableau_config_file', msg_text,
                                 running_user=current_user.id)
        db.session.commit()
        if form.form_continue.data == 'continue':
            next_page = 'main.edit_processor_billing'
        else:
            next_page = 'main.edit_processor_export'
        return redirect(url_for(next_page, object_name=object_name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/billing', methods=['GET', 'POST'])
@login_required
def edit_processor_billing(object_name):
    form_description = """
    Compare planned, reported and invoiced spends in one table.  
    Add Invoices to the table with total spend for best results.
    """
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_billing',
        edit_progress=100, edit_name='Bill', form_title='BILLING',
        form_description=form_description)
    form = ProcessorContinueForm()
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            next_page = 'main.processor_page'
        else:
            next_page = 'main.edit_processor_billing'
        return redirect(url_for(next_page, object_name=object_name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/billing/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_billing_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    msg_text = 'Saving billing invoice.'
    cur_proc.launch_task(
        '.write_billing_invoice', _(msg_text),
        running_user=current_user.id, new_data=mem, object_form=object_form)
    db.session.commit()
    data = 'success'
    msg = 'SUCCESS: {} invoice has been saved.'.format(cur_proc.name)
    return jsonify({'data': data, 'message': msg, 'level': data})


@bp.route('/processor/<object_name>')
@login_required
def processor_page(object_name):
    kwargs = Processor().get_current_processor(
        object_name, 'processor_page', edit_progress=100, edit_name='Page')
    if not kwargs['object'].local_path:
        return redirect(url_for('main.edit_request_processor',
                                object_name=object_name))
    if kwargs['object'].name == 'SCREENSHOT':
        form = ScreenshotForm()
        form.set_choices()
        kwargs['form'] = form
        kwargs['form_title'] = 'SCREENSHOT'
        kwargs['form_description'] = 'Collection of screenshots, click to view.'
        return render_template('screenshot.html', **kwargs)
    elif kwargs['object'].is_brandtracker():
        return render_template('brandtracker/processor_brandtracker.html',
                               **kwargs)
    else:
        return render_template('dashboards/dashboard.html', **kwargs)


@bp.route('/processor/<object_name>/popup')
@login_required
def processor_popup(object_name):
    processor_for_page = Processor.query.filter_by(
        name=object_name).first_or_404()
    return render_template('processor_popup.html', processor=processor_for_page)


@bp.route('/run_object', methods=['GET', 'POST'])
@login_required
def run_object():
    cur_obj_text = request.form['object_type']
    obj_name = request.form['object_name']
    run_type = request.form['run_type']
    if cur_obj_text == 'Processor':
        cur_obj = Processor
        task_name = '.run_processor'
        arg_trans = {
            'full': '--api all --ftp all --dbi all --exp all --tab --analyze',
            'import': '--api all --ftp all --dbi all --analyze',
            'export': '--exp all --tab --analyze',
            'basic': '--basic --analyze',
            'update': '--update all --noprocess'}
        for api in vmc.api_translation.values():
            arg_trans[api] = '--api {} --analyze'.format(api)
    else:
        cur_obj = Uploader
        task_name = '.run_uploader'
        arg_trans = {'create': '--create',
                     'campaign': '--api fb --upload c',
                     'adset': '--api fb --upload as',
                     'ad': '--api fb --upload ad'}
    run_obj = cur_obj.query.filter_by(name=obj_name).first_or_404()
    if not run_obj.local_path:
        msg = 'The {} {} has not been created, finish creating it.'.format(
            cur_obj_text, obj_name)
        lvl = 'danger'
    elif run_obj.get_task_in_progress(task_name):
        msg = 'The {} {} is already running.'.format(cur_obj_text, obj_name)
        lvl = 'warning'
    else:
        post_body = ('Running {} for {}: {}...'.format(
            run_type, cur_obj_text, obj_name))
        run_obj.launch_task(task_name, _(post_body),
                            running_user=current_user.id,
                            run_args=arg_trans[run_type.lower()])
        cur_obj.last_run_time = datetime.utcnow()
        db.session.commit()
        msg = post_body
        lvl = 'success'
    msg = '<strong>{}</strong>, {}'.format(current_user.username, msg)
    return jsonify({'data': 'success', 'message': msg, 'level': lvl})


@bp.route('/processor/<object_name>/edit', methods=['GET', 'POST'])
@login_required
def edit_processor(object_name):
    form_description = """
    General descriptive data about a processor.
    This is essentially metadata, such as processor name and client name.
    """
    kwargs = Processor().get_current_processor(
        object_name, 'edit_processor', edit_progress=25, edit_name='Basic',
        form_title='BASIC', form_description=form_description)
    processor_to_edit = kwargs['processor']
    form = EditProcessorForm(processor_to_edit.name)
    form.set_choices()
    kwargs['form'] = form
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.cur_client.data).check_and_add()
        form_product = Product(name=form.cur_product.data,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.cur_campaign.data,
                                 product_id=form_product.id).check_and_add()
        processor_to_edit.name = form.name.data
        processor_to_edit.description = form.description.data
        processor_to_edit.local_path = form.local_path.data
        processor_to_edit.tableau_workbook = form.tableau_workbook.data
        processor_to_edit.tableau_view = form.tableau_view.data
        processor_to_edit.tableau_datasource = form.tableau_datasource.data
        processor_to_edit.start_date = form.start_date.data
        processor_to_edit.end_date = form.end_date.data
        processor_to_edit.campaign_id = form_campaign.id
        db.session.commit()
        flash(_('Your changes have been saved.'))
        post_body = ('Create Processor {}...'.format(processor_to_edit.name))
        processor_to_edit.launch_task('.create_processor', _(post_body),
                                      current_user.id,
                                      current_app.config['BASE_PROCESSOR_PATH'])
        creation_text = 'Processor basic information was edited.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=processor_to_edit.id)
        db.session.add(post)
        db.session.commit()
        if form.tableau_datasource.data:
            msg_text = 'Adding tableau export file: {}'.format(
                processor_to_edit.name)
            processor_to_edit.launch_task('.write_tableau_config_file',
                                          msg_text,
                                          running_user=current_user.id)
            db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_import',
                                    object_name=processor_to_edit.name))
        else:
            return redirect(url_for('main.processor_page',
                                    object_name=processor_to_edit.name))
    elif request.method == 'GET':
        form.name.data = processor_to_edit.name
        form.description.data = processor_to_edit.description
        form.brandtracker_toggle.data = processor_to_edit.is_brandtracker()
        form.local_path.data = processor_to_edit.local_path
        form.tableau_workbook.data = processor_to_edit.tableau_workbook
        form.tableau_view.data = processor_to_edit.tableau_view
        form.tableau_datasource.data = processor_to_edit.tableau_datasource
        form.start_date.data = processor_to_edit.start_date
        form.end_date.data = processor_to_edit.end_date
        form_campaign = Campaign.query.filter_by(
            id=processor_to_edit.campaign_id).first_or_404()
        form_product = Product.query.filter_by(
            id=form_campaign.product_id).first_or_404()
        form_client = Client.query.filter_by(
            id=form_product.client_id).first_or_404()
        form.cur_campaign.data = form_campaign.name
        form.cur_product.data = form_product.name
        form.cur_client.data = form_client.name
    return render_template('create_processor.html', **kwargs)


@bp.route('/request_processor', methods=['GET', 'POST'])
@login_required
def request_processor():
    form = ProcessorRequestForm()
    form.set_choices()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.cur_client.data).check_and_add()
        form_product = Product(
            name=form.cur_product.data,
            client_id=form_client.id).check_and_add()
        form_campaign = Campaign(
            name=form.cur_campaign.data,
            product_id=form_product.id).check_and_add()
        new_processor = Processor(
            name=form.name.data, description=form.description.data,
            requesting_user_id=current_user.id, plan_path=form.plan_path.data,
            start_date=form.start_date.data, end_date=form.end_date.data,
            first_report_=form.first_report.data,
            campaign_id=form_campaign.id, user_id=current_user.id)
        db.session.add(new_processor)
        db.session.commit()
        creation_text = 'Processor was requested for creation.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=new_processor.id)
        db.session.add(post)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_account',
                                    object_name=new_processor.name))
        else:
            return redirect(url_for('main.processor'))
    buttons = Processor().get_navigation_buttons('ProcessorRequest')
    return render_template('create_processor.html', user=cur_user,
                           title=_('Processor'), form=form, edit_progress="25",
                           edit_name='Basic', buttons=buttons)


@bp.route('/processor/<object_name>/edit/request', methods=['GET', 'POST'])
@login_required
def edit_request_processor(object_name):
    form_description = """
    General descriptive data about a processor.
    This is essentially metadata, such as processor name and client name.
    """
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_request_processor', edit_progress=25,
        edit_name='Basic', buttons='ProcessorRequest', form_title='BASIC',
        form_description=form_description)
    processor_to_edit = kwargs['processor']
    form = EditProcessorRequestForm(processor_to_edit.name)
    form.set_choices()
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.cur_client.data).check_and_add()
        form_product = Product(name=form.cur_product.data,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.cur_campaign.data,
                                 product_id=form_product.id).check_and_add()
        processor_to_edit.name = form.name.data
        processor_to_edit.description = form.description.data
        processor_to_edit.plan_path = form.plan_path.data
        processor_to_edit.start_date = form.start_date.data
        processor_to_edit.end_date = form.end_date.data
        processor_to_edit.first_report_ = form.first_report.data
        processor_to_edit.campaign_id = form_campaign.id
        db.session.commit()
        creation_text = 'Processor request was edited.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=processor_to_edit.id)
        db.session.add(post)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_plan',
                                    object_name=processor_to_edit.name))
        else:
            return redirect(url_for('main.edit_request_processor',
                                    object_name=processor_to_edit.name))
    elif request.method == 'GET':
        form.name.data = processor_to_edit.name
        form.brandtracker_toggle.data = processor_to_edit.is_brandtracker()
        form.description.data = processor_to_edit.description
        form.plan_path.data = processor_to_edit.plan_path
        form.start_date.data = processor_to_edit.start_date
        form.end_date.data = processor_to_edit.end_date
        form.first_report.data = processor_to_edit.first_report_
        form_campaign = Campaign.query.filter_by(
            id=processor_to_edit.campaign_id).first_or_404()
        form_product = Product.query.filter_by(
            id=form_campaign.product_id).first_or_404()
        form_client = Client.query.filter_by(
            id=form_product.client_id).first_or_404()
        form.cur_campaign.data = form_campaign.name
        form.cur_product.data = form_product.name
        form.cur_client.data = form_client.name
    kwargs['form'] = form
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/plan/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_plan_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    plan_saved = utl.check_and_add_media_plan(
        mem, cur_proc, object_type=Processor, current_user=current_user)
    if plan_saved:
        data = 'success'
        msg = 'SUCCESS: {} plan has been saved.'.format(cur_proc.name)
    else:
        data = 'danger'
        msg = "Plan not saved - is 'Media Plan' the name of a sheet?"
        msg = 'FAILED: {}. {}'.format(cur_proc.name, msg)
    return jsonify({'data': data, 'message': msg, 'level': data})


@bp.route('/processor/<object_name>/edit/plan_normal/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_plan_normal_upload_file(object_name):
    return edit_processor_plan_upload_file(object_name)


@bp.route('/processor/<object_name>/edit/plan')
@login_required
def edit_processor_plan(object_name):
    kwargs = Processor().get_plan_kwargs(
        object_name, request_flow=True, cur_form=ProcessorPlanForm())
    form = kwargs['form']
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            next_page = 'main.edit_processor_account'
        else:
            next_page = 'main.edit_processor_plan'
        return redirect(url_for(next_page, object_name=object_name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/plan_normal')
@login_required
def edit_processor_plan_normal(object_name):
    kwargs = Processor().get_plan_kwargs(
        object_name, request_flow=False, cur_form=ProcessorPlanForm())
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/accounts', methods=['GET', 'POST'])
@login_required
def edit_processor_account(object_name):
    form_description = """
    The accounts to create an API for so the processor can automatically pull 
    data.
    Add an account with 'Add Account' and fill in the card.
    The values for each Account Type differ.
    You can view exactly what to place in each by clicking the Question Mark
    on the left hand side of your screen.  
    Then select 'How do I add a new API to the processor?'
    """
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_account',
        edit_progress=50, edit_name='Accounts', buttons='ProcessorRequest',
        form_title='ACCOUNTS', form_description=form_description)
    cur_proc = kwargs['processor']
    accounts = GeneralAccountForm().set_accounts(Account, cur_proc)
    form = GeneralAccountForm(accounts=accounts)
    kwargs['form'] = form
    if form.add_child.data:
        form.accounts.append_entry()
        kwargs['form'] = form
        return render_template('create_processor.html', **kwargs)
    for act in form.accounts:
        if act.delete.data:
            kwargs = dict(
                processor_id=cur_proc.id,
                key=act.key.data,
                account_id=act.account_id.data,
                campaign_id=act.campaign_id.data)
            act = Account.query.filter_by(**kwargs).first()
            if not act:
                kwargs = {k: v if v else None for k, v in kwargs.items()}
                act = Account.query.filter_by(**kwargs).first()
            if act:
                db.session.delete(act)
                db.session.commit()
            return redirect(url_for('main.edit_processor_account',
                                    object_name=cur_proc.name))
    if request.method == 'POST':
        msg_text = 'Setting accounts for {}'.format(cur_proc.name)
        task = cur_proc.launch_task(
            '.set_processor_accounts', _(msg_text),
            running_user=current_user.id, form_sources=form.accounts.data)
        db.session.commit()
        task.wait_and_get_job()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_fees',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_account',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/fees', methods=['GET', 'POST'])
@login_required
def edit_processor_fees(object_name):
    form_description = """
    Configure the adserving, reporting and agency fees used by the processor.
    Adserving rates by type can be edited and saved using 'View Rate Card'.
    Old rate cards can be selected and used from the dropdown.
    Note Digital and Traditional Agency Fees should be provided as decimal.
    """
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_fees', edit_progress=75,
        edit_name='Fees', buttons='ProcessorRequest',
        form_title='FEES', form_description=form_description)
    cur_proc = kwargs['processor']
    form = FeeForm()
    if request.method == 'POST':
        form.validate()
        cur_proc.digital_agency_fees = form.digital_agency_fees.data
        cur_proc.trad_agency_fees = form.trad_agency_fees.data
        cur_proc.rate_card_id = form.rate_card.data.id
        cur_proc.dcm_service_fees = int(
            form.dcm_service_fees.data.replace('%', '')) / 100
        db.session.commit()
        creation_text = 'Processor fees were edited.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=cur_proc.id)
        db.session.add(post)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_conversions',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_fees',
                                    object_name=cur_proc.name))
    elif request.method == 'GET':
        form.digital_agency_fees.data = cur_proc.digital_agency_fees
        form.trad_agency_fees.data = cur_proc.trad_agency_fees
        form_rate_card = RateCard.query.filter_by(
            id=cur_proc.rate_card_id).first()
        form.rate_card.data = form_rate_card
        if cur_proc.dcm_service_fees:
            dcm_fee = '{}%'.format(round(cur_proc.dcm_service_fees * 100))
        else:
            dcm_fee = '0%'
        form.dcm_service_fees.data = dcm_fee
    kwargs['form'] = form
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/conversions',
          methods=['GET', 'POST'])
@login_required
def edit_processor_conversions(object_name):
    form_description = """
    The conversions for this campaign.  This page maps conversions to 
    the name they will appear in the processor output.  
    """
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_conversions',
        edit_progress=75, edit_name='Conversions', buttons='ProcessorRequest',
        form_title='CONVERSIONS', form_description=form_description)
    cur_proc = kwargs['processor']
    conversions = GeneralConversionForm().set_conversions(Conversion, cur_proc)
    form = GeneralConversionForm(conversions=conversions)
    kwargs['form'] = form
    if form.add_child.data:
        form.conversions.append_entry()
        kwargs['form'] = form
        return render_template('create_processor.html', **kwargs)
    for conv in form.conversions:
        if conv.delete.data:
            conv = Conversion.query.filter_by(
                key=conv.key.data, conversion_name=conv.conversion_name.data,
                conversion_type=conv.conversion_type.data,
                dcm_category=conv.dcm_category.data,
                processor_id=cur_proc.id).first()
            if conv:
                db.session.delete(conv)
                db.session.commit()
            return redirect(url_for('main.edit_processor_conversions',
                                    object_name=cur_proc.name))
    if request.method == 'POST':
        msg_text = 'Setting conversions for {}'.format(cur_proc.name)
        task = cur_proc.launch_task(
            '.set_processor_conversions', _(msg_text),
            running_user=current_user.id, form_sources=form.conversions.data)
        db.session.commit()
        task.wait_and_get_job()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_finish',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_conversions',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/finish',
          methods=['GET', 'POST'])
@login_required
def edit_processor_finish(object_name):
    form_description = """
    The final step in processor creation.  Add relevant people to the processor
    to receive notifications on the data.  Note you must press 
    'Save & Continue' on this tab to finish the request and build the processor.
    """
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_finish', edit_progress=100,
        edit_name='Finish', buttons='ProcessorRequest', form_title='FINISH',
        form_description=form_description)
    cur_proc = kwargs['processor']
    form = ProcessorRequestFinishForm()
    form.set_user_choices()
    if request.method == 'GET':
        form.add_current_users(Processor, cur_proc)
        kwargs['form'] = form
    elif request.method == 'POST':
        form.validate()
        for usr in form.followers.data:
            u = User.query.filter_by(username=usr).first()
            if u:
                u.follow_processor(cur_proc)
                db.session.commit()
        if form.owner.data:
            cur_proc.user_id = form.owner.data.id
        else:
            cur_proc.user_id = current_user.id
        db.session.commit()
        if form.form_continue.data == 'continue':
            msg_text = 'Sending request and attempting to build processor: {}' \
                       ''.format(cur_proc.name)
            cur_proc.launch_task(
                '.build_processor_from_request', _(msg_text),
                running_user=current_user.id)
            db.session.commit()
            return redirect(url_for('main.edit_processor',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_finish',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/fix',
          methods=['GET', 'POST'])
@login_required
def edit_processor_request_fix(object_name):
    kwargs = Processor().get_current_processor(
        object_name=object_name, current_page='edit_processor_request_fix',
        edit_progress=33, edit_name='New Fix', buttons='ProcessorRequestFix')
    cur_proc = kwargs['processor']
    form = ProcessorFixForm()
    form.set_vendor_key_choices(current_processor_id=cur_proc.id)
    kwargs['form'] = form
    if request.method == 'POST':
        new_processor_request = Requests(
            processor_id=cur_proc.id, fix_type=form.fix_type.data,
            column_name=form.cname.data,
            wrong_value=form.wrong_value.data,
            correct_value=form.correct_value.data,
            filter_column_name=form.filter_column_name.data,
            filter_column_value=form.filter_column_value.data,
            fix_description=form.fix_description.data
        )
        db.session.add(new_processor_request)
        db.session.commit()
        creation_text = "Processor {} fix request {} was created".format(
            cur_proc.name, new_processor_request.id)
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=cur_proc.id,
                    request_id=new_processor_request.id)
        db.session.add(post)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_submit_fix',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_request_fix',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/fix/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_request_fix_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
    fix_type = [x['value'] for x in object_form if x['name'] == 'fix_type'][0]
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    msg_text = ''
    if fix_type == 'New File':
        new_name = file_name
        new_name = new_name.replace('.csv', '')
        msg_text = 'Adding new raw_data data for {}'.format(new_name)
        cur_proc.launch_task(
            '.write_raw_data', _(msg_text),
            running_user=current_user.id, new_data=mem,
            vk=None, mem_file=True, new_name=new_name, file_type=file_type)
    elif fix_type == 'Upload File':
        vk = [x['value'] for x in object_form if x['name'] == 'data_source'][0]
        ds = ProcessorDatasources.query.filter_by(vendor_key=vk).first()
        msg_text = 'Adding raw data for {}'.format(ds.vendor_key)
        cur_proc.launch_task(
            '.write_raw_data', _(msg_text),
            running_user=current_user.id, new_data=mem,
            vk=ds.vendor_key, mem_file=True, file_type=file_type)
    elif fix_type == 'Update Plan':
        utl.check_and_add_media_plan(mem, cur_proc, current_user=current_user)
    elif fix_type == 'Spend Cap':
        msg_text = 'Adding new spend cap'
        cur_proc.launch_task(
            '.save_spend_cap_file', _(msg_text),
            running_user=current_user.id, new_data=mem)
    db.session.commit()
    return jsonify({'data': 'success', 'message': msg_text, 'level': 'success'})


@bp.route('/processor/<object_name>/edit/fix/submit',
          methods=['GET', 'POST'])
@login_required
def edit_processor_submit_fix(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_submit_fix',
        edit_progress=66, edit_name='Submit Fixes',
        buttons='ProcessorRequestFix')
    cur_proc = kwargs['processor']
    fixes = cur_proc.get_open_requests()
    form = ProcessorContinueForm()
    kwargs['fixes'] = fixes
    kwargs['form'] = form
    if request.method == 'POST':
        creation_text = ("Submitted {} fix request(s) for "
                         "processor {}").format(len(fixes), cur_proc.name)
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=cur_proc.id)
        db.session.add(post)
        db.session.commit()
        msg_text = ('Attempting to fix requests and notifying followers '
                    'for processor: {}').format(cur_proc.name)
        cur_proc.launch_task(
            '.processor_fix_requests', _(msg_text),
            running_user=current_user.id)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_all_fix',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_submit_fix',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/fix/all',
          methods=['GET', 'POST'])
@login_required
def edit_processor_all_fix(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_all_fix',
        edit_progress=100, edit_name='All Fixes', buttons='ProcessorRequestFix')
    cur_proc = kwargs['processor']
    fixes = cur_proc.get_all_requests()
    form = ProcessorContinueForm()
    kwargs['fixes'] = fixes
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.processor_page',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_all_fix',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/resolve_fix/<fix_id>')
@login_required
def resolve_fix(fix_id):
    request_fix = Requests.query.get(fix_id)
    if request_fix is None:
        flash(_('Request #{} not found.'.format(request_fix)))
        return redirect(url_for('main.index'))
    msg_txt = 'The fix #{} has been marked as resolved!'.format(request_fix.id)
    request_fix.mark_resolved()
    post = Post(body=msg_txt, author=current_user,
                processor_id=request_fix.processor.id,
                request_id=fix_id)
    db.session.add(post)
    db.session.commit()
    flash(_(msg_txt.format(request_fix.id)))
    return redirect(url_for('main.edit_processor_view_fix',
                            object_name=request_fix.processor.name,
                            fix_id=request_fix.id))


@bp.route('/unresolve_fix/<fix_id>')
@login_required
def unresolve_fix(fix_id):
    request_fix = Requests.query.get(fix_id)
    if request_fix is None:
        flash(_('Request #{} not found.'.format(request_fix)))
        return redirect(url_for('main.index'))
    msg_txt = 'The fix #{} has been marked as unresolved!'.format(
        request_fix.id)
    request_fix.mark_unresolved()
    post = Post(body=msg_txt, author=current_user,
                processor_id=request_fix.processor.id,
                request_id=fix_id)
    db.session.add(post)
    db.session.commit()
    flash(_(msg_txt))
    return redirect(url_for('main.edit_processor_view_fix',
                            object_name=request_fix.processor.name,
                            fix_id=request_fix.id))


@bp.route('/processor/<object_name>/edit/fix/<fix_id>',
          methods=['GET', 'POST'])
@login_required
def edit_processor_view_fix(object_name, fix_id):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_view_fix', edit_progress=100,
        edit_name='View Fixes', buttons='ProcessorRequestFix', fix_id=fix_id)
    cur_proc = kwargs['processor']
    fixes = Requests.query.filter_by(id=fix_id).all()
    form = ProcessorRequestCommentForm()
    kwargs['fixes'] = fixes
    kwargs['form'] = form
    if request.method == 'POST':
        if form.post.data:
            language = guess_language(form.post.data)
            if language == 'UNKNOWN' or len(language) > 5:
                language = ''
            post = Post(body=form.post.data, author=current_user,
                        language=language, processor_id=cur_proc.id,
                        request_id=fix_id)
            db.session.add(post)
            db.session.commit()
            flash(_('Your post is now live!'))
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_submit_fix',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_view_fix',
                                    object_name=cur_proc.name,
                                    fix_id=fix_id))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/note',
          methods=['GET', 'POST'])
@login_required
def edit_processor_note(object_name):
    kwargs = Processor().get_current_processor(
        object_name=object_name, current_page='edit_processor_note',
        edit_progress=33, edit_name='New Note', buttons='ProcessorNote')
    cur_proc = kwargs['processor']
    form = ProcessorNoteForm()
    kwargs['form'] = form
    if request.method == 'POST':
        new_note = Notes(
            processor_id=cur_proc.id, user_id=current_user.id,
            note_text=form.note_text.data, notification=form.notification.data,
            notification_day=form.notification_day.data,
            vendor=form.vendor.data, country=form.country.data,
            environment=form.environment.data,
            kpi=form.kpi.data, created_at=datetime.utcnow(),
            start_date=form.start_date.data, end_date=form.end_date.data,
            dimensions=form.dimensions.data)
        db.session.add(new_note)
        db.session.commit()
        creation_text = "Processor {} note {} was created".format(
            cur_proc.name, new_note.id)
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=cur_proc.id, note_id=new_note.id)
        db.session.add(post)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_all_notes',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_note',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/note/all',
          methods=['GET', 'POST'])
@login_required
def edit_processor_all_notes(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_all_notes', edit_progress=100,
        edit_name='All Notes', buttons='ProcessorNote')
    cur_proc = kwargs['processor']
    all_notes = cur_proc.get_notes()
    form = ProcessorContinueForm()
    kwargs['notes'] = all_notes
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.processor_page',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_all_notes',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/edit/note/<note_id>',
          methods=['GET', 'POST'])
@login_required
def edit_processor_view_note(object_name, note_id):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_note_fix', edit_progress=100,
        edit_name='View Notes', buttons='ProcessorNote', note_id=note_id)
    cur_proc = kwargs['processor']
    all_notes = Notes.query.filter_by(id=note_id).all()
    form = ProcessorRequestCommentForm()
    kwargs['notes'] = all_notes
    kwargs['form'] = form
    if request.method == 'POST':
        if form.post.data:
            language = guess_language(form.post.data)
            if language == 'UNKNOWN' or len(language) > 5:
                language = ''
            post = Post(body=form.post.data, author=current_user,
                        language=language, processor_id=cur_proc.id,
                        note_id=note_id)
            db.session.add(post)
            db.session.commit()
            flash(_('Your post is now live!'))
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_all_notes',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_view_note',
                                    object_name=cur_proc.name,
                                    note_id=note_id))
    return render_template('create_processor.html', **kwargs)


@bp.route('/close_note/<note_id>')
@login_required
def close_note(note_id):
    cur_note = Notes.query.get(note_id)
    if cur_note is None:
        flash(_('Note #{} not found.'.format(cur_note)))
        return redirect(url_for('main.explore'))
    msg_txt = 'The note #{} has been closed!'.format(cur_note.id)
    cur_note.mark_resolved()
    post = Post(body=msg_txt, author=current_user,
                processor_id=cur_note.processor.id, note_id=note_id)
    db.session.add(post)
    db.session.commit()
    flash(_(msg_txt.format(cur_note.id)))
    return redirect(url_for('main.edit_processor_view_note',
                            object_name=cur_note.processor.name,
                            note_id=cur_note.id))


@bp.route('/open_note/<note_id>')
@login_required
def open_note(note_id):
    cur_note = Notes.query.get(note_id)
    if cur_note is None:
        flash(_('Note #{} not found.'.format(cur_note)))
        return redirect(url_for('main.explore'))
    msg_txt = 'The note #{} has been opened!'.format(cur_note.id)
    cur_note.mark_unresolved()
    post = Post(body=msg_txt, author=current_user,
                processor_id=cur_note.processor.id, note_id=note_id)
    db.session.add(post)
    db.session.commit()
    flash(_(msg_txt))
    return redirect(url_for('main.edit_processor_view_note',
                            object_name=cur_note.processor.name,
                            note_id=cur_note.id))


@bp.route('/processor/<object_name>/edit/note/auto',
          methods=['GET', 'POST'])
@login_required
def edit_processor_auto_notes(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_auto_notes',
        edit_progress=100, edit_name='Automatic Notes',
        buttons='ProcessorNote')
    cur_proc = kwargs['processor']
    form = ProcessorAutoAnalysisForm()
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.processor_page',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_all_notes',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/get_auto_note', methods=['GET', 'POST'])
@login_required
def get_auto_note():
    obj_name = request.form['object_name']
    cur_proc = Processor.query.filter_by(name=obj_name).first_or_404()
    note_name = request.form['note_name']
    sub_note_name = request.form['sub_note_name']
    form = ProcessorAutoAnalysisForm()
    form = render_template('_form.html', form=form)
    raw_html = ''
    button_dict = {}
    analysis = None
    import processor.reporting.analyze as az
    if note_name == 'Topline':
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.topline_col,
            parameter=az.Analyze.topline_col).first()
        buttons = ['Total', 'Two Week', 'Last Week']
        button_dict = [{'name': x, 'active': x == sub_note_name}
                       for x in buttons]
    elif note_name == 'All':
        task = cur_proc.launch_task(
            '.build_processor_analysis_email', _('Getting processor analysis.'),
            running_user=current_user.id)
        db.session.commit()
        job = task.wait_and_get_job(loops=20)
        job_result = job.result[0]
        raw_html = render_template('processor_auto_analysis.html',
                                   analysis=job_result)
    if analysis and analysis.data:
        df = pd.DataFrame(analysis.data).T
    else:
        df = pd.DataFrame()
    table_data = df_to_html(df, 'datasource_table')
    raw_data = df[df.columns].replace({'\\$': '', ',': ''}, regex=True)
    raw_data = raw_data.reset_index().to_dict(orient='records')
    if analysis:
        analysis = analysis.message.capitalize()
    return jsonify({'data': table_data,
                    'raw_data': raw_data,
                    'raw_data_columns': df.columns.tolist(),
                    'auto_note_form': form,
                    'buttons': button_dict,
                    'data_message': analysis,
                    'raw_html': raw_html})


@bp.route('/get_report_builder', methods=['GET', 'POST'])
@login_required
def get_report_builder():
    obj_name = request.form['object_name']
    cur_proc = Processor.query.filter_by(name=obj_name).first_or_404()
    report_name = request.form['name']
    report_date = request.form['date']
    processor_reports = ProcessorReports.query.filter_by(
        processor_id=cur_proc.id, report_date=report_date).all()
    report_names = [x.report_name for x in processor_reports]
    processor_report = ProcessorReports.query.filter_by(
        processor_id=cur_proc.id, report_name=report_name,
        report_date=report_date).first()
    if processor_report is None:
        processor_report = ProcessorReports.query.filter_by(
            processor_id=cur_proc.id, report_name='Auto',
            report_date=report_date).first()
    report = json.loads(processor_report.report)
    return jsonify({'data': report, 'report_names': report_names})


@bp.route('/processor/<object_name>/edit/report_builder',
          methods=['GET', 'POST'])
@login_required
def edit_processor_report_builder(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_report_builder',
        edit_progress=100, edit_name='Report Builder',
        buttons='ProcessorNote')
    cur_proc = kwargs['processor']
    today = datetime.utcnow().date()
    processor_reports = ProcessorReports.query.filter_by(
        processor_id=cur_proc.id, report_date=today).all()
    if not processor_reports:
        processor_reports = ProcessorReports.query.filter_by(
            processor_id=cur_proc.id).order_by(
            ProcessorReports.report_date.desc()).first()
        processor_reports = ProcessorReports.query.filter_by(
            processor_id=cur_proc.id, report_date=processor_reports.report_date
        ).all()
    default = ('Auto' if 'Auto' in [x.report_name for x in processor_reports]
               else processor_reports[0].report_name)
    form = ProcessorReportBuilderForm(
        names=[(x.report_name, x.report_name) for x in processor_reports],
        default_name=default, default_date=processor_reports[0].report_date)
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.processor_page',
                                    object_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_report_builder',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader')
@login_required
def uploader():
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    page = request.args.get('page', 1, type=int)
    current_clients = Client.query.order_by(Client.name)
    uploaders = current_user.uploader.order_by(
        Uploader.last_run_time.desc()).paginate(
        page=page, per_page=current_app.config['POSTS_PER_PAGE'],
        error_out=False)
    next_url = (url_for('main.upload', username=cur_user.username,
                        page=uploaders.next_num)
                if uploaders.has_next else None)
    prev_url = (url_for('main.upload', username=cur_user.username,
                        page=uploaders.prev_num)
                if uploaders.has_prev else None)
    return render_template('uploader.html', title=_('Uploader'),
                           user=cur_user, processors=uploaders.items,
                           uploaders=uploaders.items,
                           next_url=next_url, prev_url=prev_url,
                           clients=current_clients)


@bp.route('/create_uploader', methods=['GET', 'POST'])
@login_required
def create_uploader():
    form = UploaderForm()
    form.set_choices()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.cur_client.data).check_and_add()
        form_product = Product(name=form.cur_product.data,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.cur_campaign.data,
                                 product_id=form_product.id).check_and_add()
        new_uploader = Uploader(
            name=form.name.data, description=form.description.data,
            user_id=current_user.id, created_at=datetime.utcnow(),
            campaign_id=form_campaign.id,
            fb_account_id=form.fb_account_id.data,
            aw_account_id=form.aw_account_id.data,
            dcm_account_id=form.dcm_account_id.data
        )
        db.session.add(new_uploader)
        db.session.commit()
        new_path = app_utl.create_local_path(new_uploader)
        new_uploader.local_path = new_path
        db.session.commit()
        new_uploader.create_object(form.media_plan.data)
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_uploader_campaign',
                                    object_name=new_uploader.name))
        else:
            return redirect(url_for('main.edit_uploader',
                                    object_name=new_uploader.name))
    buttons = Processor().get_navigation_buttons('UploaderFacebook')
    return render_template('create_processor.html', user=cur_user,
                           title=_('Uploader'), form=form, edit_progress="25",
                           edit_name='Basic', buttons=buttons)


def get_uploader_run_links():
    run_links = {}
    for idx, run_arg in enumerate(
            ('create', 'campaign', 'adset', 'ad')):
        run_link = dict(title=run_arg.capitalize())
        run_links[idx] = run_link
    return run_links


def get_uploader_edit_links():
    edit_links = {}
    for idx, edit_file in enumerate(
            ('Creator', 'Campaign', 'Adset', 'Ad')):
        edit_links[idx] = dict(title=edit_file, nest=[])
    return edit_links


def get_uploader_request_links(object_name):
    req_links = {
        0: {'title': 'Request Duplication',
            'href': url_for('main.edit_uploader_duplication',
                            object_name=object_name)}
    }
    return req_links


def get_uploader_view_selector(uploader_type='Facebook'):
    view_selector = [{'view': 'Facebook', 'active': False,
                      'value': 'main.edit_uploader_campaign'},
                     {'view': 'DCM', 'active': False,
                      'value': 'main.edit_uploader_campaign_dcm'},
                     {'view': 'Adwords', 'active': False,
                      'value': 'main.edit_uploader_campaign_aw'}]
    for view in view_selector:
        if view['view'] == uploader_type:
            view['active'] = True
    return view_selector


def get_current_uploader(object_name, current_page, edit_progress=0,
                         edit_name='Page', buttons='Uploader', fix_id=None,
                         uploader_type='Facebook'):
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    posts, next_url, prev_url = Post().get_posts_for_objects(
        cur_obj=cur_up, fix_id=fix_id, current_page=current_page,
        object_name='uploader')
    run_links = get_uploader_run_links()
    edit_links = get_uploader_edit_links()
    request_links = get_uploader_request_links(cur_up.name)
    view_selector = get_uploader_view_selector(uploader_type)
    nav_buttons = Processor().get_navigation_buttons(buttons + uploader_type)
    args = {'object': cur_up, 'posts': posts.items, 'title': _('Uploader'),
            'object_name': cur_up.name, 'user': cur_user,
            'object_id': cur_up.id, 'edit_progress': edit_progress,
            'edit_name': edit_name, 'buttons': nav_buttons,
            'object_function_call': {'object_name': cur_up.name},
            'run_links': run_links, 'edit_links': edit_links,
            'request_links': request_links,
            'next_url': next_url, 'prev_url': prev_url,
            'view_selector': view_selector,
            'uploader_type': uploader_type}
    return args


@bp.route('/uploader/<object_name>')
@login_required
def uploader_page(object_name):
    kwargs = get_current_uploader(object_name, 'uploader_page',
                                  edit_progress=100, edit_name='Page')
    kwargs['uploader_objects'] = kwargs['object'].uploader_objects
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader/<object_name>/edit/upload_file',
          methods=['GET', 'POST'])
@login_required
@error_handler
def edit_uploader_upload_file(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    utl.check_and_add_media_plan(mem, cur_up, object_type=Uploader,
                                 current_user=current_user)
    cur_up.media_plan = True
    db.session.commit()
    msg = 'File was saved.'
    return jsonify({'data': 'success', 'message': msg, 'level': 'success'})


@bp.route('/uploader/<object_name>/edit', methods=['GET', 'POST'])
@login_required
def edit_uploader(object_name):
    kwargs = get_current_uploader(object_name, 'edit_uploader',
                                  edit_progress=20, edit_name='Basic')
    uploader_to_edit = kwargs['object']
    form = EditUploaderForm(object_name)
    form.set_choices()
    kwargs['form'] = form
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.cur_client.data).check_and_add()
        form_product = Product(name=form.cur_product.data,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.cur_campaign.data,
                                 product_id=form_product.id).check_and_add()
        uploader_to_edit.name = form.name.data
        uploader_to_edit.description = form.description.data
        uploader_to_edit.campaign_id = form_campaign.id
        uploader_to_edit.fb_account_id = form.fb_account_id.data
        uploader_to_edit.aw_account_id = form.aw_account_id.data
        uploader_to_edit.dcm_account_id = form.dcm_account_id.data
        db.session.commit()
        uploader_to_edit.create_object(None)
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_uploader_campaign',
                                    object_name=uploader_to_edit.name))
        else:
            return redirect(url_for('main.edit_uploader',
                                    object_name=uploader_to_edit.name))
    elif request.method == 'GET':
        form.name.data = uploader_to_edit.name
        form.description.data = uploader_to_edit.description
        form_campaign = Campaign.query.filter_by(
            id=uploader_to_edit.campaign_id).first_or_404()
        form_product = Product.query.filter_by(
            id=form_campaign.product_id).first_or_404()
        form_client = Client.query.filter_by(
            id=form_product.client_id).first_or_404()
        form.cur_campaign.data = form_campaign.name
        form.cur_product.data = form_product.name
        form.cur_client.data = form_client.name
        form.fb_account_id.data = uploader_to_edit.fb_account_id
        form.aw_account_id.data = uploader_to_edit.aw_account_id
        form.dcm_account_id.data = uploader_to_edit.dcm_account_id
    return render_template('create_processor.html', **kwargs)


def set_uploader_relations_in_db(uploader_id, form_relations,
                                 object_level='Campaign',
                                 uploader_type='Facebook'):
    cur_up = Uploader.query.get(uploader_id)
    up_obj = UploaderObjects.query.filter_by(
        uploader_id=cur_up.id, object_level=object_level,
        uploader_type=uploader_type).first()
    for rel in form_relations:
        up_rel = UploaderRelations.query.filter_by(
            uploader_objects_id=up_obj.id,
            impacted_column_name=rel['impacted_column_name']).first()
        up_rel.relation_constant = rel['relation_constant']
        up_rel.position = rel['position']
        db.session.commit()
    return True


def uploader_name_file_upload(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    up_obj = UploaderObjects.query.filter_by(
        uploader_id=cur_up.id, object_level=object_level).first()
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    if current_key == 'name_creator':
        if up_obj.name_create_type == 'Match Table':
            current_key = 'match_table'
    msg_text = 'Saving file {} for {}'.format(file_name, cur_up.name)
    cur_up.launch_task(
        '.write_uploader_file', _(msg_text),
        running_user=current_user.id, new_data=mem,
        parameter=current_key, mem_file=True,
        object_level=object_level)
    db.session.commit()
    return jsonify({'data': 'success', 'message': msg_text, 'level': 'success'})


@bp.route('/uploader/<object_name>/edit/campaign/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_campaign_name_file_upload(object_name):
    return uploader_name_file_upload(object_name)


@bp.route('/uploader/<object_name>/edit/adset/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_adset_name_file_upload(object_name):
    return uploader_name_file_upload(object_name)


@bp.route('/uploader/<object_name>/edit/ad/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_ad_name_file_upload(object_name):
    return uploader_name_file_upload(object_name)


@bp.route('/get_uploader_relation_position', methods=['GET', 'POST'])
@login_required
def get_uploader_relation_position():
    object_name = request.form['object_name']
    object_level = request.form['object_level']
    uploader_type = request.form['uploader_type']
    column_name = request.form['column_name']
    cur_up = Uploader.query.filter_by(name=object_name).first()
    up_obj = UploaderObjects.query.filter_by(
        uploader_id=cur_up.id, object_level=object_level,
        uploader_type=uploader_type).first()
    rel = up_obj.uploader_relations.filter_by(
        impacted_column_name=column_name).first()
    pos_list = rel.position
    response = {'position': pos_list, 'column_name': column_name}
    return jsonify(response)


def edit_uploader_base_objects(object_name, object_level, next_level='Page',
                               uploader_type='Facebook'):
    kwargs = get_current_uploader(object_name, 'edit_uploader',
                                  edit_progress=40, edit_name=object_level,
                                  uploader_type=uploader_type)
    cur_up = kwargs['object']
    up_obj = UploaderObjects.query.filter_by(
        uploader_id=cur_up.id, object_level=object_level,
        uploader_type=uploader_type).first()
    relations = EditUploaderMediaPlanForm.set_relations(
        data_source=UploaderRelations, cur_upo=up_obj)
    if up_obj.name_create_type == 'Media Plan':
        form_object = EditUploaderMediaPlanForm
        form = form_object(relations=relations)
        if request.method == 'POST':
            form.validate()
            up_obj.media_plan_columns = form.media_plan_columns.data
            up_obj.partner_filter = form.partner_name_filter.data
            up_obj.name_create_type = form.name_create_type.data
            db.session.commit()
        elif request.method == 'GET':
            plan_choices = up_obj.string_to_list(up_obj.media_plan_columns)
            plan_choices = [(x, x) for x in plan_choices]
            plan_choices += form.media_plan_columns.choices
            form.media_plan_columns.choices = plan_choices
            form.name_create_type.data = up_obj.name_create_type
            form.media_plan_columns.data = up_obj.media_plan_columns
            form.partner_name_filter.data = up_obj.partner_filter
    else:
        form_object = EditUploaderNameCreateForm
        form = form_object(relations=relations)
        if request.method == 'POST':
            form.validate()
            up_obj.name_create_type = form.name_create_type.data
            up_obj.duplication_type = form.duplication_type.data
            db.session.commit()
        if request.method == 'GET':
            form.name_create_type.data = up_obj.name_create_type
            form.duplication_type.data = up_obj.duplication_type
    if request.method == 'POST':
        if uploader_type == 'Adwords':
            route_suffix = '_aw'
        elif uploader_type == 'DCM':
            route_suffix = '_dcm'
        else:
            route_suffix = ''
        set_uploader_relations_in_db(
            uploader_id=cur_up.id, form_relations=form.relations.data,
            object_level=object_level, uploader_type=uploader_type)
        if form.form_continue.data == 'continue':
            msg_text = 'Creating and uploading {} for uploader.'.format(
                object_level)
            cur_up.launch_task(
                '.uploader_create_and_upload_objects', _(msg_text),
                running_user=current_user.id, object_level=object_level,
                uploader_type=uploader_type)
            db.session.commit()
            return redirect(url_for(
                'main.edit_uploader_{}{}'.format(
                    next_level.lower(), route_suffix),
                object_name=cur_up.name))
        else:
            msg_text = 'Creating {} for uploader.'.format(object_level)
            task = cur_up.launch_task(
                '.uploader_create_objects', _(msg_text),
                running_user=current_user.id, object_level=object_level,
                uploader_type=uploader_type)
            db.session.commit()
            task.wait_and_get_job(loops=30)
            return redirect(url_for(
                'main.edit_uploader_{}{}'.format(
                    object_level.lower(), route_suffix),
                object_name=cur_up.name))
    kwargs['form'] = form
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader/<object_name>/edit/campaign', methods=['GET', 'POST'])
@login_required
def edit_uploader_campaign(object_name):
    object_level = 'Campaign'
    next_level = 'Adset'
    return edit_uploader_base_objects(object_name, object_level, next_level)


@bp.route('/uploader/<object_name>/edit/adset', methods=['GET', 'POST'])
@login_required
def edit_uploader_adset(object_name):
    object_level = 'Adset'
    next_level = 'Creative'
    return edit_uploader_base_objects(object_name, object_level, next_level)


@bp.route('/uploader/<object_name>/edit/creative', methods=['GET', 'POST'])
@login_required
def edit_uploader_creative(object_name):
    kwargs = get_current_uploader(object_name, 'edit_uploader_creative',
                                  edit_progress=80, edit_name='Creative')
    uploader_to_edit = kwargs['object']
    form = EditUploaderCreativeForm(uploader_to_edit.name)
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_uploader_creative',
                                    object_name=uploader_to_edit.name))
        else:
            return redirect(url_for('main.edit_uploader_ad',
                                    object_name=uploader_to_edit.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader/<object_name>/edit/creative/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_file_upload(object_name):
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request, object_name)
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, 'creative_file')
    msg_text = 'Saving file {} for {}'.format(file_name, cur_up.name)
    cur_up.launch_task(
        '.uploader_save_creative', _(msg_text),
        running_user=current_user.id, file=mem, file_name=file_name)
    db.session.commit()
    return jsonify({'data': 'success', 'message': msg_text, 'level': 'success'})


@bp.route('/uploader/<object_name>/edit/ad', methods=['GET', 'POST'])
@login_required
def edit_uploader_ad(object_name):
    object_level = 'Ad'
    next_level = 'Ad'
    return edit_uploader_base_objects(object_name, object_level, next_level)


@bp.route('/uploader/<object_name>/edit/duplicate',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_duplication(object_name):
    kwargs = get_current_uploader(object_name, 'edit_uploader_duplication',
                                  edit_progress=100, edit_name='Duplicate')
    cur_up = kwargs['object']
    form = UploaderDuplicateForm()
    kwargs['form'] = form
    if request.method == 'POST':
        msg_text = 'Sending request and attempting to duplicate uploader: {}' \
                   ''.format(cur_up.name)
        cur_up.launch_task(
            '.duplicate_uploader', _(msg_text),
            running_user=current_user.id, form_data=form.data)
        db.session.commit()
        return redirect(url_for('main.uploader_page',
                                object_name=cur_up.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader/<object_name>/edit/campaign/dcm',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_campaign_dcm(object_name):
    uploader_type = 'DCM'
    object_level = 'Campaign'
    next_level = 'Adset'
    return edit_uploader_base_objects(object_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<object_name>/edit/adset/dcm',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_adset_dcm(object_name):
    uploader_type = 'DCM'
    object_level = 'Adset'
    next_level = 'Ad'
    return edit_uploader_base_objects(object_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<object_name>/edit/ad/dcm',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_ad_dcm(object_name):
    uploader_type = 'DCM'
    object_level = 'Ad'
    next_level = 'Page'
    return edit_uploader_base_objects(object_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<object_name>/edit/campaign/aw',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_campaign_aw(object_name):
    uploader_type = 'Adwords'
    object_level = 'Campaign'
    next_level = 'Adset'
    return edit_uploader_base_objects(object_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<object_name>/edit/adset/aw',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_adset_aw(object_name):
    uploader_type = 'Adwords'
    object_level = 'Adset'
    next_level = 'Ad'
    return edit_uploader_base_objects(object_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<object_name>/edit/ad/aw',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_ad_aw(object_name):
    uploader_type = 'Adwords'
    object_level = 'Ad'
    next_level = 'Ad'
    return edit_uploader_base_objects(object_name, object_level, next_level,
                                      uploader_type)


@bp.route('/help')
@login_required
def app_help():
    skip_slides = ['Data Processor Basic Tutorial', 'Tutorial Complete!',
                   'Data Processor Tutorial 2 Electric Boogaloo']
    ts = TutorialStage.query.filter(
        TutorialStage.sub_header != 'Question',
        TutorialStage.header.notin_(skip_slides)).order_by(
        TutorialStage.id)
    ts = utl.group_sql_to_dict(ts, group_by='header')
    walk = Walkthrough().get_walk_questions('')
    tutorials = Tutorial.query.all()
    return render_template('help.html', title=_('Help'), tutorial_stages=ts,
                           walkthrough=walk, tutorials=tutorials,
                           user=current_user)


@bp.route('/processor/<object_name>/edit/duplicate',
          methods=['GET', 'POST'])
@login_required
def edit_processor_duplication(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_duplication',
        edit_progress=100, edit_name='Duplicate', buttons='ProcessorDuplicate')
    cur_proc = kwargs['processor']
    form = ProcessorDuplicateForm()
    kwargs['form'] = form
    if request.method == 'POST':
        msg_text = 'Sending request and attempting to duplicate processor: {}' \
                   ''.format(cur_proc.name)
        cur_proc.launch_task(
            '.duplicate_processor', _(msg_text),
            running_user=current_user.id, form_data=form.data)
        db.session.commit()
        return redirect(url_for('main.processor_page',
                                object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/dashboard', methods=['GET', 'POST'])
@login_required
def edit_processor_dashboard(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_dashboard', edit_progress=100,
        edit_name='Dashboard', buttons='ProcessorDashboard')
    return render_template('dashboards/dashboard.html', **kwargs)


def set_dashboard_filters_in_db(object_id, form_dicts,
                                base_object_class=Dashboard,
                                foreign_key_col='dashboard_id',
                                dep_object_class=DashboardFilter):
    obj = base_object_class.query.get(object_id)
    old_objects = dep_object_class.query.filter_by(
        **{foreign_key_col: obj.id}).all()
    new_objects = []
    for form_dict in form_dicts:
        new_object = dep_object_class()
        new_object.set_from_form(form_dict, obj)
        new_object.get_form_dict()
        new_objects.append(new_object)
    new_form_dicts = [x.form_dict for x in new_objects]
    for old_object in old_objects:
        old_object.get_form_dict()
        if old_object.form_dict not in new_form_dicts:
            db.session.delete(old_object)
    old_dicts = [x.form_dict for x in old_objects]
    for new_object in new_objects:
        if new_object.form_dict not in old_dicts:
            db.session.add(new_object)
    db.session.commit()
    return True


@bp.route('/get_dash_form', methods=['GET'])
@login_required
def processor_get_dash_form():
    form = ProcessorDashboardForm()
    form_html = render_template('_form.html', form=form)
    return jsonify({'form_html': form_html})


@bp.route('/processor/<object_name>/dashboard/create',
          methods=['GET', 'POST'])
@login_required
def processor_dashboard_create(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='processor_dashboard_create',
        edit_progress=50, edit_name='Create', buttons='ProcessorDashboard')
    cur_proc = kwargs['processor']
    form = ProcessorDashboardForm()
    kwargs['form'] = form
    if form.add_child.data:
        form.static_filters.append_entry()
        kwargs['form'] = form
        return render_template('create_processor.html', **kwargs)
    if request.method == 'POST':
        form.validate()
        form.dimensions.data = (
            form.dimensions.data if form.dimensions.data else 'eventdate')
        new_dash = Dashboard(
            processor_id=cur_proc.id, name=form.name.data,
            user_id=current_user.id, chart_type=form.chart_type.data,
            dimensions=form.dimensions.data, metrics=form.metrics.data,
            default_view=form.default_view.data, created_at=datetime.utcnow())
        db.session.add(new_dash)
        db.session.commit()
        set_dashboard_filters_in_db(new_dash.id, form.static_filters.data)
        creation_text = "New Dashboard {} for Processor {} was created!".format(
            new_dash.name, cur_proc.name)
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=cur_proc.id)
        db.session.add(post)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.processor_dashboard_all',
                                    object_name=cur_proc.name))
        elif request.args.get('render') == 'false':
            return jsonify({'data': 'success', 'message': creation_text,
                            'level': 'success'})
        else:
            return redirect(url_for('main.processor_dashboard_create',
                                    object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/dashboard/get', methods=['GET'])
@login_required
def processor_dashboards_get(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='processor_dashboard_all', edit_progress=100,
        edit_name='View All', buttons='ProcessorDashboard')
    cur_proc = kwargs['processor']
    dashboards = cur_proc.get_all_dashboards()
    dash_data = []
    for dash in dashboards:
        dash_id = dash.id
        metrics = dash.get_metrics_json()
        dimensions = dash.get_dimensions_json()
        chart_type = dash.chart_type
        chart_filters = dash.get_filters_json()
        default_view = dash.default_view
        dash_html = render_template('dashboards/_dash_card.html', dash=dash)
        dash_data.append(
            {'id': dash_id, 'metrics': metrics, 'dimensions': dimensions,
             'chart_type': chart_type, 'chart_filters': chart_filters,
             'default_view': default_view, 'html': dash_html})
    return jsonify(dash_data)


@bp.route('/processor/<object_name>/dashboard/all', methods=['GET', 'POST'])
@login_required
def processor_dashboard_all(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='processor_dashboard_all', edit_progress=100,
        edit_name='View All', buttons='ProcessorDashboard')
    cur_proc = kwargs['processor']
    dashboards = cur_proc.get_all_dashboards()
    for dash in dashboards:
        static_filters = ProcessorDashboardForm().set_filters(
            DashboardFilter, dash)
        dash_form = ProcessorDashboardForm(static_filters=static_filters)
        dash_form.name.data = dash.name
        dash_form.chart_type.data = dash.chart_type
        dash_form.dimensions.data = dash.get_dimensions()[0]
        dash_form.metrics.data = dash.get_metrics()
        for k, v in dash_form._fields.items():
            if k not in ['csrf_token', 'form_continue']:
                dash_form._fields[k].id = '{}__{}'.format(k, dash.id)
        dash.add_form(dash_form)
    kwargs['dashboards'] = dashboards
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<object_name>/dashboard/<dashboard_id>',
          methods=['GET', 'POST'])
@login_required
def processor_dashboard_view(object_name, dashboard_id):
    kwargs = Processor().get_current_processor(
        object_name, current_page='processor_dashboard_all', edit_progress=100,
        edit_name='View Dash', buttons='ProcessorDashboard')
    dashboard = Dashboard.query.filter_by(id=dashboard_id).all()
    for dash in dashboard:
        static_filters = ProcessorDashboardForm().set_filters(
            DashboardFilter, dash)
        dash_form = ProcessorDashboardForm(static_filters=static_filters)
        for static_filter in dash_form.static_filters.entries:
            selected_choices = static_filter.filter_val.__dict__['object_data']
            selected_choices = DashboardFilter.convert_string_to_list(
                selected_choices)
            static_filter.filter_val.choices = [
                (x, x) for x in selected_choices]
            static_filter.filter_val.data = selected_choices
            selected_choices = static_filter.filter_col.__dict__['object_data']
            selected_choices = DashboardFilter.convert_string_to_list(
                selected_choices)
            static_filter.filter_col.data = selected_choices[0]
        dash_form.name.data = dash.name
        dash_form.chart_type.data = dash.chart_type
        dash_form.dimensions.data = dash.get_dimensions()[0]
        dash_form.metrics.data = dash.get_metrics()
        dash.add_form(dash_form)
    for dash in dashboard:
        if dash.form[0].add_child.data:
            dash.form[0].static_filters.append_entry()
            kwargs['dashboards'] = dashboard
            return render_template('create_processor.html', **kwargs)
    kwargs['dashboards'] = dashboard
    return render_template('create_processor.html', **kwargs)


@bp.route('/get_dashboard_properties', methods=['GET', 'POST'])
@login_required
def get_dashboard_properties():
    if 'dashboard_id' in request.form and request.form['dashboard_id']:
        dash = Dashboard.query.get(int(request.form['dashboard_id']))
        metrics = dash.get_metrics_json()
        dimensions = dash.get_dimensions_json()
        chart_type = dash.chart_type
        chart_filters = dash.get_filters_json()
        default_view = dash.default_view
    else:
        metrics = []
        dimensions = []
        chart_type = []
        chart_filters = []
        default_view = []
    dash_properties = {'metrics': metrics, 'dimensions': dimensions,
                       'chart_type': chart_type, 'chart_filters': chart_filters,
                       'default_view': default_view}
    return jsonify(dash_properties)


@bp.route('/save_dashboard', methods=['GET', 'POST'])
@login_required
def save_dashboard():
    dash = Dashboard.query.get(int(request.form['dashboard_id']))
    object_form = json.loads(request.form.to_dict()['object_form'])
    dash.name = object_form['name']
    dash.chart_type = object_form['chart_type']
    dash.dimensions = object_form['dimensions']
    dash.metrics = object_form['metrics']
    dash.default_view = object_form['default_view']
    db.session.commit()
    if 'static_filters' in object_form:
        set_dashboard_filters_in_db(dash.id, object_form['static_filters'])
    msg = 'The dashboard {} has been saved!'.format(dash.name)
    return jsonify({'data': 'success', 'message': msg, 'level': 'success'})


@bp.route('/delete_dashboard', methods=['GET', 'POST'])
@login_required
def delete_dashboard():
    dash = Dashboard.query.get(int(request.form['dashboard_id']))
    db.session.delete(dash)
    db.session.commit()
    msg = 'The dashboard {} has been deleted.'.format(dash.name)
    return jsonify({'data': 'success', 'message': msg, 'level': 'success'})


@bp.route('/delete_processor/<object_name>', methods=['GET', 'POST'])
@login_required
def delete_processor(object_name):
    form = ProcessorDeleteForm()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    del_processor = Processor.query.filter_by(name=object_name).first()
    form.processor_name.data = del_processor.name
    if request.method == 'POST':
        form.validate()
        for p_number in del_processor.projects:
            del_processor.projects.remove(p_number)
            db.session.commit()
        db.session.delete(del_processor)
        db.session.commit()
        delete_text = 'Processor {} has been deleted.'.format(object_name)
        flash(_(delete_text))
        post = Post(body=delete_text, author=current_user)
        db.session.add(post)
        db.session.commit()
        return redirect(url_for('main.clients'))
    return render_template('create_processor.html', user=cur_user,
                           title=_('Delete Processor'), form=form,
                           edit_progress="100", edit_name='Delete')


@bp.route('/processor_change_project_number', methods=['GET', 'POST'])
@login_required
def processor_change_project_number():
    processor_id = request.form['object_name']
    p_numbers = json.loads(request.form['project_numbers'])
    cur_obj = Processor.query.filter_by(id=processor_id).first_or_404()
    if p_numbers:
        for p_number in p_numbers:
            parse_number = p_number.split('_')[0]
            proj = Project.query.filter_by(project_number=parse_number).first()
            if proj and proj not in cur_obj.projects:
                cur_obj.projects.append(proj)
                db.session.commit()
        msg = 'Project numbers {} now associated with {}'.format(
            ', '.join(p_numbers), cur_obj.name)
    else:
        for p_number in cur_obj.projects:
            cur_obj.projects.remove(p_number)
            db.session.commit()
        msg = 'Project numbers removed from {}'.format(cur_obj.name)
    post = Post(body=msg, author=current_user,
                processor_id=cur_obj.id)
    db.session.add(post)
    db.session.commit()
    lvl = 'success'
    msg = '<strong>{}</strong>, {}'.format(current_user.username, msg)
    return jsonify({'data': 'success', 'message': msg, 'level': lvl})


@bp.route('/processor/<object_name>/edit/duplicate_from_another',
          methods=['GET', 'POST'])
@login_required
def edit_processor_duplication_from_another(object_name):
    kwargs = Processor().get_current_processor(
        object_name, current_page='edit_processor_duplication',
        edit_progress=100, edit_name='Duplicate', buttons='ProcessorDuplicate')
    cur_proc = kwargs['processor']
    form = ProcessorDuplicateAnotherForm()
    form.new_proc.data = cur_proc.id
    form.new_name.data = cur_proc.name
    form.new_start_date.data = cur_proc.start_date
    form.new_end_date.data = cur_proc.end_date
    kwargs['form'] = form
    if request.method == 'POST':
        msg_text = 'Sending request and attempting to duplicate processor: {}' \
                   ''.format(cur_proc.name)
        proc_dup = Processor.query.get(form.data['old_proc'].id)
        proc_dup.launch_task(
            '.duplicate_processor', _(msg_text),
            running_user=current_user.id, form_data=form.data)
        db.session.commit()
        return redirect(url_for('main.processor_page',
                                object_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/walkthrough/edit', methods=['GET', 'POST'])
@login_required
def edit_walkthrough():
    form = WalkthroughUploadForm()
    kwargs = {'form': form}
    return render_template('create_processor.html', **kwargs)


@bp.route('/walkthrough/edit/upload_file', methods=['GET', 'POST'])
@login_required
def edit_walkthrough_upload_file():
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request)
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    msg_text = 'Updating walkthroughs'
    current_user.launch_task(
        '.update_walkthrough', _(msg_text),
        running_user=current_user.id, new_data=mem)
    db.session.commit()
    return jsonify({'data': 'success', 'message': msg_text, 'level': 'success'})


@bp.route('/upload_test', methods=['GET', 'POST'])
@login_required
def upload_test():
    current_form = UploadTestForm()
    cur_user = User.query.get(current_user.id)
    return render_template('upload_test.html', form=current_form,
                           test_list=[1, 2, 3], cur_user=cur_user)


@bp.route('/upload_test/upload_file', methods=['GET', 'POST'])
@login_required
def upload_test_upload_file():
    current_key, object_name, object_form, object_level = \
        utl.parse_upload_file_request(request)
    mem, file_name, file_type = \
        utl.get_file_in_memory_from_request(request, current_key)
    return jsonify({'data': 'success'})


@bp.route('/chat', methods=['GET', 'POST'])
@login_required
def chat():
    conversations = Conversation.query.filter_by(
        user_id=current_user.id).order_by(Conversation.created_at.desc()).all()
    intro_message = ('Hello I am ALI (Artificial Liquid Interface) '
                     'what can I help you with today?<br>')
    models_to_search = [Processor, Plan, TutorialStage, Notes, WalkthroughSlide,
                        Uploader, Project, Rfp, Specs, Contacts]
    for db_model in models_to_search:
        if hasattr(db_model, 'get_example_prompt'):
            ex_prompt = db_model.get_example_prompt(db_model)
            intro_message += ex_prompt
    kwargs = {'title': 'LQA CHAT', 'conversations': conversations,
              'intro_message': intro_message}
    if g and g.search_form.q.data:
        kwargs['initial_chat'] = g.search_form.q.data
    return render_template('chat.html', **kwargs)


@bp.route('/post_chat', methods=['GET', 'POST'])
@login_required
def post_chat():
    message = request.form['message']
    conversation_id = request.form['conversation_id']
    config_path = os.path.join('processor', 'config')
    aly = az.Analyze(load_chat=True, chat_path=config_path)
    models_to_search = [Processor, Plan, TutorialStage, Notes, WalkthroughSlide,
                        Uploader, Project, Rfp, Specs, Contacts]
    try:
        response, html_response = aly.chat.get_response(
            message, models_to_search, db=db, current_user=current_user)
    except:
        msg = 'Unhandled exception'
        current_app.logger.error(msg, exc_info=sys.exc_info())
        response = 'Sorry!  An error has occurred the admin has been notified.'
        html_response = ''
    new_chat = Chat(text=message, response=response,
                    conversation_id=conversation_id,
                    timestamp=datetime.utcnow(), html_response=html_response)
    db.session.add(new_chat)
    db.session.commit()
    response = {'response': new_chat.response,
                'html_response': new_chat.html_response}
    return jsonify(response)


@bp.route('/get_conversation', methods=['GET', 'POST'])
@login_required
def get_conversation():
    conversation_id = request.form['conversation_id']
    conv = db.session.get(Conversation, conversation_id)
    chats = Chat.query.filter_by(conversation_id=conv.id).order_by(
        Chat.timestamp)
    response = {'id': conv.id, 'chats': [x.to_dict() for x in chats]}
    return jsonify(response)


@bp.route('/post_conversation', methods=['GET', 'POST'])
@login_required
def post_conversation():
    message = request.form['message']
    conv = Conversation(name=message, user_id=current_user.id,
                        created_at=datetime.utcnow())
    db.session.add(conv)
    db.session.commit()
    response = {'id': conv.id, 'name': conv.name}
    return jsonify(response)


@bp.route('/project_number', methods=['GET', 'POST'])
@login_required
def project_number():
    kwargs = Project().get_current_project(edit_name='ProjectNumbers')
    return render_template('project_number.html', **kwargs)


@bp.route('/project_number/<object_name>', methods=['GET', 'POST'])
@login_required
def project_number_page(object_name):
    kwargs = Project().get_current_project(
        object_name, 'project_number_page', edit_progress=100, edit_name='Page')
    return render_template('dashboards/dashboard.html', **kwargs)


@bp.route('/project_number/<object_name>/edit', methods=['GET', 'POST'])
@login_required
def project_edit(object_name):
    form_description = """
    Basic information about this project number.
    """
    kwargs = Project().get_current_project(
        object_name, current_page='project_edit',
        edit_progress=100, edit_name='Basic', form_title='Basic',
        form_description=form_description)
    cur_project = kwargs['object']
    form = EditProjectForm(object_name)
    form.set_choices()
    kwargs['form'] = form
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.cur_client.data).check_and_add()
        form_product = Product(name=form.cur_product.data,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.cur_campaign.data,
                                 product_id=form_product.id).check_and_add()
        cur_project.campaign_id = form_campaign.id
        cur_project.project_name = form.project_name.data
        cur_project.project_number = form.project_number.data
        cur_project.start_date = form.start_date.data
        cur_project.end_date = form.end_date.data
        for proc in form.cur_processors.data:
            cur_proc = Processor.query.filter_by(name=proc).first()
            if cur_proc not in cur_project.processor_associated:
                cur_project.processor_associated.append(cur_proc)
        for proc in cur_project.processor_associated:
            if proc.name not in form.cur_processors.data:
                cur_project.processor_associated.remove(proc)
        for plan_name in form.cur_plans.data:
            cur_plan = Plan.query.filter_by(name=plan_name).first()
            if cur_plan not in cur_project.plan_associated:
                cur_project.plan_associated.append(cur_plan)
        for cur_plan in cur_project.plan_associated:
            if cur_plan.name not in form.cur_plans.data:
                cur_project.plan_associated.remove(cur_plan)
        db.session.commit()
        if form.form_continue.data == 'continue':
            next_page = 'main.project_billing'
        else:
            next_page = 'main.project_edit'
        return redirect(url_for(next_page, object_name=object_name))
    elif request.method == 'GET':
        form.project_name.data = cur_project.project_name
        form.project_number.data = cur_project.project_number
        form.start_date.data = cur_project.flight_start_date
        form.end_date.data = cur_project.flight_end_date
        form_campaign = Campaign.query.filter_by(
            id=cur_project.campaign_id).first()
        form_product = Product.query.filter_by(
            id=form_campaign.product_id).first()
        form_client = Client.query.filter_by(
            id=form_product.client_id).first()
        form.cur_campaign.data = form_campaign.name
        form.cur_product.data = form_product.name
        form.cur_client.data = form_client.name
        form.cur_processors.data = [
            x.name for x in cur_project.processor_associated]
        form.cur_plans.data = [
            x.name for x in cur_project.plan_associated]
    return render_template('create_processor.html', **kwargs)


@bp.route('/project_number/<object_name>/billing', methods=['GET', 'POST'])
@login_required
def project_billing(object_name):
    form_description = """
    Compare planned, reported and invoiced spends in one table.  
    Add Invoices to the table with total spend for best results.
    """
    kwargs = Project().get_current_project(
        object_name, current_page='project_billing',
        edit_progress=100, edit_name='Bill', form_title='BILLING',
        form_description=form_description)
    form = ProcessorContinueForm()
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            next_page = 'main.project_number'
        else:
            next_page = 'main.project_billing'
        return redirect(url_for(next_page, object_name=object_name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/url_from_view_function', methods=['GET', 'POST'])
@login_required
@error_handler
def url_from_view_function():
    object_name = request.form['object_name']
    view_function = request.form['view_function']
    url = url_for(str(view_function), object_name=object_name)
    return jsonify({'url': url})
