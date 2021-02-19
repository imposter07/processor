import io
import html
import json
import zipfile
import pandas as pd
from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, \
    jsonify, current_app, send_file
from flask_login import current_user, login_required
from flask_babel import _, get_locale
from guess_language import guess_language
from app import db
from app.main.forms import EditProfileForm, PostForm, SearchForm, MessageForm, \
    ProcessorForm, EditProcessorForm, ImportForm, ProcessorCleanForm,\
    ProcessorExportForm, UploaderForm, EditUploaderForm, ProcessorRequestForm,\
    GeneralAccountForm, EditProcessorRequestForm, FeeForm, \
    GeneralConversionForm, ProcessorRequestFinishForm,\
    ProcessorContinueForm, ProcessorFixForm, ProcessorRequestCommentForm,\
    ProcessorDuplicateForm, EditUploaderMediaPlanForm,\
    EditUploaderNameCreateForm, EditUploaderCreativeForm,\
    UploaderDuplicateForm, ProcessorDashboardForm, ProcessorCleanDashboardForm,\
    ProcessorMetricsForm, ProcessorMetricForm, PlacementForm
from app.models import User, Post, Message, Notification, Processor, \
    Client, Product, Campaign, ProcessorDatasources, TaskScheduler, \
    Uploader, Account, RateCard, Conversion, Requests, UploaderObjects,\
    UploaderRelations, Dashboard, DashboardFilter, ProcessorAnalysis
from app.translate import translate
from app.main import bp
import processor.reporting.vmcolumns as vmc
import uploader.upload.creator as cre


@bp.before_app_request
def before_request():
    if current_user.is_authenticated:
        current_user.last_seen = datetime.utcnow()
        db.session.commit()
        g.search_form = SearchForm()
    g.locale = str(get_locale())


@bp.route('/', methods=['GET', 'POST'])
@bp.route('/index', methods=['GET', 'POST'])
@login_required
def index():
    form = PostForm()
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
        page, current_app.config['POSTS_PER_PAGE'], False)
    next_url = url_for('main.index', page=posts.next_num) \
        if posts.has_next else None
    prev_url = url_for('main.index', page=posts.prev_num) \
        if posts.has_prev else None
    return render_template('index.html', title=_('Home'), form=form,
                           posts=posts.items, next_url=next_url,
                           prev_url=prev_url)


@bp.route('/health_check', methods=['GET'])
def health_check():
    return jsonify({'data': 'success'}), 200


@bp.route('/explore')
@login_required
def explore():
    page = request.args.get('page', 1, type=int)
    posts = Post.query.order_by(Post.timestamp.desc()).paginate(
        page, current_app.config['POSTS_PER_PAGE'], False)
    next_url = url_for('main.explore', page=posts.next_num) \
        if posts.has_next else None
    prev_url = url_for('main.explore', page=posts.prev_num) \
        if posts.has_prev else None
    return render_template('index.html', title=_('Explore'),
                           posts=posts.items, next_url=next_url,
                           prev_url=prev_url)


@bp.route('/get_processor_by_date', methods=['GET', 'POST'])
@login_required
def get_processor_by_date():
    processors = Processor.query.order_by(Processor.created_at)
    event_response = [{'title': 'Created: {}'.format(x.name),
                       'start': x.created_at.date().isoformat(),
                       'url': url_for(
                           'main.processor_page', processor_name=x.name),
                       'color': 'LightGreen',
                       'textColor': 'Black',
                       'borderColor': 'DimGray'}
                      for x in processors]
    event_response.extend(
        [{'title': x.name,
          'start': x.start_date.isoformat(),
          'end': x.end_date.isoformat(),
          'url': url_for('main.processor_page', processor_name=x.name),
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
        Processor.created_at.desc()).paginate(
        page, 3, False)
    processor_html = [render_template('processor_popup.html', processor=x)
                      for x in processors.items]
    return jsonify({'items': processor_html,
                    'pages': processors.pages,
                    'has_next': processors.has_next,
                    'has_prev': processors.has_prev})


@bp.route('/get_processor_by_user', methods=['GET', 'POST'])
@login_required
def get_processor_by_user():
    import datetime as dt
    processors = Processor.query.order_by(Processor.created_at)
    seven_days_ago = dt.datetime.today() - dt.timedelta(days=7)
    processors = processors.filter(Processor.end_date > seven_days_ago.date())
    for p in processors:
        p.ppd = '{0:.0f}'.format(p.posts.filter(
            Post.timestamp > seven_days_ago.date()).count() / 7)
    from collections import defaultdict
    groups = defaultdict(list)
    for obj in processors:
        groups[obj.user_id].append(obj)
    new_list = list(groups.values())
    new_list.sort(key=len, reverse=True)
    for u in new_list:
        cu = u[0].user
        cu.ppd = '{0:.0f}'.format(cu.posts.filter(
            Post.timestamp > seven_days_ago.date()).count() / 7)
        if cu.id in [3, 5, 7, 9, 10, 11, 51, 63, 66]:
            cu.data = True
        else:
            cu.data = False
    current_users = User.query.order_by(User.username).all()
    processor_html = render_template('processor_user_map.html',
                                     processors=new_list,
                                     current_users=current_users)
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
                key in sorted(new_dict.keys(), key=lambda x: x.name)}
    clients_html = render_template('_client_directory.html',
                                   client_dict=new_dict)
    return jsonify({'items': processor_html,
                    'client_directory': clients_html})


@bp.route('/processor_change_owner', methods=['GET', 'POST'])
@login_required
def processor_change_owner():
    processor_name = request.form['processor_name']
    new_owner = request.form['new_owner']
    cur_obj = Processor.query.filter_by(name=processor_name).first_or_404()
    new_user = User.query.filter_by(username=new_owner).first_or_404()
    cur_obj.user_id = new_user.id
    db.session.commit()
    lvl = 'success'
    msg = 'You have successfully assigned {} as the owner {}'.format(
        new_user.username, cur_obj.name)
    msg = '<strong>{}</strong>, {}'.format(current_user.username, msg)
    return jsonify({'data': 'success', 'message': msg, 'level': lvl})


@bp.route('/clients')
@login_required
def clients():
    current_clients = Client.query.order_by(Client.name).all()
    current_users = User.query.order_by(User.username).all()
    return render_template('clients.html', title=_('Clients'),
                           clients=current_clients,
                           current_users=current_users)


@bp.route('/user/<username>')
@login_required
def user(username):
    user_page = User.query.filter_by(username=username).first_or_404()
    page = request.args.get('page', 1, type=int)
    posts = user_page.posts.order_by(Post.timestamp.desc()).paginate(
        page, current_app.config['POSTS_PER_PAGE'], False)
    next_url = url_for('main.user', username=user_page.username,
                       page=posts.next_num) if posts.has_next else None
    prev_url = url_for('main.user', username=user_page.username,
                       page=posts.prev_num) if posts.has_prev else None
    return render_template('user.html', user=user_page, posts=posts.items,
                           next_url=next_url, prev_url=prev_url,
                           title=_('User | {}'.format(username)))


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


@bp.route('/follow_processor/<processor_name>')
@login_required
def follow_processor(processor_name):
    processor_follow = Processor.query.filter_by(name=processor_name).first()
    if processor_follow is None:
        flash(_('Processor {} not found.'.format(processor_name)))
        return redirect(url_for('main.index'))
    current_user.follow_processor(processor_follow)
    db.session.commit()
    flash(_('You are following {}!'.format(processor_name)))
    return redirect(url_for('main.processor_page',
                            processor_name=processor_name))


@bp.route('/unfollow_processor/<processor_name>')
@login_required
def unfollow_processor(processor_name):
    processor_unfollow = Processor.query.filter_by(name=processor_name).first()
    if processor_unfollow is None:
        flash(_('Processor {} not found.'.format(processor_name)))
        return redirect(url_for('main.index'))
    current_user.unfollow_processor(processor_unfollow)
    db.session.commit()
    flash(_('You are no longer following {}!'.format(processor_name)))
    return redirect(url_for('main.processor_page',
                            processor_name=processor_name))


@bp.route('/get_task_progress', methods=['GET', 'POST'])
@login_required
def get_task_progress():
    object_name = request.form['object_name']
    task_name = request.form['task_name']
    cur_obj = request.form['object_type']
    if cur_obj == 'Processor':
        cur_obj = Processor
    else:
        cur_obj = Uploader
    job_name, table_name, proc_arg = translate_table_name_to_job(
        task_name, proc_arg={})
    cur_obj = cur_obj.query.filter_by(name=object_name).first()
    task = cur_obj.get_task_in_progress(name=job_name)
    if task:
        percent = task.get_progress()
    else:
        percent = 90
    return jsonify({'percent': percent})


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
            page, current_app.config['POSTS_PER_PAGE'], False)
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
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    page = request.args.get('page', 1, type=int)
    current_clients = Client.query.order_by(Client.name)
    processors = cur_user.processor_followed.paginate(
        page, current_app.config['POSTS_PER_PAGE'], False)
    if len(processors.items) == 0:
        processors = Processor.query.paginate(
            page, current_app.config['POSTS_PER_PAGE'], False)
    next_url = (url_for('main.processor', username=cur_user.username,
                        page=processors.next_num)
                if processors.has_next else None)
    prev_url = (url_for('main.processor', username=cur_user.username,
                        page=processors.prev_num)
                if processors.has_prev else None)
    return render_template('processor.html', title=_('Processor'),
                           user=cur_user, processors=processors.items,
                           next_url=next_url, prev_url=prev_url,
                           clients=current_clients)


def get_navigation_buttons(buttons=None):
    if buttons == 'ProcessorRequest':
        buttons = [{'Basic': 'main.edit_request_processor'},
                   {'Accounts': 'main.edit_processor_account'},
                   {'Fees': 'main.edit_processor_fees'},
                   {'Conversions': 'main.edit_processor_conversions'},
                   {'Finish': 'main.edit_processor_finish'}]
    elif buttons == 'ProcessorRequestFix':
        buttons = [{'New Fix': 'main.edit_processor_request_fix'},
                   {'Submit Fixes': 'main.edit_processor_submit_fix'},
                   {'All Fixes': 'main.edit_processor_all_fix'}]
    elif buttons == 'ProcessorDuplicate':
        buttons = [{'Duplicate': 'main.edit_processor_duplication'}]
    elif buttons == 'ProcessorDashboard':
        buttons = [{'Create': 'main.processor_dashboard_create'},
                   {'View All': 'main.processor_dashboard_all'}]
    elif buttons == 'UploaderDCM':
        buttons = [{'Basic': 'main.edit_uploader'},
                   {'Campaign': 'main.edit_uploader_campaign_dcm'},
                   {'Adset': 'main.edit_uploader_adset_dcm'},
                   {'Ad': 'main.edit_uploader_ad_dcm'}]
    elif buttons == 'UploaderFacebook':
        buttons = [{'Basic': 'main.edit_uploader'},
                   {'Campaign': 'main.edit_uploader_campaign'},
                   {'Adset': 'main.edit_uploader_adset'},
                   {'Creative': 'main.edit_uploader_creative'},
                   {'Ad': 'main.edit_uploader_ad'}]
    elif buttons == 'UploaderAdwords':
        buttons = [{'Basic': 'main.edit_uploader'},
                   {'Campaign': 'main.edit_uploader_campaign_aw'},
                   {'Adset': 'main.edit_uploader_adset_aw'},
                   {'Ad': 'main.edit_uploader_ad_aw'}]
    else:
        buttons = [{'Basic': 'main.edit_processor'},
                   {'Import': 'main.edit_processor_import'},
                   {'Clean': 'main.edit_processor_clean'},
                   {'Export': 'main.edit_processor_export'}]
    return buttons


@bp.route('/create_processor', methods=['GET', 'POST'])
@login_required
def create_processor():
    form = ProcessorForm()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.client_name).check_and_add()
        form_product = Product(
            name=form.product_name, client_id=form_client.id).check_and_add()
        form_campaign = Campaign(
            name=form.campaign_name, product_id=form_product.id).check_and_add()
        new_processor = Processor(
            name=form.name.data, description=form.description.data,
            user_id=current_user.id, created_at=datetime.utcnow(),
            local_path=form.local_path.data, start_date=form.start_date.data,
            end_date=form.end_date.data,
            tableau_workbook=form.tableau_workbook.data,
            tableau_view=form.tableau_view.data, campaign_id=form_campaign.id)
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
                                    processor_name=new_processor.name))
        else:
            return redirect(url_for('main.processor'))
    return render_template('create_processor.html', user=cur_user,
                           title=_('Processor'), form=form, edit_progress="25",
                           edit_name='Basic', buttons=get_navigation_buttons())


def get_processor_run_links():
    run_links = {}
    for idx, run_arg in enumerate(
            (('full', 'Runs all processor modes from import to export.'),
             ('import', 'Runs api import. A modal will '
                        'popup specifying a specific API or all.'),
             ('basic', 'Runs regular processor that cleans all data '
                       'and generates Raw Data Output.'),
             ('export', 'Runs export to database and tableau refresh.'),
             ('update', 'RARELY NEEDED - Runs processor update on vendormatrix '
                        'and dictionaries based on code changes.'))):
        run_link = dict(title=run_arg[0].capitalize(),
                        tooltip=run_arg[1])
        run_links[idx] = run_link
    return run_links


def get_processor_edit_links():
    edit_links = {}
    for idx, edit_file in enumerate(
            (('Vendormatrix', 'Directly edit the vendormatrix. '
                              'This is the config file for all datasources.'),
             ('Translate', 'Directly edit the translation config. This config '
                           'file changes a dictionary value into another.'),
             ('Constant', 'Directly edit the constant config. This config file '
                          'sets a dictionary value for all dictionaries in '
                          'the processor instance.'),
             ('Relation', 'Directly edit relation config. This config file '
                          'specifies relations between dictionary values. '))):
        edit_links[idx] = dict(title=edit_file[0],
                               nest=[], tooltip=edit_file[1])
        if edit_file[0] == 'Relation':
            edit_links[idx]['nest'] = ['Campaign', 'Targeting', 'Creative',
                                       'Vendor', 'Country', 'Serving', 'Copy']
    return edit_links


def get_processor_output_links():
    output_links = {}
    for idx, out_file in enumerate(
            (('FullOutput', 'Downloads the Raw Data Output.'),
             ('Vendor', 'View modal table of data grouped by Vendor'),
             ('Target', 'View modal table of data grouped by Target'),
             ('Creative', 'View modal table of data grouped by Creative'),
             ('Copy', 'View modal table of data grouped by Copy'),
             ('BuyModel', 'View modal table of data grouped by BuyModel'))):
        output_links[idx] = dict(title=out_file[0], nest=[],
                                 tooltip=out_file[1])
    return output_links


def get_processor_request_links(processor_name):
    run_links = {0: {'title': 'View Initial Request',
                     'href': url_for('main.edit_request_processor',
                                     processor_name=processor_name),
                     'tooltip': 'View/Edits the initial request that made this '
                                'processor instance. This will not change '
                                'anything unless the processor is rebuilt.'},
                 1: {'title': 'Request Data Fix',
                     'href': url_for('main.edit_processor_request_fix',
                                     processor_name=processor_name),
                     'tooltip': 'Request a fix for the current processor data '
                                'set, including changing values, adding'
                                ' files etc.'},
                 2: {'title': 'Request Duplication',
                     'href': url_for('main.edit_processor_duplication',
                                     processor_name=processor_name),
                     'tooltip': 'Duplicates current processor instance based on'
                                ' date to use new instance going forward.'},
                 3: {'title': 'Request Dashboard',
                     'href': url_for('main.processor_dashboard_create',
                                     processor_name=processor_name),
                     'tooltip': 'Create a dashboard in the app that queries the'
                                ' database based on this processor instance.'}
                 }
    return run_links


def get_posts_for_objects(cur_obj, fix_id, current_page, object_name):
    page = request.args.get('page', 1, type=int)
    post_filter = {'{}_id'.format(object_name): cur_obj.id}
    if fix_id:
        post_filter['request_id'] = fix_id
    query = Post.query
    for attr, value in post_filter.items():
        query = query.filter(getattr(Post, attr) == value)
    posts = (query.
             order_by(Post.timestamp.desc()).
             paginate(page, 5, False))
    next_url = url_for('main.' + current_page, page=posts.next_num,
                       **{'{}_name'.format(object_name): cur_obj.name}
                       ) if posts.has_next else None
    prev_url = url_for('main.' + current_page, page=posts.prev_num,
                       **{'{}_name'.format(object_name): cur_obj.name}
                       ) if posts.has_prev else None
    return posts, next_url, prev_url


def get_current_processor(processor_name, current_page, edit_progress=0,
                          edit_name='Page', buttons=None, fix_id=None):
    cur_proc = Processor.query.filter_by(name=processor_name).first_or_404()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    posts, next_url, prev_url = get_posts_for_objects(
        cur_obj=cur_proc, fix_id=fix_id, current_page=current_page,
        object_name='processor')
    api_imports = {0: {'All': 'import'}}
    for idx, (k, v) in enumerate(vmc.api_translation.items()):
        api_imports[idx + 1] = {k: v}
    run_links = get_processor_run_links()
    edit_links = get_processor_edit_links()
    output_links = get_processor_output_links()
    request_links = get_processor_request_links(processor_name)
    args = dict(object=cur_proc, processor=cur_proc,
                posts=posts.items, title=_('Processor'),
                object_name=cur_proc.name, user=cur_user,
                edit_progress=edit_progress, edit_name=edit_name,
                api_imports=api_imports,
                object_function_call={'processor_name': cur_proc.name},
                run_links=run_links, edit_links=edit_links,
                output_links=output_links, request_links=request_links,
                next_url=next_url, prev_url=prev_url)
    args['buttons'] = get_navigation_buttons(buttons)
    return args


def add_df_to_processor_dict(form_import, processor_dicts):
    for fi in form_import:
        if ('raw_file' in fi and fi['raw_file']
                and '{"data":"success' not in fi['raw_file']):
            df = convert_file_to_df(fi['raw_file'])
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


def convert_file_to_df(current_file):
    df = pd.read_csv(current_file)
    return df


def get_file_in_memory_from_request(current_request, current_key):
    file = current_request.files[current_key]
    file_name = file.filename
    mem = io.BytesIO()
    mem.write(file.read())
    mem.seek(0)
    return mem, file_name


def parse_upload_file_request(current_request):
    current_form = current_request.form.to_dict()
    current_key = list(current_form.keys())[0]
    current_form = json.loads(current_form[current_key])
    object_name = current_form['object_name']
    object_form = current_form['object_form']
    object_level = current_form['object_level']
    return current_key, object_name, object_form, object_level


@bp.route('/processor/<processor_name>/edit/import/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_import_upload_file(processor_name):
    current_key, object_name, object_form, object_level = \
        parse_upload_file_request(request)
    cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
    mem, file_name = get_file_in_memory_from_request(request, current_key)
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
            vk=None, mem_file=True, new_name=new_name)
    else:
        msg_text = 'Adding raw data for {}'.format(ds.vendor_key)
        cur_proc.launch_task(
            '.write_raw_data', _(msg_text),
            running_user=current_user.id, new_data=mem,
            vk=ds.vendor_key, mem_file=True)
    db.session.commit()
    return jsonify({'data': 'success: {}'.format(processor_name)})


@bp.route('/processor/<processor_name>/edit/import', methods=['GET', 'POST'])
@login_required
def edit_processor_import(processor_name):
    kwargs = get_current_processor(processor_name, 'edit_processor_import',
                                   50, 'Import')
    cur_proc = kwargs['processor']
    apis = ImportForm().set_apis(ProcessorDatasources, kwargs['processor'])
    form = ImportForm(apis=apis)
    form.set_vendor_key_choices(current_processor_id=cur_proc.id)
    kwargs['form'] = form
    if form.add_child.data:
        form.apis.append_entry()
        kwargs['form'] = form
        return render_template('create_processor.html', **kwargs)
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
                                    processor_name=processor_name))
    if request.method == 'POST':
        if cur_proc.get_task_in_progress('.set_processor_imports'):
            flash(_('The data sources are already being set.'))
        else:
            form_imports = set_processor_imports_in_db(
                processor_id=cur_proc.id, form_imports=form.apis.data)
            msg_text = ('Setting imports in '
                        'vendormatrix for {}').format(processor_name)
            task = cur_proc.launch_task(
                '.set_processor_imports', _(msg_text),
                running_user=current_user.id, form_imports=form_imports,
                set_in_db=False)
            db.session.commit()
            task.wait_and_get_job(loops=20)
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_clean',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_import',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


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
                 'Uploader': '.write_uploader_file'}
    msg = '<strong>{}</strong>, {}'.format(current_user.username, msg_text)
    if table_name in ['delete_dict', 'imports', 'data_sources', 'OutputData']:
        return jsonify({'data': 'success', 'message': msg, 'level': 'success'})
    job_name = arg_trans[table_name]
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
    if table_name == 'download_raw_data':
        proc_arg['parameter'] = 'Download'
    arg_trans = {'Translate': '.get_translation_dict',
                 'Vendormatrix': '.get_vendormatrix',
                 'Constant': '.get_constant_dict',
                 'Relation': '.get_relational_config',
                 'OutputData': '.get_data_tables',
                 'dictionary_order': '.get_dict_order',
                 'raw_data': '.get_raw_data',
                 'download_raw_data': '.get_raw_data',
                 'dictionary': '.get_dictionary',
                 'delete_dict': '.delete_dict',
                 'rate_card': '.get_rate_card',
                 'edit_conversions': '.get_processor_conversions',
                 'data_sources': '.get_processor_sources',
                 'imports': '.get_processor_sources'}
    for x in ['Uploader', 'Campaign', 'Adset', 'Ad', 'Creator',
              'uploader_full_relation', 'edit_relation', 'name_creator',
              'uploader_current_name', 'uploader_creative_files',
              'upload_filter', 'match_table']:
        arg_trans[x] = '.get_uploader_file'
    job_name = arg_trans[table_name]
    return job_name, table_name, proc_arg


@bp.route('/get_table', methods=['GET', 'POST'])
@login_required
def get_table():
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    proc_arg = {'running_user': cur_user.id}
    cur_obj = request.form['object_type']
    table_name = request.form['table']
    if cur_obj == 'Processor':
        cur_obj = Processor
    else:
        cur_obj = Uploader
        proc_arg['parameter'] = table_name
        table_name = 'Uploader'
        proc_arg['uploader_type'] = request.form['uploader_type']
        proc_arg['object_level'] = request.form['object_level']
    cur_proc = cur_obj.query.filter_by(
        name=request.form['object_name']).first_or_404()
    job_name, table_name, proc_arg = translate_table_name_to_job(
        table_name=table_name, proc_arg=proc_arg)
    if cur_proc.get_task_in_progress(job_name):
        flash(_('This job: {} is already running!').format(table_name))
        return jsonify({'data': {'data': [], 'cols': [], 'name': table_name}})
    if request.form['vendorkey'] != 'None':
        proc_arg['vk'] = request.form['vendorkey']
        table_name = '{}vendorkey{}'.format(
            table_name, request.form['vendorkey'].replace(' ', '___'))
    msg_text = 'Getting {} table for {}'.format(table_name, cur_proc.name)
    task = cur_proc.launch_task(job_name, _(msg_text), **proc_arg)
    db.session.commit()
    if job_name in ['.get_processor_sources']:
        job = task.wait_and_get_job(loops=20)
        if job:
            df = pd.DataFrame([{'Result': 'DATA WAS REFRESHED.'}])
        else:
            df = pd.DataFrame([{'Result': 'DATA IS REFRESHING.'}])
    else:
        job = task.wait_and_get_job(force_return=True)
        df = job.result[0]
    if ('parameter' in proc_arg and (proc_arg['parameter'] == 'FullOutput' or
                                     proc_arg['parameter'] == 'Download')):
        z = zipfile.ZipFile(df)
        df = z.read('raw.csv')
        z.close()
        mem = io.BytesIO()
        mem.write(df)
        mem.seek(0)
        return send_file(mem,
                         as_attachment=True,
                         attachment_filename='test.csv',
                         mimetype='text/csv'
                         )
    pd.set_option('display.max_colwidth', -1)
    df = df.reset_index()
    if 'index' in df.columns:
        df = df[[x for x in df.columns if x != 'index'] + ['index']]
    cols = json.dumps(df.columns.tolist())
    for base_name in ['Relation', 'Uploader']:
        if base_name in table_name:
            table_name = '{}{}'.format(base_name, proc_arg['parameter'])
            if 'vk' in proc_arg:
                table_name = '{}vendorkey{}'.format(
                    table_name, request.form['vendorkey'].replace(' ', '___'))
    table_name = "modalTable{}".format(table_name)
    data = df.to_html(index=False, table_id=table_name,
                      classes="table table-responsive-sm small table-dark")
    return jsonify({'data': {'data': data, 'cols': cols, 'name': table_name}})


@bp.context_processor
def utility_functions():
    def print_in_console(message):
        print(str(message))
    return dict(mdebug=print_in_console)


def df_to_html(df, name):
    pd.set_option('display.max_colwidth', -1)
    df = df.reset_index()
    if 'index' in df.columns:
        df = df[[x for x in df.columns if x != 'index'] + ['index']]
    data = df.to_html(
        index=False, table_id=name,
        classes='table table-striped table-responsive-sm small',
        border=0)
    cols = json.dumps(df.columns.tolist())
    return {'data': {'data': data, 'cols': cols, 'name': name}}


def rename_duplicates(old):
    seen = {}
    for x in old:
        if x in seen:
            seen[x] += 1
            new_val = '{} {}'.format(x, seen[x])
            if new_val in old:
                yield '{}-{}'.format(new_val, 1)
            else:
                yield new_val
        else:
            seen[x] = 0
            yield x


def get_placement_form(data_source):
    form = PlacementForm()
    ds_dict = data_source.get_form_dict_with_split()
    auto_order_cols = list(rename_duplicates(ds_dict['auto_dictionary_order']))
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
    metric_dict = df.drop('index', axis=1).to_dict(orient='index')
    metric_dict = {v['Metric Name']: [v['Metric Value']] for k, v in
                   metric_dict.items()}
    ds_dict = {'original_vendor_key': datasource_name,
               'vendor_key': datasource_name,
               'active_metrics': json.dumps(metric_dict),
               'vm_rules': request.form['rules_table']}
    for col in ['full_placement_columns', 'placement_columns',
                'auto_dictionary_placement', 'auto_dictionary_order']:
        new_data = get_col_from_serialize_dict(data, col)
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


@bp.route('/get_datasource_table', methods=['GET', 'POST'])
@login_required
def get_datasource_table():
    obj_name = request.form['object_name']
    cur_proc = Processor.query.filter_by(name=obj_name).first_or_404()
    import processor.reporting.analyze as az
    analysis = ProcessorAnalysis.query.filter_by(
        processor_id=cur_proc.id, key=az.Analyze.raw_columns).first()
    if analysis and analysis.data:
        df = pd.DataFrame(analysis.data)
    else:
        df = pd.DataFrame(columns=['Vendor Key'])
    analysis = ProcessorAnalysis.query.filter_by(
        processor_id=cur_proc.id, key=az.Analyze.raw_file_update_col).first()
    if analysis and analysis.data:
        tdf = pd.DataFrame(analysis.data)
    else:
        tdf = pd.DataFrame(columns=['source'])
    if df.empty:
        df = pd.merge(tdf, df, how='outer', left_on='source',
                      right_on='Vendor Key')
        df = df.drop(['Vendor Key'], axis=1)
        df = df.rename(columns={'source': 'Vendor Key'})
    else:
        df = pd.merge(df, tdf, how='outer', left_on='Vendor Key',
                      right_on='source')
        df = df.drop(['source'], axis=1)
    analysis = ProcessorAnalysis.query.filter_by(
        processor_id=cur_proc.id, key=az.Analyze.unknown_col).first()
    if analysis and analysis.data:
        tdf = pd.DataFrame(analysis.data)
        tdf['Vendor Key'] = tdf['Vendor Key'].str.strip("'")
        cols = [x for x in tdf.columns if x != 'Vendor Key']
        col = 'Undefined Plan Net'
        tdf[col] = tdf[cols].values.tolist()
        tdf[col] = tdf[col].str.join('_')
        tdf = tdf.drop(cols, axis=1)
        tdf = tdf.groupby(['Vendor Key'], as_index=False).agg(
            {col: '|'.join})
    else:
        tdf = pd.DataFrame(columns=['Vendor Key'])
    df = pd.merge(df, tdf, how='outer', left_on='Vendor Key',
                  right_on='Vendor Key')
    analysis = ProcessorAnalysis.query.filter_by(
        processor_id=cur_proc.id, key=az.Analyze.vk_metrics).first()
    if analysis and analysis.data:
        tdf = pd.DataFrame(analysis.data)
    else:
        tdf = pd.DataFrame(columns=['Vendor Key'])
    df = pd.merge(df, tdf, how='outer', left_on='Vendor Key',
                  right_on='Vendor Key')
    table_data = df_to_html(df, 'datasource_table')
    return jsonify({'data': table_data})


@bp.route('/processor/<processor_name>/edit/clean/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_clean_upload_file(processor_name):
    current_key, object_name, object_form, object_level = \
        parse_upload_file_request(request)
    cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
    mem, file_name = get_file_in_memory_from_request(request, current_key)
    ds = [x for x in object_form if x['name'] == 'data_source_clean']
    if ds:
        ds = ds[0]['value']
    search_dict = {}
    search_dict['processor_id'] = cur_proc.id
    search_dict['vendor_key'] = ds
    ds = ProcessorDatasources.query.filter_by(**search_dict).first()
    msg_text = 'Adding raw data for {}'.format(ds.vendor_key)
    cur_proc.launch_task(
        '.write_raw_data', _(msg_text),
        running_user=current_user.id, new_data=mem,
        vk=ds.vendor_key, mem_file=True)
    db.session.commit()
    return jsonify({'data': 'success: {}'.format(processor_name)})


@bp.route('/processor/<processor_name>/edit/clean', methods=['GET', 'POST'])
@login_required
def edit_processor_clean(processor_name):
    kwargs = get_current_processor(processor_name, 'edit_processor_clean',
                                   edit_progress=75, edit_name='Clean')
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


@bp.route('/processor/<processor_name>/edit/export', methods=['GET', 'POST'])
@login_required
def edit_processor_export(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_export',
                                   edit_progress=100, edit_name='Export')
    cur_proc = kwargs['processor']
    form = ProcessorExportForm()
    sched = TaskScheduler.query.filter_by(processor_id=cur_proc.id).first()
    kwargs['form'] = form
    if request.method == 'GET':
        form.tableau_workbook.data = cur_proc.tableau_workbook
        form.tableau_view.data = cur_proc.tableau_view
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
        cancel_schedule(sched)
        if form.schedule.data:
            msg_text = 'Scheduling processor: {}'.format(processor_name)
            cur_proc.schedule_job('.full_run_processor', msg_text,
                                  start_date=form.schedule_start.data,
                                  end_date=form.schedule_end.data,
                                  scheduled_time=form.run_time.data,
                                  interval=form.interval.data)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.processor_page',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_export',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<processor_name>')
@login_required
def processor_page(processor_name):
    kwargs = get_current_processor(processor_name, 'processor_page',
                                   edit_progress=100, edit_name='Page')
    return render_template('dashboard.html', **kwargs)


@bp.route('/processor/<processor_name>/popup')
@login_required
def processor_popup(processor_name):
    processor_for_page = Processor.query.filter_by(
        name=processor_name).first_or_404()
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
            'update': '--update all --noprocess',
            'fb': '--api fb --analyze',
            'aw': '--api aw --analyze',
            'tw': '--api tw --analyze',
            'ttd': '--api ttd --analyze',
            'ga': '--api ga --analyze',
            'nb': '--api nb --analyze',
            'af': '--api af --analyze',
            'sc': '--api sc --analyze',
            'aj': '--api aj --analyze',
            'dc': '--api dc --analyze',
            'rs': '--api rs --analyze',
            'db': '--api db --analyze',
            'vk': '--api vk --analyze',
            'rc': '--api rc --analyze',
            'szk': '--api szk --analyze',
            'red': '--api red --analyze',
            'dcm': '--api dc --analyze',
            'dv': '--api dv --analyze',
            'adk': '--api adk --analyze',
            'inn': '--api inn -- analyze',
            'tik': '--api tik --analyze',
            'amz': '--api amz --analyze',
            'cri': '--api cri --analyze'}
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


@bp.route('/processor/<processor_name>/edit', methods=['GET', 'POST'])
@login_required
def edit_processor(processor_name):
    kwargs = get_current_processor(processor_name, 'edit_processor',
                                   edit_progress=25, edit_name='Basic')
    processor_to_edit = Processor.query.filter_by(
        name=processor_name).first_or_404()
    form = EditProcessorForm(processor_name)
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.client_name).check_and_add()
        form_product = Product(name=form.product_name,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.campaign_name,
                                 product_id=form_product.id).check_and_add()
        processor_to_edit.name = form.name.data
        processor_to_edit.description = form.description.data
        processor_to_edit.local_path = form.local_path.data
        processor_to_edit.tableau_workbook = form.tableau_workbook.data
        processor_to_edit.tableau_view = form.tableau_view.data
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
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_import',
                                    processor_name=processor_to_edit.name))
        else:
            return redirect(url_for('main.processor_page',
                                    processor_name=processor_to_edit.name))
    elif request.method == 'GET':
        form.name.data = processor_to_edit.name
        form.description.data = processor_to_edit.description
        form.local_path.data = processor_to_edit.local_path
        form.tableau_workbook.data = processor_to_edit.tableau_workbook
        form.tableau_view.data = processor_to_edit.tableau_view
        form.start_date.data = processor_to_edit.start_date
        form.end_date.data = processor_to_edit.end_date
        form_campaign = Campaign.query.filter_by(
            id=processor_to_edit.campaign_id).first_or_404()
        form_product = Product.query.filter_by(
            id=form_campaign.product_id).first_or_404()
        form_client = Client.query.filter_by(
            id=form_product.client_id).first_or_404()
        form.cur_campaign.data = form_campaign
        form.cur_product.data = form_product
        form.cur_client.data = form_client
    kwargs['form'] = form
    return render_template('create_processor.html',  **kwargs)


def convert_media_plan_to_df(current_file):
    mp = cre.MediaPlan(current_file)
    return mp.df


def check_and_add_media_plan(media_plan_data, processor_to_edit,
                             object_type=Processor):
    if media_plan_data:
        df = convert_media_plan_to_df(media_plan_data)
        msg_text = ('Attempting to save media plan for processor: {}'
                    ''.format(processor_to_edit.name))
        processor_to_edit.launch_task(
            '.save_media_plan', _(msg_text),
            running_user=current_user.id,
            media_plan=df, object_type=object_type)
        db.session.commit()


@bp.route('/request_processor', methods=['GET', 'POST'])
@login_required
def request_processor():
    form = ProcessorRequestForm()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.client_name).check_and_add()
        form_product = Product(
            name=form.product_name, client_id=form_client.id).check_and_add()
        form_campaign = Campaign(
            name=form.campaign_name, product_id=form_product.id).check_and_add()
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
        check_and_add_media_plan(form.media_plan.data, new_processor)
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_account',
                                    processor_name=new_processor.name))
        else:
            return redirect(url_for('main.processor'))
    return render_template('create_processor.html', user=cur_user,
                           title=_('Processor'), form=form, edit_progress="25",
                           edit_name='Basic',
                           buttons=get_navigation_buttons('ProcessorRequest'))


@bp.route('/processor/<processor_name>/edit/request', methods=['GET', 'POST'])
@login_required
def edit_request_processor(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_request_processor',
                                   edit_progress=25, edit_name='Basic',
                                   buttons='ProcessorRequest')
    processor_to_edit = Processor.query.filter_by(
        name=processor_name).first_or_404()
    form = EditProcessorRequestForm(processor_name)
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.client_name).check_and_add()
        form_product = Product(name=form.product_name,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.campaign_name,
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
        check_and_add_media_plan(form.media_plan.data, processor_to_edit)
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_account',
                                    processor_name=processor_to_edit.name))
        else:
            return redirect(url_for('main.edit_request_processor',
                                    processor_name=processor_to_edit.name))
    elif request.method == 'GET':
        form.name.data = processor_to_edit.name
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
        form.cur_campaign.data = form_campaign
        form.cur_product.data = form_product
        form.cur_client.data = form_client
    kwargs['form'] = form
    return render_template('create_processor.html',  **kwargs)


@bp.route('/processor/<processor_name>/edit/accounts', methods=['GET', 'POST'])
@login_required
def edit_processor_account(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_account',
                                   edit_progress=50, edit_name='Accounts',
                                   buttons='ProcessorRequest')
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
            act = Account.query.filter_by(
                key=act.key.data, account_id=act.account_id.data,
                campaign_id=act.campaign_id.data, username=act.username.data,
                password=act.password.data, processor_id=cur_proc.id).first()
            if act:
                db.session.delete(act)
                db.session.commit()
            return redirect(url_for('main.edit_processor_account',
                                    processor_name=processor_name))
    if request.method == 'POST':
        msg_text = 'Setting accounts for {}'.format(processor_name)
        task = cur_proc.launch_task(
            '.set_processor_accounts', _(msg_text),
            running_user=current_user.id, form_sources=form.accounts.data)
        db.session.commit()
        task.wait_and_get_job()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_fees',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_account',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<processor_name>/edit/fees', methods=['GET', 'POST'])
@login_required
def edit_processor_fees(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_fees',
                                   edit_progress=75, edit_name='Fees',
                                   buttons='ProcessorRequest')
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
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_fees',
                                    processor_name=cur_proc.name))
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


@bp.route('/processor/<processor_name>/edit/conversions',
          methods=['GET', 'POST'])
@login_required
def edit_processor_conversions(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_conversions',
                                   edit_progress=75, edit_name='Conversions',
                                   buttons='ProcessorRequest')
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
                                    processor_name=processor_name))
    if request.method == 'POST':
        msg_text = 'Setting conversions for {}'.format(processor_name)
        task = cur_proc.launch_task(
            '.set_processor_conversions', _(msg_text),
            running_user=current_user.id, form_sources=form.conversions.data)
        db.session.commit()
        task.wait_and_get_job()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_finish',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_conversions',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<processor_name>/edit/finish',
          methods=['GET', 'POST'])
@login_required
def edit_processor_finish(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_finish',
                                   edit_progress=100, edit_name='Finish',
                                   buttons='ProcessorRequest')
    cur_proc = kwargs['processor']
    cur_users = ProcessorRequestFinishForm().set_users(Processor, cur_proc)
    form = ProcessorRequestFinishForm(assigned_users=cur_users)
    kwargs['form'] = form
    if form.add_child.data:
        form.assigned_users.append_entry()
        kwargs['form'] = form
        return render_template('create_processor.html', **kwargs)
    for usr in form.assigned_users:
        if usr.delete.data:
            delete_user = usr.assigned_user.data
            if delete_user:
                delete_user.unfollow_processor(cur_proc)
                db.session.commit()
            return redirect(url_for('main.edit_processor_finish',
                                    processor_name=processor_name))
    if request.method == 'POST':
        for usr in form.assigned_users:
            follow_user = usr.assigned_user.data
            if follow_user:
                follow_user.follow_processor(cur_proc)
                db.session.commit()
        if form.form_continue.data == 'continue':
            msg_text = 'Sending request and attempting to build processor: {}' \
                       ''.format(processor_name)
            cur_proc.launch_task(
                '.build_processor_from_request', _(msg_text),
                running_user=current_user.id)
            db.session.commit()
            return redirect(url_for('main.processor_page',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_finish',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<processor_name>/edit/fix',
          methods=['GET', 'POST'])
@login_required
def edit_processor_request_fix(processor_name):
    kwargs = get_current_processor(processor_name=processor_name,
                                   current_page='edit_processor_request_fix',
                                   edit_progress=33, edit_name='New Fix',
                                   buttons='ProcessorRequestFix')
    cur_proc = kwargs['processor']
    form = ProcessorFixForm()
    form.set_vendor_key_choices(current_processor_id=cur_proc.id)
    kwargs['form'] = form
    if request.method == 'POST':
        new_processor_request = Requests(
            processor_id=cur_proc.id, fix_type=form.fix_type.data,
            column_name=form.column_name.data,
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
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_request_fix',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<processor_name>/edit/fix/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_request_fix_upload_file(processor_name):
    current_key, object_name, object_form, object_level =\
        parse_upload_file_request(request)
    cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
    fix_type = [x['value'] for x in object_form if x['name'] == 'fix_type'][0]
    mem, file_name = get_file_in_memory_from_request(request, current_key)
    if fix_type == 'New File':
        new_name = file_name
        new_name = new_name.replace('.csv', '')
        msg_text = 'Adding new raw_data data for {}'.format(new_name)
        cur_proc.launch_task(
            '.write_raw_data', _(msg_text),
            running_user=current_user.id, new_data=mem,
            vk=None, mem_file=True, new_name=new_name)
    elif fix_type == 'Upload File':
        vk = [x['value'] for x in object_form if x['name'] == 'data_source'][0]
        ds = ProcessorDatasources.query.filter_by(vendor_key=vk).first()
        msg_text = 'Adding raw data for {}'.format(ds.vendor_key)
        cur_proc.launch_task(
            '.write_raw_data', _(msg_text),
            running_user=current_user.id, new_data=mem,
            vk=ds.vendor_key, mem_file=True)
    elif fix_type == 'Update Plan':
        check_and_add_media_plan(mem, cur_proc)
    elif fix_type == 'Spend Cap':
        msg_text = 'Adding new spend cap'
        cur_proc.launch_task(
            '.save_spend_cap_file', _(msg_text),
            running_user=current_user.id, new_data=mem)
    db.session.commit()
    return jsonify({'data': 'success: {}'.format(processor_name)})


@bp.route('/processor/<processor_name>/edit/fix/submit',
          methods=['GET', 'POST'])
@login_required
def edit_processor_submit_fix(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_submit_fix',
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
                    'for processor: {}').format(processor_name)
        cur_proc.launch_task(
            '.processor_fix_requests', _(msg_text),
            running_user=current_user.id)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_processor_all_fix',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_submit_fix',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<processor_name>/edit/fix/all',
          methods=['GET', 'POST'])
@login_required
def edit_processor_all_fix(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_all_fix',
                                   edit_progress=100, edit_name='All Fixes',
                                   buttons='ProcessorRequestFix')
    cur_proc = kwargs['processor']
    fixes = cur_proc.get_all_requests()
    form = ProcessorContinueForm()
    kwargs['fixes'] = fixes
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.processor_page',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_all_fix',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/resolve_fix/<fix_id>')
@login_required
def resolve_fix(fix_id):
    request_fix = Requests.query.get(fix_id)
    if request_fix is None:
        flash(_('Request #{} not found.'.format(request_fix)))
        return redirect(url_for('main.edit_processor_submit_fix',
                                processor_name=request_fix.processor.name))
    msg_txt = 'The fix #{} has been marked as resolved!'.format(request_fix.id)
    request_fix.mark_resolved()
    post = Post(body=msg_txt, author=current_user,
                processor_id=request_fix.processor.id,
                request_id=fix_id)
    db.session.add(post)
    db.session.commit()
    flash(_(msg_txt.format(request_fix.id)))
    return redirect(url_for('main.edit_processor_view_fix',
                            processor_name=request_fix.processor.name,
                            fix_id=request_fix.id))


@bp.route('/unresolve_fix/<fix_id>')
@login_required
def unresolve_fix(fix_id):
    request_fix = Requests.query.get(fix_id)
    if request_fix is None:
        flash(_('Request #{} not found.'.format(request_fix)))
        return redirect(url_for('main.edit_processor_submit_fix',
                                processor_name=request_fix.processor.name))
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
                            processor_name=request_fix.processor.name,
                            fix_id=request_fix.id))


@bp.route('/processor/<processor_name>/edit/fix/<fix_id>',
          methods=['GET', 'POST'])
@login_required
def edit_processor_view_fix(processor_name, fix_id):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_view_fix',
                                   edit_progress=100, edit_name='View Fixes',
                                   buttons='ProcessorRequestFix',
                                   fix_id=fix_id)
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
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_view_fix',
                                    processor_name=cur_proc.name,
                                    fix_id=fix_id))
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader')
@login_required
def uploader():
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    page = request.args.get('page', 1, type=int)
    current_clients = Client.query.order_by(Client.name)
    uploaders = current_user.uploader.order_by(
        Uploader.last_run_time.desc()).paginate(
        page, current_app.config['POSTS_PER_PAGE'], False)
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


def check_base_uploader_object(uploader_id, object_level='Campaign',
                               uploader_type='Facebook'):
    new_uploader = Uploader.query.get(uploader_id)
    upo = UploaderObjects.query.filter_by(
        uploader_id=new_uploader.id, object_level=object_level,
        uploader_type=uploader_type).first()
    if not upo:
        upo = UploaderObjects(uploader_id=new_uploader.id,
                              object_level=object_level,
                              uploader_type=uploader_type)
        if object_level == 'Campaign':
            upo.media_plan_columns = [
                'Campaign ID', 'Placement Phase (If Needed) ',
                'Partner Name', 'Country', 'Creative (If Needed)']
            upo.file_filter = 'Facebook|Instagram'
            upo.name_create_type = 'Media Plan'
            if uploader_type == 'Facebook':
                upo.file_filter = 'Facebook|Instagram'
            elif uploader_type == 'Adwords':
                upo.file_filter = 'Google SEM|Search|GDN'
            else:
                upo.file_filter = ''
        elif object_level == 'Adset':
            upo.media_plan_columns = ['Placement Name']
            upo.name_create_type = 'Media Plan'
            if uploader_type == 'Facebook':
                upo.file_filter = 'Facebook|Instagram'
            elif uploader_type == 'Adwords':
                upo.file_filter = 'Google SEM|Search|GDN'
            else:
                upo.file_filter = ''
        elif object_level == 'Ad':
            upo.media_plan_columns = ['Placement Name']
            upo.name_create_type = 'File'
            upo.duplication_type = 'All'
            if uploader_type == 'Facebook':
                upo.file_filter = 'Facebook|Instagram'
            elif uploader_type == 'Adwords':
                upo.file_filter = 'Google SEM|Search|GDN'
            else:
                upo.file_filter = ''
        else:
            pass
        db.session.add(upo)
        db.session.commit()
    return jsonify({'data': 'success'})


def check_relation_uploader_objects(uploader_id, object_level='Campaign',
                                    uploader_type='Facebook'):
    import datetime as dt
    import uploader.upload.fbapi as up_fbapi
    import uploader.upload.awapi as up_awapi
    import uploader.upload.dcapi as up_dcapi
    new_uploader = Uploader.query.get(uploader_id)
    upo = UploaderObjects.query.filter_by(uploader_id=new_uploader.id,
                                          object_level=object_level,
                                          uploader_type=uploader_type).first()
    constant_col = 'relation_constant'
    position_col = 'position'
    default_sd = dt.datetime.today().strftime('%m/%d/%Y')
    default_ed = (dt.datetime.today()
                  + dt.timedelta(days=7)).strftime('%m/%d/%Y')
    relation_column_names = []
    if object_level == 'Campaign':
        if uploader_type == 'Facebook':
            fb_cu = up_fbapi.CampaignUpload
            relation_column_names = {
                fb_cu.objective: {constant_col: 'LINK_CLICKS'},
                fb_cu.spend_cap: {constant_col: '1000'},
                fb_cu.status: {constant_col: 'PAUSED'}}
        elif uploader_type == 'Adwords':
            aw_cu = up_awapi.CampaignUpload
            relation_column_names = {
                aw_cu.status: {constant_col: 'PAUSED'},
                aw_cu.sd: {constant_col: default_sd},
                aw_cu.ed: {constant_col: default_ed},
                aw_cu.budget: {constant_col: '10'},
                aw_cu.method: {constant_col: 'STANDARD'},
                aw_cu.freq: {constant_col: '5|DAY|ADGROUP'},
                aw_cu.channel: {position_col: [1]},
                aw_cu.channel_sub: {constant_col: ''},
                aw_cu.network: {position_col: [1]},
                aw_cu.strategy: {constant_col: 'TARGET_SPEND|5'},
                aw_cu.settings: {constant_col: ''},
                aw_cu.location: {position_col: [2]},
                aw_cu.language: {position_col: [2]},
                aw_cu.platform: {position_col: [3]}
            }
        elif uploader_type == 'DCM':
            dcm_cu = up_dcapi.CampaignUpload
            relation_column_names = {
                dcm_cu.advertiserId: {constant_col: ''},
                dcm_cu.sd: {constant_col: default_sd},
                dcm_cu.ed: {constant_col: default_ed},
                dcm_cu.defaultLandingPage: {constant_col: ''},
            }
    elif object_level == 'Adset':
        if uploader_type == 'Facebook':
            fb_asu = up_fbapi.AdSetUpload
            relation_column_names = {
                fb_asu.cam_name: {position_col: [0, 15, 1, 2, 4]},
                fb_asu.target: {position_col: [22]},
                fb_asu.country: {position_col: [2]},
                fb_asu.age_min: {constant_col: '18'},
                fb_asu.age_max: {constant_col: '44'},
                fb_asu.genders: {constant_col: 'M'},
                fb_asu.device: {position_col: [19]},
                fb_asu.pubs: {position_col: [1]},
                fb_asu.pos: {constant_col: ''},
                fb_asu.budget_type: {constant_col: 'lifetime'},
                fb_asu.budget_value: {constant_col: '10'},
                fb_asu.goal: {constant_col: 'LINK_CLICKS'},
                fb_asu.bid: {constant_col: '2'},
                fb_asu.start_time: {constant_col: default_sd},
                fb_asu.end_time: {constant_col: default_ed},
                fb_asu.status: {constant_col: 'PAUSED'},
                fb_asu.bill_evt: {constant_col: 'IMPRESSIONS'},
                fb_asu.prom_page: {constant_col: '_'}
            }
        elif uploader_type == 'DCM':
            dcm_asu = up_dcapi.PlacementUpload
            relation_column_names = {
                dcm_asu.campaignId: {position_col: [0, 15, 1, 2, 4]},
                dcm_asu.siteId: {position_col: [1]},
                dcm_asu.compatibility: {position_col: [21]},
                dcm_asu.size: {position_col: [20]},
                dcm_asu.paymentSource: {constant_col: 'PLACEMENT_AGENCY_PAID'},
                dcm_asu.tagFormats: {position_col: [10]},
                dcm_asu.startDate: {constant_col: default_sd},
                dcm_asu.endDate: {constant_col: default_ed},
                dcm_asu.pricingType: {constant_col: 'PRICING_TYPE_CPM'},
            }
        elif uploader_type == 'Adwords':
            aw_asu = up_awapi.AdGroupUpload
            relation_column_names = {
                aw_asu.cam_name: {position_col: [0, 15, 1, 2, 4]},
                aw_asu.status: {constant_col: 'PAUSED'},
                aw_asu.bid_type: {position_col: [7]},
                aw_asu.bid_val: {constant_col: '2'},
                aw_asu.age_range: {constant_col: 'age34'},
                aw_asu.gender: {constant_col: 'gendermale'},
                aw_asu.keyword: {position_col: [3]},
                aw_asu.topic: {position_col: ''},
                aw_asu.placement: {constant_col: ''},
                aw_asu.affinity: {constant_col: ''},
                aw_asu.in_market: {constant_col: ''},
            }
    elif object_level == 'Ad':
        if uploader_type == 'Facebook':
            fb_adu = up_fbapi.AdUpload
            relation_column_names = {
                fb_adu.cam_name: {},
                fb_adu.adset_name: {},
                fb_adu.filename: {position_col: [3, 0]},
                fb_adu.prom_page: {constant_col: '_'},
                fb_adu.ig_id: {constant_col: '_'},
                fb_adu.link: {position_col: ''},
                fb_adu.d_link: {constant_col: 'liquidadvertising.com'},
                fb_adu.title: {position_col: [3, 1]},
                fb_adu.body: {position_col: [3, 1]},
                fb_adu.desc: {position_col: [3, 1]},
                fb_adu.cta: {constant_col: 'DOWNLOAD'},
                fb_adu.view_tag: {constant_col: ''},
                fb_adu.status: {constant_col: 'PAUSED'},
            }
        elif uploader_type == 'Adwords':
            aw_adu = up_awapi.AdUpload
            relation_column_names = {
                aw_adu.ag_name: {},
                aw_adu.cam_name: {},
                aw_adu.type: {position_col: [1]},
                aw_adu.headline1: {position_col: ''},
                aw_adu.headline2: {position_col: ''},
                aw_adu.headline3: {position_col: ''},
                aw_adu.description: {position_col: ''},
                aw_adu.description2: {position_col: ''},
                aw_adu.business_name: {constant_col: 'Business'},
                aw_adu.final_url: {position_col: ''},
                aw_adu.track_url: {position_col: ''},
                aw_adu.display_url: {constant_col: ''},
                aw_adu.marketing_image: {position_col: ''},
                aw_adu.image: {constant_col: ''},
            }
        elif uploader_type == 'DCM':
            dcm_adu = up_dcapi.AdUpload
            relation_column_names = {
                dcm_adu.campaignId: {},
                dcm_adu.creativeRotation: {},
                dcm_adu.deliverySchedule: {},
                dcm_adu.endTime: {constant_col: default_ed},
                dcm_adu.startTime: {constant_col: default_sd},
                dcm_adu.type: {position_col: ''},
                dcm_adu.placementAssignments: {},
                dcm_adu.creative: {},
            }
    for col in relation_column_names:
        relation = UploaderRelations.query.filter_by(
            uploader_objects_id=upo.id, impacted_column_name=col).first()
        if not relation:
            new_relation = UploaderRelations(
                uploader_objects_id=upo.id, impacted_column_name=col,
                **relation_column_names[col])
            db.session.add(new_relation)
            db.session.commit()


def create_base_uploader_objects(uploader_id):
    for uploader_type in ['Facebook', 'DCM', 'Adwords']:
        for obj in ['Campaign', 'Adset', 'Ad']:
            check_base_uploader_object(uploader_id, obj, uploader_type)
        for obj in ['Campaign', 'Adset', 'Ad']:
            check_relation_uploader_objects(uploader_id, obj, uploader_type)
    return jsonify({'data': 'success'})


@bp.route('/create_uploader', methods=['GET', 'POST'])
@login_required
def create_uploader():
    form = UploaderForm()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.client_name).check_and_add()
        form_product = Product(
            name=form.product_name, client_id=form_client.id).check_and_add()
        form_campaign = Campaign(
            name=form.campaign_name, product_id=form_product.id).check_and_add()
        new_path = '/mnt/c/clients/{}/{}/{}/{}/uploader'.format(
            form_client.name, form_product.name, form_campaign.name,
            form.name.data)
        new_uploader = Uploader(
            name=form.name.data, description=form.description.data,
            user_id=current_user.id, created_at=datetime.utcnow(),
            local_path=new_path, campaign_id=form_campaign.id,
            fb_account_id=form.fb_account_id.data,
            aw_account_id=form.aw_account_id.data,
            dcm_account_id=form.dcm_account_id.data
        )
        db.session.add(new_uploader)
        db.session.commit()
        create_base_uploader_objects(new_uploader.id)
        post_body = 'Create Uploader {}...'.format(new_uploader.name)
        new_uploader.launch_task('.create_uploader', _(post_body),
                                 current_user.id,
                                 current_app.config['BASE_UPLOADER_PATH'])
        creation_text = ('Uploader {} was requested for creation.'
                         ''.format(new_uploader.name))
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    uploader_id=new_uploader.id)
        db.session.add(post)
        db.session.commit()
        check_and_add_media_plan(form.media_plan.data, new_uploader,
                                 object_type=Uploader)
        if form.media_plan.data:
            new_uploader.media_plan = True
            db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_uploader_campaign',
                                    uploader_name=new_uploader.name))
        else:
            return redirect(url_for('main.edit_uploader',
                                    uploader_name=new_uploader.name))
    return render_template('create_processor.html', user=cur_user,
                           title=_('Uploader'), form=form, edit_progress="25",
                           edit_name='Basic')


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


def get_uploader_request_links(uploader_name):
    req_links = {
                 0: {'title': 'Request Duplication',
                     'href': url_for('main.edit_uploader_duplication',
                                     uploader_name=uploader_name)}
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


def get_current_uploader(uploader_name, current_page, edit_progress=0,
                         edit_name='Page', buttons='Uploader', fix_id=None,
                         uploader_type='Facebook'):
    cur_up = Uploader.query.filter_by(name=uploader_name).first_or_404()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    posts, next_url, prev_url = get_posts_for_objects(
        cur_obj=cur_up, fix_id=fix_id, current_page=current_page,
        object_name='uploader')
    run_links = get_uploader_run_links()
    edit_links = get_uploader_edit_links()
    request_links = get_uploader_request_links(uploader_name)
    view_selector = get_uploader_view_selector(uploader_type)
    nav_buttons = get_navigation_buttons(buttons + uploader_type)
    args = {'object': cur_up, 'posts': posts.items, 'title': _('Uploader'),
            'object_name': cur_up.name, 'user': cur_user,
            'edit_progress': edit_progress, 'edit_name': edit_name,
            'buttons': nav_buttons,
            'object_function_call': {'uploader_name': cur_up.name},
            'run_links': run_links, 'edit_links': edit_links,
            'request_links': request_links,
            'next_url': next_url, 'prev_url': prev_url,
            'view_selector': view_selector,
            'uploader_type': uploader_type}
    return args


@bp.route('/uploader/<uploader_name>')
@login_required
def uploader_page(uploader_name):
    kwargs = get_current_uploader(uploader_name, 'uploader_page',
                                  edit_progress=100, edit_name='Page')
    kwargs['uploader_objects'] = kwargs['object'].uploader_objects
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader/<uploader_name>/edit/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_upload_file(uploader_name):
    current_key, object_name, object_form, object_level =\
        parse_upload_file_request(request)
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    mem, file_name = get_file_in_memory_from_request(request, current_key)
    check_and_add_media_plan(mem, cur_up, object_type=Uploader)
    cur_up.media_plan = True
    db.session.commit()
    return jsonify({'data': 'success: {}'.format(uploader_name)})


@bp.route('/uploader/<uploader_name>/edit', methods=['GET', 'POST'])
@login_required
def edit_uploader(uploader_name):
    kwargs = get_current_uploader(uploader_name, 'edit_uploader',
                                  edit_progress=20, edit_name='Basic')
    uploader_to_edit = kwargs['object']
    form = EditUploaderForm(uploader_name)
    if request.method == 'POST':
        form.validate()
        form_client = Client(name=form.client_name).check_and_add()
        form_product = Product(name=form.product_name,
                               client_id=form_client.id).check_and_add()
        form_campaign = Campaign(name=form.campaign_name,
                                 product_id=form_product.id).check_and_add()
        uploader_to_edit.name = form.name.data
        uploader_to_edit.description = form.description.data
        uploader_to_edit.campaign_id = form_campaign.id
        uploader_to_edit.fb_account_id = form.fb_account_id.data
        uploader_to_edit.aw_account_id = form.aw_account_id.data
        uploader_to_edit.dcm_account_id = form.dcm_account_id.data
        db.session.commit()
        create_base_uploader_objects(uploader_to_edit.id)
        flash(_('Your changes have been saved.'))
        post_body = ('Create Uploader {}...'.format(uploader_to_edit.name))
        uploader_to_edit.launch_task('.create_uploader', _(post_body),
                                     current_user.id,
                                     current_app.config['BASE_UPLOADER_PATH'])
        creation_text = 'Uploader was requested for creation.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    uploader_id=uploader_to_edit.id)
        db.session.add(post)
        db.session.commit()
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_uploader_campaign',
                                    uploader_name=uploader_to_edit.name))
        else:
            return redirect(url_for('main.edit_uploader',
                                    uploader_name=uploader_to_edit.name))
    elif request.method == 'GET':
        form.name.data = uploader_to_edit.name
        form.description.data = uploader_to_edit.description
        form_campaign = Campaign.query.filter_by(
            id=uploader_to_edit.campaign_id).first_or_404()
        form_product = Product.query.filter_by(
            id=form_campaign.product_id).first_or_404()
        form_client = Client.query.filter_by(
            id=form_product.client_id).first_or_404()
        form.cur_campaign.data = form_campaign
        form.cur_product.data = form_product
        form.cur_client.data = form_client
        form.fb_account_id.data = uploader_to_edit.fb_account_id
        form.aw_account_id.data = uploader_to_edit.aw_account_id
        form.dcm_account_id.data = uploader_to_edit.dcm_account_id
    kwargs['form'] = form
    return render_template('create_processor.html',  **kwargs)


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


def uploader_name_file_upload(uploader_name):
    current_key, object_name, object_form, object_level =\
        parse_upload_file_request(request)
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    up_obj = UploaderObjects.query.filter_by(
        uploader_id=cur_up.id, object_level=object_level).first()
    mem, file_name = get_file_in_memory_from_request(request, current_key)
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
    return jsonify({'data': 'success: {}'.format(uploader_name)})


@bp.route('/uploader/<uploader_name>/edit/campaign/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_campaign_name_file_upload(uploader_name):
    return uploader_name_file_upload(uploader_name)


@bp.route('/uploader/<uploader_name>/edit/adset/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_adset_name_file_upload(uploader_name):
    return uploader_name_file_upload(uploader_name)


@bp.route('/uploader/<uploader_name>/edit/ad/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_ad_name_file_upload(uploader_name):
    return uploader_name_file_upload(uploader_name)


def edit_uploader_base_objects(uploader_name, object_level, next_level='Page',
                               uploader_type='Facebook'):
    kwargs = get_current_uploader(uploader_name, 'edit_uploader',
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
                uploader_name=cur_up.name))
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
                uploader_name=cur_up.name))
    kwargs['form'] = form
    return render_template('create_processor.html',  **kwargs)


@bp.route('/uploader/<uploader_name>/edit/campaign', methods=['GET', 'POST'])
@login_required
def edit_uploader_campaign(uploader_name):
    object_level = 'Campaign'
    next_level = 'Adset'
    return edit_uploader_base_objects(uploader_name, object_level, next_level)


@bp.route('/uploader/<uploader_name>/edit/adset', methods=['GET', 'POST'])
@login_required
def edit_uploader_adset(uploader_name):
    object_level = 'Adset'
    next_level = 'Creative'
    return edit_uploader_base_objects(uploader_name, object_level, next_level)


@bp.route('/uploader/<uploader_name>/edit/creative', methods=['GET', 'POST'])
@login_required
def edit_uploader_creative(uploader_name):
    kwargs = get_current_uploader(uploader_name, 'edit_uploader_creative',
                                  edit_progress=80, edit_name='Creative')
    # uploader_to_edit = kwargs['object']
    form = EditUploaderCreativeForm(uploader_name)
    kwargs['form'] = form
    if request.method == 'POST':
        if form.form_continue.data == 'continue':
            return redirect(url_for('main.edit_uploader_creative',
                                    uploader_name=uploader_name))
        else:
            return redirect(url_for('main.edit_uploader_ad',
                                    uploader_name=uploader_name))
    return render_template('create_processor.html',  **kwargs)


@bp.route('/uploader/<uploader_name>/edit/creative/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_file_upload(uploader_name):
    current_key, object_name, object_form, object_level =\
        parse_upload_file_request(request)
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    mem, file_name = get_file_in_memory_from_request(request, 'creative_file')
    msg_text = 'Saving file {} for {}'.format(file_name, cur_up.name)
    cur_up.launch_task(
        '.uploader_save_creative', _(msg_text),
        running_user=current_user.id, file=mem, file_name=file_name)
    db.session.commit()
    return jsonify({'data': 'success: {}'.format(uploader_name)})


@bp.route('/uploader/<uploader_name>/edit/ad', methods=['GET', 'POST'])
@login_required
def edit_uploader_ad(uploader_name):
    object_level = 'Ad'
    next_level = 'Ad'
    return edit_uploader_base_objects(uploader_name, object_level, next_level)


@bp.route('/uploader/<uploader_name>/edit/duplicate',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_duplication(uploader_name):
    kwargs = get_current_uploader(uploader_name, 'edit_uploader_duplication',
                                  edit_progress=100, edit_name='Duplicate')
    cur_up = kwargs['object']
    form = UploaderDuplicateForm()
    kwargs['form'] = form
    if request.method == 'POST':
        msg_text = 'Sending request and attempting to duplicate uploader: {}' \
                   ''.format(uploader_name)
        cur_up.launch_task(
            '.duplicate_uploader', _(msg_text),
            running_user=current_user.id, form_data=form.data)
        db.session.commit()
        return redirect(url_for('main.uploader_page',
                                uploader_name=cur_up.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader/<uploader_name>/edit/campaign/dcm',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_campaign_dcm(uploader_name):
    uploader_type = 'DCM'
    object_level = 'Campaign'
    next_level = 'Adset'
    return edit_uploader_base_objects(uploader_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<uploader_name>/edit/adset/dcm',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_adset_dcm(uploader_name):
    uploader_type = 'DCM'
    object_level = 'Adset'
    next_level = 'Ad'
    return edit_uploader_base_objects(uploader_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<uploader_name>/edit/ad/dcm',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_ad_dcm(uploader_name):
    uploader_type = 'DCM'
    object_level = 'Ad'
    next_level = 'Page'
    return edit_uploader_base_objects(uploader_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<uploader_name>/edit/campaign/aw',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_campaign_aw(uploader_name):
    uploader_type = 'Adwords'
    object_level = 'Campaign'
    next_level = 'Adset'
    return edit_uploader_base_objects(uploader_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<uploader_name>/edit/adset/aw',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_adset_aw(uploader_name):
    uploader_type = 'Adwords'
    object_level = 'Adset'
    next_level = 'Ad'
    return edit_uploader_base_objects(uploader_name, object_level, next_level,
                                      uploader_type)


@bp.route('/uploader/<uploader_name>/edit/ad/aw',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_ad_aw(uploader_name):
    uploader_type = 'Adwords'
    object_level = 'Ad'
    next_level = 'Ad'
    return edit_uploader_base_objects(uploader_name, object_level, next_level,
                                      uploader_type)


@bp.route('/help')
@login_required
def app_help():
    return render_template('help.html', title=_('Help'))


@bp.route('/processor/<processor_name>/edit/duplicate',
          methods=['GET', 'POST'])
@login_required
def edit_processor_duplication(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_duplication',
                                   edit_progress=100, edit_name='Duplicate',
                                   buttons='ProcessorDuplicate')
    cur_proc = kwargs['processor']
    form = ProcessorDuplicateForm()
    kwargs['form'] = form
    if request.method == 'POST':
        msg_text = 'Sending request and attempting to duplicate processor: {}' \
                   ''.format(processor_name)
        cur_proc.launch_task(
            '.duplicate_processor', _(msg_text),
            running_user=current_user.id, form_data=form.data)
        db.session.commit()
        return redirect(url_for('main.processor_page',
                                processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/get_metrics', methods=['GET', 'POST'])
@login_required
def get_metrics():
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    dimensions = [request.form['x_col']]
    if 'dashboard_id' in request.form and request.form['dashboard_id']:
        dash = Dashboard.query.get(request.form['dashboard_id'])
        metrics = dash.get_metrics()
        dimensions = dash.get_dimensions()
    else:
        if 'filter_col' in request.form:
            dimensions += request.form['filter_col'].split('|')
        metrics = request.form['y_col'].split('|')
    proc_arg = {'running_user': cur_user.id,
                'dimensions': dimensions,
                'metrics': metrics}
    if 'filter_dict' in request.form:
        proc_arg['filter_dict'] = json.loads(request.form['filter_dict'])
    obj_name = request.form['object_name']
    cur_proc = Processor.query.filter_by(name=obj_name).first_or_404()
    msg_text = 'Getting metric table for {}'.format(cur_proc.name)
    if request.form['elem'] == '#totalMetrics':
        job_name = '.get_processor_total_metrics'
        proc_arg = {x: proc_arg[x] for x in proc_arg
                    if x in ['running_user', 'filter_dict']}
    elif request.form['elem'] == '#dash_placeholderMetrics':
        job_name = '.get_raw_file_data_table'
        proc_arg['parameter'] = request.form['vendor_key']
    else:
        job_name = '.get_data_tables_from_db'
    task = cur_proc.launch_task(job_name, _(msg_text), **proc_arg)
    db.session.commit()
    job = task.wait_and_get_job(force_return=True)
    df = job.result[0]
    data = df.reset_index().to_dict(orient='records')
    return jsonify(data)


@bp.route('/processor/<processor_name>/dashboard', methods=['GET', 'POST'])
@login_required
def edit_processor_dashboard(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='edit_processor_dashboard',
                                   edit_progress=100, edit_name='Dashboard',
                                   buttons='ProcessorDashboard')
    return render_template('dashboard.html', **kwargs)


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


@bp.route('/processor/<processor_name>/dashboard/create',
          methods=['GET', 'POST'])
@login_required
def processor_dashboard_create(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='processor_dashboard_create',
                                   edit_progress=50, edit_name='Create',
                                   buttons='ProcessorDashboard')
    cur_proc = kwargs['processor']
    form = ProcessorDashboardForm()
    kwargs['form'] = form
    if form.add_child.data:
        form.static_filters.append_entry()
        kwargs['form'] = form
        return render_template('create_processor.html', **kwargs)
    if request.method == 'POST':
        form.validate()
        new_dash = Dashboard(
            processor_id=cur_proc.id, name=form.name.data,
            user_id=current_user.id, chart_type=form.chart_type.data,
            dimensions=form.dimensions.data, metrics=form.metrics.data,
            created_at=datetime.utcnow())
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
                                    processor_name=processor_name))
        else:
            return redirect(url_for('main.processor_dashboard_create',
                                    processor_name=processor_name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<processor_name>/dashboard/all', methods=['GET', 'POST'])
@login_required
def processor_dashboard_all(processor_name):
    kwargs = get_current_processor(processor_name,
                                   current_page='processor_dashboard_all',
                                   edit_progress=100, edit_name='View All',
                                   buttons='ProcessorDashboard')
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


@bp.route('/processor/<processor_name>/dashboard/<dashboard_id>',
          methods=['GET', 'POST'])
@login_required
def processor_dashboard_view(processor_name, dashboard_id):
    kwargs = get_current_processor(processor_name,
                                   current_page='processor_dashboard_all',
                                   edit_progress=100, edit_name='View Dash',
                                   buttons='ProcessorDashboard')
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
    else:
        metrics = []
        dimensions = []
        chart_type = []
        chart_filters = []
    dash_properties = {'metrics': metrics, 'dimensions': dimensions,
                       'chart_type': chart_type, 'chart_filters': chart_filters}
    return jsonify(dash_properties)


def get_col_from_serialize_dict(data, col_name):
    col_keys = [k for k, v in data.items() if v == col_name and 'name' in k]
    col_val_keys = [x.replace('name', 'value') for x in col_keys]
    col_vals = [v for k, v in data.items() if k in col_val_keys]
    return col_vals


def clean_serialize_dict(data):
    new_dict = {}
    for col in ['name', 'chart_type', 'dimensions', 'metrics']:
        new_dict[col] = get_col_from_serialize_dict(data, col)
    filter_idx = [v.replace('static_filters-', '').replace('-filter_col', '')
                  for k, v in data.items() if 'filter_col' in v and 'name' in k]
    new_dict['static_filters'] = []
    for filter_num in filter_idx:
        filter_dict = {}
        for col in ['filter_col', 'filter_val']:
            search_val = 'static_filters-{}-{}'.format(filter_num, col)
            col_vals = get_col_from_serialize_dict(data, search_val)
            filter_dict[col] = col_vals
        new_dict['static_filters'].append(filter_dict)
    return new_dict


@bp.route('/save_dashboard', methods=['GET', 'POST'])
@login_required
def save_dashboard():
    dash = Dashboard.query.get(int(request.form['dashboard_id']))
    object_form = request.form.to_dict()
    object_form = clean_serialize_dict(object_form)
    dash.name = object_form['name'][0]
    dash.chart_type = object_form['chart_type'][0]
    dash.dimensions = object_form['dimensions'][0]
    dash.metrics = object_form['metrics']
    db.session.commit()
    set_dashboard_filters_in_db(dash.id, object_form['static_filters'])
    msg = 'The dashboard {} has been saved!'.format(dash.name)
    return jsonify({'data': 'success', 'message': msg, 'level': 'success'})
