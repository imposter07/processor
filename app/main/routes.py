import io
import html
import json
from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, \
    jsonify, current_app
from flask_login import current_user, login_required
from flask_babel import _, get_locale
from guess_language import guess_language
from app import db
from app.main.forms import EditProfileForm, PostForm, SearchForm, MessageForm, \
    ProcessorForm, EditProcessorForm, ImportForm, ProcessorCleanForm,\
    ProcessorExportForm, UploaderForm, EditUploaderForm, ProcessorRequestForm,\
    GeneralAccountForm, EditProcessorRequestForm, FeeForm, \
    GeneralConversionForm, ProcessorRequestFinishForm,\
    ProcessorRequestFixForm, ProcessorFixForm, ProcessorRequestCommentForm,\
    ProcessorDuplicateForm, EditUploaderCampaignMediaPlanForm,\
    EditUploaderCampaignCreateForm, EditUploaderCreativeForm
from app.models import User, Post, Message, Notification, Processor, \
    Client, Product, Campaign, ProcessorDatasources, TaskScheduler, \
    Uploader, Account, RateCard, Conversion, Requests, UploaderObjects,\
    UploaderRelations
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


@bp.route('/clients')
@login_required
def clients():
    current_clients = Client.query.order_by(Client.name).all()
    return render_template('clients.html', title=_('Clients'),
                           clients=current_clients)


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


@bp.route('/get_processor_tasks', methods=['GET', 'POST'])
@login_required
def get_processor_tasks():
    processor_name = request.form['object_name']
    cur_proc = Processor.query.filter_by(name=processor_name).first()
    task = cur_proc.get_task_in_progress(name='.get_data_tables')
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
    processors = cur_user.processor_followed.order_by(
        Processor.last_run_time.desc()).paginate(
        page, current_app.config['POSTS_PER_PAGE'], False)
    if len(processors.items) == 0:
        processors = Processor.query.order_by(
            Processor.last_run_time.desc()).paginate(
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
    elif buttons == 'Uploader':
        buttons = [{'Basic': 'main.edit_uploader'},
                   {'Campaigns': 'main.edit_uploader_campaign'},
                   {'Adsets': 'main.edit_uploader'},
                   {'Creative': 'main.edit_uploader_creative'},
                   {'Ads': 'main.edit_uploader'}]
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


def get_processor_run_links(processor_name, edit_name):
    run_links = {}
    for idx, run_arg in enumerate(
            ('full', 'import', 'basic', 'export', 'update')):
        run_url = url_for('main.run_processor', processor_name=processor_name,
                          redirect_dest=edit_name, processor_args=run_arg)
        if run_arg == 'import':
            run_href = "javascript: show_modal_table('importRunButton');"
        else:
            run_href = run_url
        run_link = dict(title=run_arg.capitalize(),
                        url=run_url,
                        href=run_href)
        run_links[idx] = run_link
    return run_links


def get_processor_edit_links():
    edit_links = {}
    for idx, edit_file in enumerate(
            ('Vendormatrix', 'Translate', 'Constant', 'Relation')):
        edit_links[idx] = dict(title=edit_file, nest=[])
        if edit_file == 'Relation':
            edit_links[idx]['nest'] = ['Campaign', 'Targeting', 'Creative',
                                       'Vendor', 'Country', 'Serving', 'Copy']
    return edit_links


def get_processor_output_links():
    output_links = {}
    for idx, out_file in enumerate(
            ('FullOutput', 'Vendor', 'Target', 'Creative', 'Copy', 'BuyModel')):
        output_links[idx] = dict(title=out_file, nest=[])
    return output_links


def get_processor_request_links(processor_name):
    run_links = {0: {'title': 'View Initial Request',
                     'href': url_for('main.edit_request_processor',
                                     processor_name=processor_name)},
                 1: {'title': 'Request Data Fix',
                     'href': url_for('main.edit_processor_request_fix',
                                     processor_name=processor_name)},
                 2: {'title': 'Request Duplication',
                     'href': url_for('main.edit_processor_duplication',
                                     processor_name=processor_name)}
                 }
    return run_links


def get_current_processor(processor_name, current_page, edit_progress=0,
                          edit_name='Page', buttons=None, fix_id=None):
    cur_proc = Processor.query.filter_by(name=processor_name).first_or_404()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    page = request.args.get('page', 1, type=int)
    post_filter = {'processor_id': cur_proc.id}
    if fix_id:
        post_filter['request_id'] = fix_id
    query = Post.query
    for attr, value in post_filter.items():
        query = query.filter(getattr(Post, attr) == value)
    posts = (query.
             order_by(Post.timestamp.desc()).
             paginate(page, 5, False))
    api_imports = {0: {'All': 'import'}}
    for idx, (k, v) in enumerate(vmc.api_translation.items()):
        api_imports[idx + 1] = {k: v}
    run_links = get_processor_run_links(processor_name, edit_name)
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
                output_links=output_links, request_links=request_links)
    args['buttons'] = get_navigation_buttons(buttons)
    next_url = url_for('main.' + current_page, processor_name=cur_proc.name,
                       page=posts.next_num) if posts.has_next else None
    prev_url = url_for('main.' + current_page, processor_name=cur_proc.name,
                       page=posts.prev_num) if posts.has_prev else None
    args['prev_url'] = prev_url
    args['next_url'] = next_url
    return args


def add_df_to_processor_dict(form_import, processor_dicts):
    for fi in form_import:
        if ('raw_file' in fi and fi['raw_file']
                and fi['raw_file'] != '{"data":"success"}\r\n'):
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
    import pandas as pd
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
    return current_key, object_name, object_form


@bp.route('/processor/<processor_name>/edit/import/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_import_upload_file(processor_name):
    current_key, object_name, object_form = parse_upload_file_request(request)
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
    return jsonify({'data': 'success'})


@bp.route('/processor/<processor_name>/edit/import', methods=['GET', 'POST'])
@login_required
def edit_processor_import(processor_name):
    kwargs = get_current_processor(processor_name, 'edit_processor_import',
                                   50, 'Import')
    cur_proc = kwargs['processor']
    apis = ImportForm().set_apis(ProcessorDatasources, kwargs['processor'])
    form = ImportForm(apis=apis)
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
    if table_name in ['delete_dict', 'imports', 'data_sources', 'OutputData']:
        return jsonify({'data': 'success'})
    job_name = arg_trans[table_name]
    task = cur_proc.launch_task(job_name, _(msg_text), **proc_arg)
    db.session.commit()
    if table_name == 'Vendormatrix':
        task.wait_and_get_job(loops=20)
    return jsonify({'data': 'success'})


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
    cur_proc = cur_obj.query.filter_by(
        name=request.form['object_name']).first_or_404()
    for base_name in ['OutputData', 'Relation']:
        if base_name in table_name:
            proc_arg['parameter'] = table_name.replace(base_name, '')
            table_name = base_name
    arg_trans = {'Translate': '.get_translation_dict',
                 'Vendormatrix': '.get_vendormatrix',
                 'Constant': '.get_constant_dict',
                 'Relation': '.get_relational_config',
                 'OutputData': '.get_data_tables',
                 'dictionary_order': '.get_dict_order',
                 'raw_data': '.get_raw_data',
                 'dictionary': '.get_dictionary',
                 'delete_dict': '.delete_dict',
                 'rate_card': '.get_rate_card',
                 'edit_conversions': '.get_processor_conversions',
                 'data_sources': '.get_processor_sources',
                 'imports': '.get_processor_sources',
                 'Uploader': '.get_uploader_file'}
    job_name = arg_trans[table_name]
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
    import pandas as pd
    if job_name in ['.get_processor_sources']:
        job = task.wait_and_get_job(loops=20)
        if job:
            df = pd.DataFrame([{'Result': 'DATA WAS REFRESHED.'}])
        else:
            df = pd.DataFrame([{'Result': 'DATA IS REFRESHING.'}])
    else:
        job = task.wait_and_get_job(force_return=True)
        df = job.result[0]
    if 'parameter' in proc_arg and proc_arg['parameter'] == 'FullOutput':
        from flask import send_file
        import zipfile
        import io
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
    table_name = "modalTable{}".format(table_name)
    data = df.to_html(index=False, table_id=table_name,
                      classes="table table-responsive-sm small table-dark")
    return jsonify({'data': {'data': data, 'cols': cols, 'name': table_name}})


@bp.route('/processor/<processor_name>/edit/clean', methods=['GET', 'POST'])
@login_required
def edit_processor_clean(processor_name):
    kwargs = get_current_processor(processor_name, 'edit_processor_clean',
                                   edit_progress=75, edit_name='Clean')
    cur_proc = kwargs['processor']
    ds = ProcessorCleanForm().set_datasources(ProcessorDatasources, cur_proc)
    form = ProcessorCleanForm(datasources=ds)
    kwargs['form'] = form
    if request.method == 'POST':
        form.validate()
        if cur_proc.get_task_in_progress('.set_data_sources'):
            flash(_('The data sources are already being set.'))
        else:
            msg_text = ('Setting data sources in vendormatrix for {}'
                        '').format(processor_name)
            task = cur_proc.launch_task('.set_data_sources', _(msg_text),
                                        running_user=current_user.id,
                                        form_sources=form.datasources.data)
            db.session.commit()
            task.wait_and_get_job(loops=20)
            if form.form_continue.data == 'continue':
                return redirect(url_for('main.edit_processor_export',
                                        processor_name=cur_proc.name))
            else:
                task.wait_and_get_job()
                return redirect(url_for('main.edit_processor_clean',
                                        processor_name=cur_proc.name))
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
    cur_proc = kwargs['processor']
    return render_template('create_processor.html',
                           tableau_workbook=cur_proc.tableau_workbook, **kwargs)


@bp.route('/processor/<processor_name>/popup')
@login_required
def processor_popup(processor_name):
    processor_for_page = Processor.query.filter_by(
        name=processor_name).first_or_404()
    return render_template('processor_popup.html', processor=processor_for_page)


@bp.route('/processor/<processor_name>/run/<redirect_dest>/<processor_args>',
          methods=['GET', 'POST'])
@login_required
def run_processor(processor_name, processor_args='', redirect_dest=None):
    processor_to_run = Processor.query.filter_by(
        name=processor_name).first_or_404()
    if processor_to_run.get_task_in_progress('.run_processor'):
        flash(_('The processor is already running.'))
    else:
        post_body = ('Running {} for processor: {}...'.format(processor_args,
                                                              processor_name))
        arg_trans = {'full': '--api all --ftp all --dbi all --exp all --tab',
                     'import': '--api all --ftp all --dbi all',
                     'export': '--exp all --tab',
                     'basic': '--basic',
                     'update': '--update all --noprocess',
                     'fb': '--api fb',
                     'aw': '--api aw',
                     'tw': '--api tw',
                     'ttd': '--api ttd',
                     'ga': '--api ga',
                     'nb': '--api nb',
                     'af': '--api af',
                     'sc': '--api sc',
                     'aj': '--api aj',
                     'dc': '--api dc',
                     'rs': '--api rs',
                     'db': '--api db',
                     'vk': '--api vk',
                     'rc': '--api rc',
                     'szk': '--api szk',
                     'red': '--api red',
                     'dcm': '--api dc',
                     'dv': '--api dv'}
        processor_to_run.launch_task('.run_processor', _(post_body),
                                     running_user=current_user.id,
                                     processor_args=arg_trans[processor_args])
        processor_to_run.last_run_time = datetime.utcnow()
        db.session.commit()
    if not redirect_dest or redirect_dest == 'Page':
        return redirect(url_for('main.processor_page',
                                processor_name=processor_to_run.name))
    elif redirect_dest == 'Basic':
        return redirect(url_for('main.edit_processor',
                                processor_name=processor_to_run.name))
    elif redirect_dest == 'Import':
        return redirect(url_for('main.edit_processor_import',
                                processor_name=processor_to_run.name))
    elif redirect_dest == 'Clean':
        return redirect(url_for('main.edit_processor_clean',
                                processor_name=processor_to_run.name))
    elif redirect_dest == 'Export':
        return redirect(url_for('main.edit_processor_export',
                                processor_name=processor_to_run.name))
    else:
        return redirect(url_for('main.processor_page',
                                processor_name=processor_to_run.name))


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
        msg_text = ('Attempting to save media plan for  processor: {}'
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
            return redirect(url_for('main.processor_page',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.edit_processor_request_fix',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **kwargs)


@bp.route('/processor/<processor_name>/edit/fix/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_processor_request_fix_upload_file(processor_name):
    current_key, object_name, object_form = parse_upload_file_request(request)
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
    db.session.commit()
    return jsonify({'data': 'success'})


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
    form = ProcessorRequestFixForm()
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
    form = ProcessorRequestFixForm()
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
    msg_txt = 'The fix #{} has been marked as unresolved!'.format(request_fix.id)
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
                           user=cur_user, uploaders=uploaders.items,
                           next_url=next_url, prev_url=prev_url)


def check_base_uploader_object(uploader_id, object_level='Campaign'):
    new_uploader = Uploader.query.get(uploader_id)
    upo = UploaderObjects.query.filter_by(uploader_id=new_uploader.id,
                                          object_level=object_level)
    if not upo:
        upo = UploaderObjects(uploader_id=new_uploader.id,
                              object_level=object_level)
        if new_uploader.media_plan:
            if object_level == 'Campaign':
                upo.media_plan_columns = [
                    'Campaign ID', 'Placement Phase\n(If Needed) ',
                    'Partner Name', 'Country', 'Creative\n(If Needed)']
            elif object_level == 'Adset':
                upo.media_plan_columns = ['Placement Name']
            upo.file_filter = 'Facebook|Instagram'
        else:
            pass
        db.session.add(upo)
        db.commit()
    return jsonify({'data': 'success'})


def check_relation_uploader_objects(uploader_id, object_level='Campaign'):
    import uploader.upload.fbapi as up_fbapi
    new_uploader = Uploader.query.get(uploader_id)
    upo = UploaderObjects.query.filter_by(uploader_id=new_uploader.id,
                                          object_level=object_level).first()
    if object_level == 'Campaign':
        relation_column_names = {
            up_fbapi.CampaignUpload.objective: 'PAUSED',
            up_fbapi.CampaignUpload.spend_cap: '1000',
            up_fbapi.CampaignUpload.status: 'LINK_CLICKS'}
    elif object_level == 'Adset':
        relation_column_names = []
    else:
        relation_column_names = []
    for col in relation_column_names:
        relation = UploaderRelations.query.filter_by(
            uploader_objects_id=upo.id, impacted_column_name=col).first()
        if not relation:
            new_relation = UploaderRelations(
                uploader_objects_id=upo.id, impacted_column_name=col,
                relation_constant=relation_column_names[col])
            db.session.add(new_relation)
            db.session.commit()


def create_base_uploader_objects(uploader_id):
    for obj in ['Campaign', 'Adset', 'Ad']:
        check_base_uploader_object(uploader_id, obj)
        check_relation_uploader_objects(uploader_id, obj)
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
            local_path=new_path, campaign_id=form_campaign.id)
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
            return redirect(url_for('main.edit_uploader',
                                    uploader_name=new_uploader.name))
        else:
            return redirect(url_for('main.upload'))
    return render_template('create_processor.html', user=cur_user,
                           title=_('Uploader'), form=form, edit_progress="25",
                           edit_name='Basic')


@bp.route('/uploader/<uploader_name>/run/<redirect_dest>/<uploader_args>',
          methods=['GET', 'POST'])
@login_required
def run_uploader(uploader_name, uploader_args='', redirect_dest=None):
    uploader_to_run = Uploader.query.filter_by(
        name=uploader_name).first_or_404()
    if uploader_to_run.get_task_in_progress('.run_uploader'):
        flash(_('The uploader is already running.'))
    else:
        post_body = ('Running {} for uploader: {}...'.format(
            uploader_args, uploader_name))
        arg_trans = {'create': '--create',
                     'campaign': '--api fb --upload c',
                     'adset': '--api fb --upload as',
                     'ad': '--api fb --upload ad'}
        uploader_to_run.launch_task('.run_uploader', _(post_body),
                                    running_user=current_user.id,
                                    uploader_args=arg_trans[uploader_args])
        uploader_to_run.last_run_time = datetime.utcnow()
        db.session.commit()
    if not redirect_dest or redirect_dest == 'Page':
        return redirect(url_for('main.uploader_page',
                                uploader_name=uploader_to_run.name))
    elif redirect_dest == 'Basic':
        return redirect(url_for('main.edit_uploader',
                                uploader_name=uploader_to_run.name))
    elif redirect_dest == 'Campaigns':
        return redirect(url_for('main.edit_uploader_campaign',
                                uploader_name=uploader_to_run.name))
    elif redirect_dest == 'Creative':
        return redirect(url_for('main.edit_uploader_creative',
                                uploader_name=uploader_to_run.name))
    else:
        return redirect(url_for('main.uploader_page',
                                uploader_name=uploader_to_run.name))


def get_uploader_run_links(uploader_name, edit_name):
    run_links = {}
    for idx, run_arg in enumerate(
            ('create', 'campaign', 'adset', 'ad')):
        run_url = url_for('main.run_uploader', uploader_name=uploader_name,
                          redirect_dest=edit_name, uploader_args=run_arg)
        run_href = run_url
        run_link = dict(title=run_arg.capitalize(),
                        url=run_url,
                        href=run_href)
        run_links[idx] = run_link
    return run_links


def get_uploader_edit_links():
    edit_links = {}
    for idx, edit_file in enumerate(
            ('Creator', 'Campaign', 'Adset', 'Ad')):
        edit_links[idx] = dict(title=edit_file, nest=[])
        if edit_file == 'Relation':
            edit_links[idx]['nest'] = ['Campaign', 'Targeting', 'Creative',
                                       'Vendor', 'Country', 'Serving', 'Copy']
    return edit_links


def get_current_uploader(uploader_name, current_page, edit_progress=0,
                         edit_name='Page', buttons='Uploader'):
    cur_up = Uploader.query.filter_by(name=uploader_name).first_or_404()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    page = request.args.get('page', 1, type=int)
    posts = (Post.query.
             filter_by(uploader_id=cur_up.id).
             order_by(Post.timestamp.desc()).
             paginate(page, 5, False))
    run_links = get_uploader_run_links(uploader_name, edit_name)
    edit_links = get_uploader_edit_links()
    # output_links = get_uploader_output_links()
    # request_links = get_uploader_request_links(processor_name)
    args = {'object': cur_up, 'posts': posts.items, 'title': _('Uploader'),
            'object_name': cur_up.name, 'user': cur_user,
            'edit_progress': edit_progress, 'edit_name': edit_name,
            'buttons': get_navigation_buttons(buttons),
            'object_function_call': {'uploader_name': cur_up.name},
            'run_links': run_links, 'edit_links': edit_links}
    next_url = url_for('main.' + current_page, uploader_name=cur_up.name,
                       page=posts.next_num) if posts.has_next else None
    prev_url = url_for('main.' + current_page, uploader_name=cur_up.name,
                       page=posts.prev_num) if posts.has_prev else None
    args['prev_url'] = prev_url
    args['next_url'] = next_url
    return args


@bp.route('/uploader/<uploader_name>')
@login_required
def uploader_page(uploader_name):
    kwargs = get_current_uploader(uploader_name, 'uploader_page',
                                  edit_progress=100, edit_name='Page')
    return render_template('create_processor.html', **kwargs)


@bp.route('/uploader/<uploader_name>/edit/upload_file',
          methods=['GET', 'POST'])
@login_required
def edit_uploader_upload_file(uploader_name):
    current_key, object_name, object_form = parse_upload_file_request(request)
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    mem, file_name = get_file_in_memory_from_request(request, current_key)
    check_and_add_media_plan(mem, cur_up, object_type=Uploader)
    cur_up.media_plan = True
    db.session.commit()
    return jsonify({'data': 'success'})


@bp.route('/uploader/<uploader_name>/edit', methods=['GET', 'POST'])
@login_required
def edit_uploader(uploader_name):
    kwargs = get_current_uploader(uploader_name, 'edit_uploader',
                                  edit_progress=25, edit_name='Basic')
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
        uploader_to_edit.fb_account_id = form.account_id.data
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
        form.account_id.data = uploader_to_edit.fb_account_id
    kwargs['form'] = form
    return render_template('create_processor.html',  **kwargs)


@bp.route('/uploader/<uploader_name>/edit/campaign', methods=['GET', 'POST'])
@login_required
def edit_uploader_campaign(uploader_name):
    kwargs = get_current_uploader(uploader_name, 'edit_uploader',
                                  edit_progress=25, edit_name='Campaigns')
    cur_up = kwargs['object']
    up_cam = UploaderObjects.query.filter_by(
        uploader_id=cur_up.id, object_level='Campaign').first()
    if cur_up.media_plan:
        form_object = EditUploaderCampaignMediaPlanForm
        relations = form_object().set_relations(UploaderRelations, up_cam)
        form = form_object(relations=relations)
        if request.method == 'POST':
            form.validate()
            up_cam.media_plan_columns = form.media_plan_columns.data
            up_cam.partner_filter = form.partner_name_filter.data
            db.session.commit()
            msg_text = 'Creating and uploading campaigns for uploader.'
            cur_up.launch_task(
                '.uploader_create_campaigns', _(msg_text),
                running_user=current_user.id)
            db.session.commit()
            if form.form_continue.data == 'continue':
                return redirect(url_for('main.edit_uploader_campaign',
                                        uploader_name=cur_up.name))
            else:
                return redirect(url_for('main.edit_uploader_campaign',
                                        uploader_name=cur_up.name))
        elif request.method == 'GET':
            form.media_plan_columns = up_cam.media_plan_columns
            form.partner_name_filter = up_cam.partner_filter
    else:
        form_object = EditUploaderCampaignCreateForm
        form = form_object(uploader_name)
    kwargs['form'] = form
    return render_template('create_processor.html',  **kwargs)


@bp.route('/uploader/<uploader_name>/edit/creative', methods=['GET', 'POST'])
@login_required
def edit_uploader_creative(uploader_name):
    kwargs = get_current_uploader(uploader_name, 'edit_uploader_creative',
                                  edit_progress=25, edit_name='Creative')
    # uploader_to_edit = kwargs['object']
    form = EditUploaderCreativeForm(uploader_name)
    kwargs['form'] = form
    return render_template('create_processor.html',  **kwargs)


@bp.route('/uploader/<uploader_name>/edit/creative/upload_file',
          methods=['GET', 'POST'])
@login_required
def uploader_file_upload(uploader_name):
    current_key, object_name, object_form = parse_upload_file_request(request)
    cur_up = Uploader.query.filter_by(name=object_name).first_or_404()
    mem, file_name = get_file_in_memory_from_request(request, 'creative_file')
    msg_text = 'Saving file {} for {}'.format(file_name, cur_up.name)
    cur_up.launch_task(
        '.uploader_save_creative', _(msg_text),
        running_user=current_user.id, file=mem, file_name=file_name)
    db.session.commit()
    return jsonify({'data': 'success'})


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
