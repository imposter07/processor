import time
from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, \
    jsonify, current_app
from flask_login import current_user, login_required
from flask_babel import _, get_locale
from guess_language import guess_language
from app import db
from app.main.forms import EditProfileForm, PostForm, SearchForm, MessageForm, \
    ProcessorForm, EditProcessorForm, ImportForm, ProcessorCleanForm
from app.models import User, Post, Message, Notification, Processor, \
    Client, Product, Campaign, ProcessorDatasources
from app.translate import translate
from app.main import bp


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
                           next_url=next_url, prev_url=prev_url)


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
                           next_url=next_url, prev_url=prev_url)


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
    processors = current_user.processor.order_by(
        Processor.last_run_time.desc()).paginate(
        page, current_app.config['POSTS_PER_PAGE'], False)
    next_url = (url_for('main.processor', username=cur_user.username,
                        page=processors.next_num)
                if processors.has_next else None)
    prev_url = (url_for('main.processor', username=cur_user.username,
                        page=processors.prev_num)
                if processors.has_prev else None)
    return render_template('processor.html', user=cur_user,
                           processors=processors.items,
                           next_url=next_url, prev_url=prev_url)


@bp.route('/create_processor', methods=['GET', 'POST'])
@login_required
def create_processor():
    form = ProcessorForm()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    if form.validate_on_submit():
        form_client = Client(name=form.client_name).check_and_add()
        form_product = Product(
            name=form.product_name, client_id=form_client.id).check_and_add()
        form_campaign = Campaign(
            name=form.campaign_name, product_id=form_product.id).check_and_add()
        new_processor = Processor(
            name=form.name.data, description=form.description.data,
            user_id=current_user.id, created_at=datetime.utcnow(),
            local_path=form.local_path.data,
            tableau_workbook=form.tableau_workbook.data,
            tableau_view=form.tableau_view.data, campaign_id=form_campaign.id)
        db.session.add(new_processor)
        db.session.commit()
        post_body = ('Create Processor {}...'.format(new_processor.name))
        new_processor.launch_task('.create_processor', _(post_body),
                                  current_user.id,
                                  current_app.config['BASE_PROCESSOR_PATH'])
        creation_text = 'Processor was requested for creation.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=new_processor.id)
        db.session.add(post)
        db.session.commit()
        if form.submit_continue.data:
            return redirect(url_for('main.edit_processor_import',
                                    processor_name=new_processor.name))
        else:
            return redirect(url_for('main.processor'))
    return render_template('create_processor.html', user=cur_user,
                           title=_('Processor'), form=form, edit_progress="25",
                           edit_name='Basic')


@bp.route('/processor/<processor_name>/edit/import', methods=['GET', 'POST'])
@login_required
def edit_processor_import(processor_name):
    cur_proc = Processor.query.filter_by(name=processor_name).first_or_404()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    apis = ImportForm().set_apis(ProcessorDatasources, cur_proc)
    form = ImportForm(apis=apis)
    template_arg = {'processor_name': cur_proc.name, 'user': cur_user,
                    'title': _('Processor'), 'form': form, 'edit_progress': 50,
                    'edit_name': 'Import'}
    if form.add_child.data:
        form.apis.append_entry()
        template_arg['form'] = form
        return render_template('create_processor.html', **template_arg)
    if form.remove_api.data:
        form.apis.pop_entry()
        template_arg['form'] = form
        return render_template('create_processor.html', **template_arg)
    if form.refresh.data:
        task = cur_proc.launch_task('.get_processor_sources',
                                    _('Refreshing imports.'),
                                    running_user=current_user.id)
        db.session.commit()
        job = task.wait_and_get_job()
        db.session.commit()
        apis = ImportForm().set_apis(ProcessorDatasources, cur_proc)
        template_arg['form']  = ImportForm(apis=apis)
        return render_template('create_processor.html', **template_arg)
    if form.validate_on_submit():
        cur_proc.launch_task('.set_processor_imports', _('Setting imports.'),
                             running_user=current_user.id,
                             form_imports=form.apis.data)
        db.session.commit()
        if form.submit_continue.data:
            return redirect(url_for('main.edit_processor_clean',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.processor_page',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **template_arg)


def adjust_path(path):
    for x in [['S:', '/mnt/s'], ['C:', '/mnt/c'], ['c:', '/mnt/c'],
              ['\\', '/']]:
        path = path.replace(x[0], x[1])
    return path


@bp.route('/processor/<processor_name>/edit/clean/log', methods=['GET', 'POST'])
@login_required
def get_processor_logfile(processor_name):
    import os
    cur_proc = Processor.query.filter_by(name=processor_name).first_or_404()
    def generate():
        with open(os.path.join(adjust_path(cur_proc.local_path),
                               'logfile.log')) as f:
            while True:
                yield f.read()
                time.sleep(1)
    return current_app.response_class(generate(), mimetype='text/plain')


@bp.route('/processor/<processor_name>/edit/clean/<datasource>')
@login_required
def edit_processor_clean_fpn(processor_name, datasource):
    cur_proc = Processor.query.filter_by(name=processor_name).first_or_404()


@bp.route('/processor/<processor_name>/edit/clean', methods=['GET', 'POST'])
@login_required
def edit_processor_clean(processor_name):
    cur_proc = Processor.query.filter_by(name=processor_name).first_or_404()
    cur_user = User.query.filter_by(id=current_user.id).first_or_404()
    proc_arg = {'running_user': current_user.id}
    ds = ProcessorCleanForm().set_datasources(ProcessorDatasources, cur_proc)
    form = ProcessorCleanForm(datasources=ds)
    template_arg = {'processor_name': cur_proc.name, 'user': cur_user,
                    'title': _('Processor'), 'form': form, 'edit_progress': 75,
                    'edit_name': "Clean"}
    if form.refresh_data_sources.data:
        task = cur_proc.launch_task(
            '.get_processor_sources', _('Refreshing data.'), **proc_arg)
        db.session.commit()
        task.wait_and_get_job()
        ds = ProcessorCleanForm().set_datasources(ProcessorDatasources, cur_proc)
        template_arg['form'] = ProcessorCleanForm(datasources=ds)
        return render_template('create_processor.html', **template_arg)
    for ds in form.datasources:
        if ds.refresh_data_source.data:
            vk = ds.vendor_key.data
            proc_arg['vk'] = vk
            task = cur_proc.launch_task(
                '.get_dict_order', _('Getting dict order table.'), **proc_arg)
            db.session.commit()
            job = task.wait_and_get_job()
            template_arg['tables'] = job.result
            return render_template('create_processor.html', **template_arg)
        elif ds.refresh_dict.data:
            vk = ds.vendor_key.data
            proc_arg['vk'] = vk
            task = cur_proc.launch_task(
                '.get_dict_order', _('Getting dict order table.'), **proc_arg)
            db.session.commit()
            job = task.wait_and_get_job()
            template_arg['tables'] = job.result
            return render_template('create_processor.html', **template_arg)
        """
        elif ds.full_placement_columns.data:
            vk = ds.vendor_key.data
            proc_arg['vk'] = vk
            task = cur_proc.launch_task(
                '.get_dict_order', _('Getting dict order table.'), **proc_arg)
            db.session.commit()
            job = task.wait_and_get_job()
            template_arg['tables'] = job.result
            return render_template('create_processor.html', **template_arg)
        """
    if form.validate_on_submit():
        cur_proc.launch_task('.set_processor_imports', _('Setting imports.'),
                             running_user=current_user.id,
                             form_imports=form.apis.data)
        db.session.commit()
        if form.submit_continue.data:
            return redirect(url_for('main.edit_processor_export',
                                    processor_name=cur_proc.name))
        else:
            return redirect(url_for('main.processor_page',
                                    processor_name=cur_proc.name))
    return render_template('create_processor.html', **template_arg)


@bp.route('/processor/<processor_name>')
@login_required
def processor_page(processor_name):
    processor_for_page = Processor.query.filter_by(
        name=processor_name).first_or_404()
    page = request.args.get('page', 1, type=int)
    posts = (Post.query.
             filter_by(processor_id=processor_for_page.id).
             order_by(Post.timestamp.desc()).
             paginate(page, current_app.config['POSTS_PER_PAGE'], False))
    next_url = url_for('main.explore', page=posts.next_num) \
        if posts.has_next else None
    prev_url = url_for('main.explore', page=posts.prev_num) \
        if posts.has_prev else None
    return render_template('processor_page.html', processor=processor_for_page,
                           posts=posts.items, next_url=next_url,
                           prev_url=prev_url)


@bp.route('/processor/<processor_name>/popup')
@login_required
def processor_popup(processor_name):
    processor_for_page = Processor.query.filter_by(
        name=processor_name).first_or_404()
    return render_template('processor_popup.html', processor=processor_for_page)


@bp.route('/processor/<processor_name>/run/<redirect>/<processor_args>',
          methods=['GET', 'POST'])
@login_required
def run_processor(processor_name, processor_args='', redirect=None):
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
                     'basic': ''}
        processor_to_run.launch_task('.run_processor', _(post_body),
                                     running_user=current_user.id,
                                     processor_args=arg_trans[processor_args])
        processor_to_run.last_run_time = datetime.utcnow()
        post = Post(body=post_body, author=current_user,
                    processor_id=processor_to_run.id)
        db.session.add(post)
        db.session.commit()
    if not redirect:
        return redirect(url_for('main.processor_page',
                                processor_name=processor_name))
    elif redirect == 'Basic':
        return redirect(url_for('main.edit_processor',
                                processor_name=processor_to_run.name))
    elif redirect =='Import':
        return redirect(url_for('main.edit_processor_import',
                                processor_name=processor_to_run.name))
    elif redirect =='Clean':
        return redirect(url_for('main.edit_processor_clean',
                                processor_name=processor_to_run.name))


@bp.route('/processor/<processor_name>/edit', methods=['GET', 'POST'])
@login_required
def edit_processor(processor_name):
    processor_to_edit = Processor.query.filter_by(
        name=processor_name).first_or_404()
    form = EditProcessorForm(processor_name)
    if form.validate_on_submit():
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
        processor_to_edit.campaign_id = form_campaign.id
        db.session.commit()
        flash(_('Your changes have been saved.'))
        post_body = ('Create Processor {}...'.format(processor_to_edit.name))
        processor_to_edit.launch_task('.create_processor', _(post_body),
                                      current_user.id,
                                      current_app.config['BASE_PROCESSOR_PATH'])
        creation_text = 'Processor was requested for creation.'
        flash(_(creation_text))
        post = Post(body=creation_text, author=current_user,
                    processor_id=processor_to_edit.id)
        db.session.add(post)
        db.session.commit()
        if form.submit_continue.data:
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
        form_campaign = Campaign.query.filter_by(
            id=processor_to_edit.campaign_id).first_or_404()
        form_product = Product.query.filter_by(
            id=form_campaign.product_id).first_or_404()
        form_client = Client.query.filter_by(
            id=form_product.client_id).first_or_404()
        form.cur_campaign.data = form_campaign
        form.cur_product.data = form_product
        form.cur_client.data = form_client
    return render_template('create_processor.html',
                           processor_name=processor_to_edit.name,
                           title=_('Edit Processor'), form=form,
                           edit_progress="25", edit_name="Basic")
