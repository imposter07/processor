import rq
import ast
import jwt
import pytz
import json
import time
import redis
from datetime import datetime, timedelta
from datetime import time as datetime_time
from hashlib import md5
from flask import current_app, url_for
from flask_login import UserMixin
import processor.reporting.vmcolumns as vmc
from werkzeug.security import generate_password_hash, check_password_hash
from app import db, login
from app.search import add_to_index, remove_from_index, query_index


class SearchableMixin(db.Model):
    __abstract__ = True

    @classmethod
    def search(cls, expression, page, per_page):
        ids, total = query_index(cls.__tablename__, expression, page, per_page)
        if total == 0:
            return cls.query.filter_by(id=0), 0
        when = []
        for i in range(len(ids)):
            when.append((ids[i], i))
        return cls.query.filter(cls.id.in_(ids)).order_by(
            db.case(when, value=cls.id)), total

    @classmethod
    def before_commit(cls, session):
        session._changes = {
            'add': list(session.new),
            'update': list(session.dirty),
            'delete': list(session.deleted)
        }

    @classmethod
    def after_commit(cls, session):
        try:
            for obj in session._changes['add']:
                if isinstance(obj, SearchableMixin):
                    add_to_index(obj.__tablename__, obj)
            for obj in session._changes['update']:
                if isinstance(obj, SearchableMixin):
                    add_to_index(obj.__tablename__, obj)
            for obj in session._changes['delete']:
                if isinstance(obj, SearchableMixin):
                    remove_from_index(obj.__tablename__, obj)
            session._changes = None
        except:
            session._changes = None
            return

    @classmethod
    def reindex(cls):
        for obj in cls.query:
            add_to_index(cls.__tablename__, obj)


db.event.listen(db.session, 'before_commit', SearchableMixin.before_commit)
db.event.listen(db.session, 'after_commit', SearchableMixin.after_commit)


followers = db.Table(
    'followers',
    db.Column('follower_id', db.Integer, db.ForeignKey('user.id')),
    db.Column('followed_id', db.Integer, db.ForeignKey('user.id'))
)


processor_followers = db.Table(
    'processor_followers',
    db.Column('follower_id', db.Integer, db.ForeignKey('user.id')),
    db.Column('followed_id', db.Integer, db.ForeignKey('processor.id'))
)


project_number_processor = db.Table(
    'project_number_processor',
    db.Column('project_id', db.Integer, db.ForeignKey('project.id')),
    db.Column('processor_id', db.Integer, db.ForeignKey('processor.id'))
)


user_tutorial = db.Table(
    'user_tutorial',
    db.Column('tutorial_stage_id', db.Integer,
              db.ForeignKey('tutorial_stage.id')),
    db.Column('user_id', db.Integer, db.ForeignKey('user.id')),
)


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True)
    email = db.Column(db.String(120), index=True, unique=True)
    password_hash = db.Column(db.String(128))
    posts = db.relationship('Post', backref='author', lazy='dynamic')
    about_me = db.Column(db.String(140))
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    followed = db.relationship(
        'User', secondary=followers,
        primaryjoin=(followers.c.follower_id == id),
        secondaryjoin=(followers.c.followed_id == id),
        backref=db.backref('followers', lazy='dynamic'), lazy='dynamic')
    processor_followed = db.relationship(
        'Processor', secondary=processor_followers,
        primaryjoin=(processor_followers.c.follower_id == id),
        secondaryjoin="processor_followers.c.followed_id == Processor.id",
        backref=db.backref('processor_followers', lazy='dynamic'),
        lazy='dynamic')
    messages_sent = db.relationship('Message',
                                    foreign_keys='Message.sender_id',
                                    backref='author', lazy='dynamic')
    messages_received = db.relationship('Message',
                                        foreign_keys='Message.recipient_id',
                                        backref='recipient', lazy='dynamic')
    last_message_read_time = db.Column(db.DateTime)
    notifications = db.relationship('Notification', backref='user',
                                    lazy='dynamic')
    tasks = db.relationship('Task', backref='user', lazy='dynamic')
    processor = db.relationship('Processor', foreign_keys='Processor.user_id',
                                backref='user', lazy='dynamic')
    processor_request_user = db.relationship(
        'Processor', foreign_keys='Processor.requesting_user_id',
        backref='request_user', lazy='dynamic')
    uploader = db.relationship('Uploader', foreign_keys='Uploader.user_id',
                               backref='user', lazy='dynamic')
    schedule = db.relationship('TaskScheduler', backref='user', lazy='dynamic')
    dashboard = db.relationship('Dashboard', backref='user', lazy='dynamic')
    notes = db.relationship('Notes', backref='user', lazy='dynamic')
    tutorial_stages_completed = db.relationship(
        'TutorialStage', secondary=user_tutorial,
        primaryjoin=(user_tutorial.c.user_id == id),
        secondaryjoin="user_tutorial.c.tutorial_stage_id == TutorialStage.id",
        backref=db.backref('user_tutorial', lazy='dynamic'),
        lazy='dynamic')

    def __repr__(self):
        return '<User {}>'.format(self.username)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def avatar(self, size):
        digest = md5(self.email.lower().encode('utf-8')).hexdigest()
        return 'https://www.gravatar.com/avatar/{}?d=identicon&s={}'.format(
            digest, size)

    def follow(self, user):
        if not self.is_following(user):
            self.followed.append(user)

    def unfollow(self, user):
        if self.is_following(user):
            self.followed.remove(user)

    def is_following(self, user):
        return self.followed.filter(
            followers.c.followed_id == user.id).count() > 0

    def followed_posts(self):
        followed = Post.query.join(
            followers, (followers.c.followed_id == Post.user_id)).filter(
                followers.c.follower_id == self.id)
        own = Post.query.filter_by(user_id=self.id)
        processor_followed = Post.query.join(
            processor_followers, (processor_followers.c.followed_id ==
                                  Post.processor_id)).filter(
                processor_followers.c.follower_id == self.id)
        all_posts = followed.union(own).union(processor_followed).order_by(
            Post.timestamp.desc())
        return all_posts

    def follow_processor(self, processor):
        if not self.is_following_processor(processor):
            self.processor_followed.append(processor)

    def unfollow_processor(self, processor):
        if self.is_following_processor(processor):
            self.processor_followed.remove(processor)

    def is_following_processor(self, processor):
        return self.processor_followed.filter(
            processor_followers.c.followed_id == processor.id).count() > 0

    def complete_tutorial_stage(self, tutorial_stage):
        if tutorial_stage not in self.tutorial_stages_completed.all():
            self.tutorial_stages_completed.append(tutorial_stage)

    def get_reset_password_token(self, expires_in=600):
        return jwt.encode(
            {'reset_password': self.id, 'exp': time.time() + expires_in},
            current_app.config['SECRET_KEY'],
            algorithm='HS256').decode('utf-8')

    @staticmethod
    def verify_reset_password_token(token):
        try:
            user_id = jwt.decode(token, current_app.config['SECRET_KEY'],
                                 algorithms=['HS256'])['reset_password']
        except:
            return
        return User.query.get(user_id)

    def new_messages(self):
        last_read_time = self.last_message_read_time or datetime(1900, 1, 1)
        return Message.query.filter_by(recipient=self).filter(
            Message.timestamp > last_read_time).count()

    def add_notification(self, name, data):
        self.notifications.filter_by(name=name).delete()
        n = Notification(name=name, payload_json=json.dumps(data), user=self)
        db.session.add(n)
        return n

    def launch_task(self, name, description, *args, **kwargs):
        rq_job = current_app.task_queue.enqueue('app.tasks' + name,
                                                self.id, *args, **kwargs)
        task = Task(id=rq_job.get_id(), name=name, description=description,
                    user=self)
        db.session.add(task)
        return task

    def get_tasks_in_progress(self):
        return Task.query.filter_by(user=self, complete=False).all()

    def get_task_in_progress(self, name):
        return Task.query.filter_by(name=name, user=self,
                                    complete=False).first()


@login.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


class Post(SearchableMixin, db.Model):
    __searchable__ = ['body']
    id = db.Column(db.Integer, primary_key=True)
    body = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    language = db.Column(db.String(5))
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    uploader_id = db.Column(db.Integer, db.ForeignKey('uploader.id'))
    request_id = db.Column(db.Integer, db.ForeignKey('requests.id'))
    note_id = db.Column(db.Integer, db.ForeignKey('notes.id'))

    def __repr__(self):
        return '<Post {}>'.format(self.body)

    def processor_run_success(self):
        return self.body[-17:] == 'finished running.'

    def processor_run_failed(self):
        return self.body[-11:] == 'run failed.'


class Message(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sender_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    recipient_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    body = db.Column(db.String(140))
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)

    def __repr__(self):
        return '<Message {}>'.format(self.body)

    def processor_run_success(self):
        return self.body[-17:] == 'finished running.'

    def processor_run_failed(self):
        return self.body[-11:] == 'run failed.'


class Notification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    timestamp = db.Column(db.Float, index=True, default=time.time())
    payload_json = db.Column(db.Text)

    def get_data(self):
        return json.loads(str(self.payload_json))


class Task(db.Model):
    id = db.Column(db.String(36), primary_key=True)
    name = db.Column(db.Text, index=True)
    description = db.Column(db.Text)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    complete = db.Column(db.Boolean, default=False)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    uploader_id = db.Column(db.Integer, db.ForeignKey('uploader.id'))

    def get_rq_job(self):
        try:
            rq_job = rq.job.Job.fetch(self.id, connection=current_app.redis)
        except (redis.exceptions.RedisError, rq.exceptions.NoSuchJobError):
            return None
        return rq_job

    def get_progress(self):
        job = self.get_rq_job()
        return job.meta.get('progress', 0) if job is not None else 100

    def wait_for_job(self, loops=1000):
        for x in range(loops):
            if self.get_progress() == 100:
                return True
            else:
                time.sleep(1.5)
        return False

    def check_return_value(self, job, force_return):
        if force_return and not job.result:
            job = self.get_rq_job()
            time.sleep(5)
            if not job.result:
                time.sleep(1)
                self.check_return_value(job, force_return)
        return job

    def wait_and_get_job(self, loops=1000, force_return=False):
        completed = self.wait_for_job(loops=loops)
        if completed:
            job = self.get_rq_job()
            job = self.check_return_value(job, force_return)
        else:
            job = None
        return job


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    product = db.relationship('Product', backref='client', lazy='dynamic')

    def check(self):
        client_check = Client.query.filter_by(name=self.name).first()
        return client_check

    def check_and_add(self):
        client_check = self.check()
        if not client_check:
            client_check = Client(name=self.name)
            db.session.add(client_check)
            db.session.commit()
            client_check = self.check()
        return client_check


class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'))
    campaign = db.relationship('Campaign', backref='product', lazy='dynamic')

    def check(self):
        product_check = Product.query.filter_by(
            name=self.name, client_id=self.client_id).first()
        return product_check

    def check_and_add(self):
        product_check = self.check()
        if not product_check:
            product_check = Product(name=self.name, client_id=self.client_id)
            db.session.add(product_check)
            db.session.commit()
            product_check = self.check()
        return product_check


class Campaign(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    product_id = db.Column(db.Integer, db.ForeignKey('product.id'))
    processor = db.relationship('Processor', backref='campaign', lazy='dynamic')
    uploader = db.relationship('Uploader', backref='campaign', lazy='dynamic')

    def check(self):
        campaign_check = Campaign.query.filter_by(
            name=self.name, product_id=self.product_id).first()
        return campaign_check

    def check_and_add(self):
        campaign_check = self.check()
        if not campaign_check:
            campaign_check = Campaign(name=self.name,
                                      product_id=self.product_id)
            db.session.add(campaign_check)
            db.session.commit()
            campaign_check = self.check()
        return campaign_check

    def get_objects(self, object_type):
        return self.uploader if object_type == 'Uploader' else self.processor


class RateCard(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    owner_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    name = db.Column(db.Text)
    description = db.Column(db.Text)
    processor = db.relationship('Processor', backref='rate_card',
                                lazy='dynamic')
    rates = db.relationship('Rates', backref='rate_card', lazy='dynamic')


class Rates(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    type_name = db.Column(db.Text)
    adserving_fee = db.Column(db.Numeric)
    reporting_fee = db.Column(db.Numeric)
    rate_card_id = db.Column(db.Integer, db.ForeignKey('rate_card.id'))


class Conversion(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    conversion_name = db.Column(db.Text)
    conversion_type = db.Column(db.Text)
    key = db.Column(db.String(64))
    dcm_category = db.Column(db.Text)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))

    def get_form_dict(self):
        form_dict = {
            'key': self.key,
            'conversion_name': self.conversion_name,
            'conversion_type': self.conversion_type,
            'dcm_category': self.dcm_category
        }
        return form_dict

    def set_from_form(self, form, current_processor):
        self.processor_id = current_processor.id
        self.key = form['key']
        self.conversion_name = form['conversion_name']
        self.conversion_type = form['conversion_type']
        self.dcm_category = form['dcm_category']


class Processor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    description = db.Column(db.String(128))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    local_path = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_run_time = db.Column(db.DateTime, default=datetime.utcnow)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    tableau_workbook = db.Column(db.Text)
    tableau_view = db.Column(db.Text)
    tableau_datasource = db.Column(db.Text)
    requesting_user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    plan_path = db.Column(db.Text)
    first_report_ = db.Column(db.Date)
    digital_agency_fees = db.Column(db.Numeric)
    trad_agency_fees = db.Column(db.Numeric)
    dcm_service_fees = db.Column(db.Numeric)
    tasks = db.relationship('Task', backref='processor', lazy='dynamic')
    posts = db.relationship('Post', backref='processor', lazy='dynamic')
    task_scheduler = db.relationship('TaskScheduler', backref='processor',
                                     lazy='dynamic')
    conversions = db.relationship('Conversion', backref='processor',
                                  lazy='dynamic')
    campaign_id = db.Column(db.Integer, db.ForeignKey('campaign.id'))
    rate_card_id = db.Column(db.Integer, db.ForeignKey('rate_card.id'))
    processor_datasources = db.relationship('ProcessorDatasources',
                                            backref='processor', lazy='dynamic')
    accounts = db.relationship('Account', backref='processor', lazy='dynamic')
    requests = db.relationship('Requests', backref='processor', lazy='dynamic')
    notes = db.relationship('Notes', backref='processor', lazy='dynamic')
    processor_analysis = db.relationship('ProcessorAnalysis',
                                         backref='processor', lazy='dynamic')
    dashboard = db.relationship(
        'Dashboard', backref='processor', lazy='dynamic')
    projects = db.relationship(
        'Project', secondary=project_number_processor,
        primaryjoin=(project_number_processor.c.processor_id == id),
        secondaryjoin="project_number_processor.c.project_id == Project.id",
        backref=db.backref('project_number_processor', lazy='dynamic'),
        lazy='dynamic')

    def launch_task(self, name, description, running_user, *args, **kwargs):
        rq_job = current_app.task_queue.enqueue('app.tasks' + name,
                                                self.id, running_user,
                                                *args, **kwargs)
        task = Task(id=rq_job.get_id(), name=name, description=description,
                    user_id=self.user_id, processor=self)
        db.session.add(task)
        return task

    def schedule_job(self, name, description, start_date, end_date,
                     scheduled_time, interval):
        eastern = pytz.timezone('US/Eastern')
        today = eastern.localize(datetime.today())
        if not scheduled_time:
            scheduled_time = datetime_time(8, 0, 0)
        first_run = datetime.combine(start_date, scheduled_time)
        first_run = eastern.localize(first_run)
        if first_run < today:
            tomorrow = today.date() + timedelta(days=1)
            first_run = datetime.combine(tomorrow, scheduled_time)
            first_run = eastern.localize(first_run)
        first_run = first_run.astimezone(pytz.utc)
        interval_sec = int(interval) * 60 * 60
        repeat = (end_date - start_date).days * (24 / int(interval))
        job = current_app.scheduler.schedule(
            scheduled_time=first_run,
            func='app.tasks' + name,
            kwargs={'processor_id': self.id,
                    'current_user_id': self.user_id,
                    'processor_args': 'full'},
            interval=int(interval_sec),
            repeat=int(repeat),
            timeout=32400,
            result_ttl=None
        )
        schedule = TaskScheduler(
            id=job.get_id(), name=name, description=description,
            created_at=datetime.utcnow(), scheduled_time=scheduled_time,
            start_date=start_date, end_date=end_date, interval=interval,
            user_id=self.user_id, processor=self)
        db.session.add(schedule)
        task = Task(id=job.get_id(), name=name, description=description,
                    user_id=self.user_id, processor=self, complete=True)
        db.session.add(task)
        return schedule

    def get_tasks_in_progress(self):
        return Task.query.filter_by(processor=self, complete=False).all()

    def get_task_in_progress(self, name):
        return Task.query.filter_by(name=name, processor=self,
                                    complete=False).first()

    def run(self, processor_args, current_user):
        post_body = ('Running {} for processor: {}...'.format(
            processor_args, self.name))
        self.launch_task('.run_processor', post_body,
                         running_user=current_user.id,
                         processor_args=processor_args)
        self.last_run_time = datetime.utcnow()
        db.session.commit()

    def get_last_post(self):
        return self.posts.order_by(Post.timestamp.desc()).first()

    def get_all_requests(self):
        return self.requests.order_by(Requests.created_at.desc()).all()

    def get_open_requests(self):
        return (self.requests.filter_by(complete=False).
                order_by(Requests.created_at.desc()).all())

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])

    def get_all_dashboards(self):
        return self.dashboard.order_by(Dashboard.created_at.desc()).all()

    def get_url(self):
        return url_for('main.processor_page', object_name=self.name)

    def get_project_numbers(self):
        return [x.project_number for x in self.projects]

    def get_notes(self):
        return self.notes.order_by(Notes.created_at.desc()).all()

    def get_current_buttons(self):
        buttons = [{'Basic': 'main.edit_processor'},
                   {'Import': 'main.edit_processor_import'},
                   {'Clean': 'main.edit_processor_clean'},
                   {'Export': 'main.edit_processor_export'}]
        return buttons


class TaskScheduler(db.Model):
    id = db.Column(db.String(36), primary_key=True)
    name = db.Column(db.String(128), index=True)
    description = db.Column(db.String(128))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    scheduled_time = db.Column(db.Time, default=datetime.utcnow)
    start_date = db.Column(db.DateTime, default=datetime.utcnow)
    end_date = db.Column(db.DateTime, default=datetime.utcnow)
    interval = db.Column(db.Integer, default=24)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))


class ProcessorDatasources(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    key = db.Column(db.String(64))
    account_id = db.Column(db.Text)
    account_filter = db.Column(db.String(128))
    start_date = db.Column(db.Date)
    api_fields = db.Column(db.String(128))
    vendor_key = db.Column(db.String(128))
    full_placement_columns = db.Column(db.Text)
    placement_columns = db.Column(db.Text)
    auto_dictionary_placement = db.Column(db.Text)
    auto_dictionary_order = db.Column(db.Text)
    active_metrics = db.Column(db.Text)
    vm_rules = db.Column(db.Text)

    def __init__(self):
        self.form_dict = self.get_import_form_dict()
        self.ds_dict = self.get_ds_form_dict()
        self.full_dict = self.get_full_dict()

    def __eq__(self, other):
        return self.get_full_dict() == other.get_full_dict()

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_full_dict(self):
        self.form_dict = self.get_import_form_dict()
        self.ds_dict = self.get_ds_form_dict()
        self.full_dict = self.form_dict.copy()
        self.full_dict.update(self.ds_dict)
        return self.full_dict

    def get_import_form_dict(self):
        form_dict = {
            'name': self.name,
            'key': self.key,
            'vendor_key': self.vendor_key,
            'account_id': self.account_id,
            'start_date': self.start_date,
            'account_filter': self.account_filter,
            'api_fields': self.api_fields
        }
        return form_dict

    @staticmethod
    def convert_string_to_list(string_value):
        if isinstance(string_value, list):
            new_list = string_value
        else:
            new_list = string_value.strip('{').strip('}').split(',')
            new_list = [y.strip('"') for y in new_list]
        return new_list

    def get_ds_form_dict(self):
        form_dict = {
            'original_vendor_key': self.vendor_key,
            'vendor_key': self.vendor_key,
            'full_placement_columns': self.full_placement_columns,
            'placement_columns': self.placement_columns,
            'auto_dictionary_placement': self.auto_dictionary_placement,
            'auto_dictionary_order': self.auto_dictionary_order,
            'active_metrics': self.active_metrics,
            'vm_rules': self.vm_rules}
        for x in ['auto_dictionary_order', 'full_placement_columns']:
            if form_dict[x]:
                val = self.convert_string_to_list(form_dict[x])
                form_dict[x] = '\n'.join(val)
        return form_dict

    def get_import_processor_dict(self):
        form_dict = {
            'name': self.name,
            'Key': self.key,
            'ID': self.account_id,
            'START DATE': self.start_date,
            'Filter': self.account_filter,
            'API_FIELDS': self.api_fields,
            vmc.vendorkey: self.vendor_key
        }
        return form_dict

    def set_from_processor(self, source, current_processor):
        self.processor_id = current_processor.id
        self.start_date = source.p[vmc.startdate].date()
        self.api_fields = '|'.join(['' if x == 'nan' else x
                                    for x in source.p[vmc.apifields]])
        self.vendor_key = source.key
        self.full_placement_columns = source.p[vmc.fullplacename]
        self.placement_columns = source.p[vmc.placement]
        self.auto_dictionary_placement = source.p[vmc.autodicplace]
        self.auto_dictionary_order = source.p[vmc.autodicord]
        self.active_metrics = str(source.get_active_metrics())
        self.vm_rules = str(source.vm_rules)
        if source.ic_params:
            self.name = source.ic_params['name']
            self.key = source.ic_params['Key']
            self.account_id = source.ic_params['ID']
            self.account_filter = source.ic_params['Filter']

    def get_update_from_form(self, form):
        update_dict = {
            self.vendor_key: form['vendor_key'],
            self.full_placement_columns: form['full_placement_columns'],
            self.placement_columns: form['placement_columns'],
            self.auto_dictionary_placement: form['auto_dictionary_placement'],
            self.auto_dictionary_order: form['auto_dictionary_order'],
            self.active_metrics: form['active_metrics'],
            self.vm_rules: form['vm_rules']
        }
        return update_dict

    def set_from_form(self, form, current_processor):
        if 'name' in form:
            if 'vendor_key' in form:
                self.vendor_key = form['vendor_key']
            self.name = form['name']
            self.processor_id = current_processor.id
            self.key = form['key']
            self.account_id = form['account_id']
            self.account_filter = form['account_filter']
            self.start_date = form['start_date']
            self.api_fields = form['api_fields']
        else:
            self.vendor_key = form['vendor_key']
            self.full_placement_columns = form['full_placement_columns']
            self.placement_columns = form['placement_columns']
            self.auto_dictionary_placement = form['auto_dictionary_placement']
            self.auto_dictionary_order = form['auto_dictionary_order']
            self.active_metrics = form['active_metrics']
            self.vm_rules = form['vm_rules']

    def get_datasource_for_processor(self):
        source = {
            vmc.vendorkey: self.vendor_key,
            vmc.fullplacename: self.full_placement_columns,
            vmc.placement: self.placement_columns,
            vmc.autodicplace: self.auto_dictionary_placement,
            vmc.autodicord: self.auto_dictionary_order,
            'active_metrics': self.active_metrics,
            'vm_rules': self.vm_rules
        }
        for x in ['active_metrics', 'vm_rules']:
            try:
                source[x] = ast.literal_eval(source[x])
            except:
                pass
        for x in [vmc.autodicord, vmc.fullplacename]:
            if source[x]:
                if '\r\n' in source[x]:
                    source[x] = '|'.join([y for y in source[x].split('\r\n')])
                elif '\n' in source[x]:
                    source[x] = '|'.join([y for y in source[x].split('\n')])
                else:
                    source[x] = '|'.join(self.convert_string_to_list(source[x]))
        return source

    def get_form_dict_with_split(self):
        form_dict = self.get_ds_form_dict()
        for x in ['auto_dictionary_order', 'full_placement_columns']:
            if form_dict[x]:
                if '\r\n' in form_dict[x]:
                    form_dict[x] = form_dict[x].split('\r\n')
                else:
                    form_dict[x] = form_dict[x].split('\n')
        return form_dict


class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    key = db.Column(db.String(64))
    account_id = db.Column(db.Text)
    campaign_id = db.Column(db.Text)
    username = db.Column(db.Text)
    password = db.Column(db.Text)

    def get_form_dict(self):
        form_dict = {
            'key': self.key,
            'account_id': self.account_id,
            'campaign_id': self.campaign_id,
            'username': self.username,
            'password': self.password
        }
        return form_dict

    def set_from_form(self, form, current_processor):
        self.processor_id = current_processor.id
        self.key = form['key']
        self.account_id = form['account_id']
        self.campaign_id = form['campaign_id']
        self.username = form['username']
        self.password = form['password']

    def get_dict_for_processor(self, name, start_date):
        proc_dict = {
            'name': name,
            'key': self.key,
            'account_id': self.account_id,
            'account_filter': self.campaign_id,
            'start_date': start_date,
            'api_fields': ''
        }
        if self.key == 'Facebook':
            proc_dict['api_fields'] = 'Actions'
        return proc_dict


class Requests(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    fix_type = db.Column(db.String(128), index=True)
    column_name = db.Column(db.Text)
    wrong_value = db.Column(db.Text)
    correct_value = db.Column(db.Text)
    filter_column_name = db.Column(db.Text)
    filter_column_value = db.Column(db.Text)
    fix_description = db.Column(db.Text)
    complete = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    fixed_time = db.Column(db.DateTime)
    posts = db.relationship('Post', backref='requests', lazy='dynamic')

    def get_form_dict(self):
        form_dict = {
            'processor_id': self.processor_id,
            'fix_type': self.fix_type,
            'column_name': self.column_name,
            'wrong_value': self.wrong_value,
            'correct_value': self.correct_value,
            'filter_column_name': self.filter_column_name,
            'filter_column_value': self.filter_column_value,
            'fix_description': self.fix_description,
        }
        return form_dict

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and 'id' not in k
                     and k not in ['created_at', 'fixed_time', 'complete',
                                   'processor']])

    def get_last_post(self):
        return self.posts.order_by(Post.timestamp.desc()).first()

    def mark_resolved(self):
        self.complete = True
        self.fixed_time = datetime.utcnow()

    def mark_unresolved(self):
        self.complete = False


class Notes(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    note_text = db.Column(db.Text)
    complete = db.Column(db.Boolean, default=False)
    notification = db.Column(db.Text)
    notification_day = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    posts = db.relationship('Post', backref='notes', lazy='dynamic')

    def get_form_dict(self):
        form_dict = {
            'processor_id': self.processor_id,
            'note_text': self.note_text,
            'complete': self.complete,
            'notification': self.notification,
            'notification_day': self.notification_day
        }
        return form_dict

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and 'id' not in k
                     and k not in ['created_at', 'fixed_time', 'complete',
                                   'processor']])

    def get_last_post(self):
        return self.posts.order_by(Post.timestamp.desc()).first()

    def mark_resolved(self):
        self.complete = True

    def mark_unresolved(self):
        self.complete = False


class Uploader(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    description = db.Column(db.String(128))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    local_path = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_run_time = db.Column(db.DateTime, default=datetime.utcnow)
    media_plan = db.Column(db.Boolean)
    fb_account_id = db.Column(db.Text)
    aw_account_id = db.Column(db.Text)
    dcm_account_id = db.Column(db.Text)
    tasks = db.relationship('Task', backref='uploader', lazy='dynamic')
    posts = db.relationship('Post', backref='uploader', lazy='dynamic')
    campaign_id = db.Column(db.Integer, db.ForeignKey('campaign.id'))
    uploader_objects = db.relationship('UploaderObjects',
                                       backref='uploader', lazy='dynamic')

    def launch_task(self, name, description, running_user, *args, **kwargs):
        rq_job = current_app.task_queue.enqueue('app.tasks' + name,
                                                self.id, running_user,
                                                *args, **kwargs)
        task = Task(id=rq_job.get_id(), name=name, description=description,
                    user_id=self.user_id, uploader=self)
        db.session.add(task)
        return task

    def get_tasks_in_progress(self):
        return Task.query.filter_by(uploader=self, complete=False).all()

    def get_task_in_progress(self, name):
        return Task.query.filter_by(name=name, uploader=self,
                                    complete=False).first()

    def get_last_post(self):
        return self.posts.order_by(Post.timestamp.desc()).first()

    @staticmethod
    def get_all_requests():
        return []

    @staticmethod
    def get_open_requests():
        return []

    @staticmethod
    def get_main_page():
        return []

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])

    def get_url(self):
        return url_for('main.uploader_page', object_name=self.name)

    def get_current_buttons(self):
        buttons = [{'Basic': 'main.edit_uploader'},
                   {'Campaign': 'main.edit_uploader_campaign'},
                   {'Adset': 'main.edit_uploader_adset'},
                   {'Creative': 'main.edit_uploader_creative'},
                   {'Ad': 'main.edit_uploader_ad'}]
        return buttons


class UploaderObjects(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    uploader_id = db.Column(db.Integer, db.ForeignKey('uploader.id'))
    uploader_type = db.Column(db.Text)
    object_level = db.Column(db.Text)
    name_create_type = db.Column(db.Text)
    media_plan_columns = db.Column(db.Text)
    spend_type = db.Column(db.Text)
    spend_value = db.Column(db.Numeric)
    objective = db.Column(db.Text)
    partner_filter = db.Column(db.Text)
    duplication_type = db.Column(db.Text)
    uploader_relations = db.relationship(
        'UploaderRelations', backref='uploader_objects', lazy='dynamic')

    @staticmethod
    def string_to_list(string_value):
        return UploaderRelations.convert_string_to_list(string_value)

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])


class UploaderRelations(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    uploader_objects_id = db.Column(db.Integer,
                                    db.ForeignKey('uploader_objects.id'))
    impacted_column_name = db.Column(db.Text)
    relation_constant = db.Column(db.Text)
    position = db.Column(db.Text)
    unresolved_relations = db.Column(db.Text)

    def get_form_dict(self):
        form_dict = {
            'impacted_column_name': self.impacted_column_name,
            'relation_constant': self.relation_constant,
            'position': self.position,
            'unresolved_relations': self.unresolved_relations
        }
        for col in ['position']:
            if form_dict[col]:
                val = self.convert_string_to_list(form_dict[col])
                form_dict[col] = val
        return form_dict

    @staticmethod
    def convert_string_to_list(string_value):
        val = string_value
        if val:
            val = ProcessorDatasources.convert_string_to_list(val)
        return val

    def set_from_form(self, form, cur_upo_id):
        self.uploader_objects_id = cur_upo_id.id
        self.impacted_column_name = form['impacted_column_name']
        self.relation_constant = form['relation_constant']
        self.position = form['position']

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])


class ProcessorAnalysis(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    key = db.Column(db.Text)
    data = db.Column(db.JSON)
    message = db.Column(db.Text)
    date = db.Column(db.Date)
    parameter = db.Column(db.Text)
    parameter_2 = db.Column(db.Text)
    filter_col = db.Column(db.Text)
    filter_val = db.Column(db.Text)
    split_col = db.Column(db.Text)


class Dashboard(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    chart_type = db.Column(db.Text)
    dimensions = db.Column(db.Text)
    metrics = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    dashboard_filters = db.relationship('DashboardFilter',
                                        backref='dashboard', lazy='dynamic')

    def __init__(self, **kwargs):
        super(Dashboard, self).__init__(**kwargs)
        self.form = None

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and
                     k not in ['id', 'user_id', 'processor', 'processor_id']])

    def get_dimensions(self):
        return self.convert_string_to_list(self.dimensions)

    def get_metrics(self):
        return self.convert_string_to_list(self.metrics)

    def get_dimensions_json(self):
        return json.dumps(self.get_dimensions())

    def get_metrics_json(self):
        return json.dumps(self.get_metrics())

    def get_filters_json(self):
        dash_filters = [x.get_converted_form_dict()
                        for x in self.dashboard_filters]
        return json.dumps(dash_filters)

    @staticmethod
    def convert_string_to_list(string_value):
        val = string_value
        if val:
            val = ProcessorDatasources.convert_string_to_list(val)
        return val

    def add_form(self, form):
        self.form = [form]


class DashboardFilter(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    dashboard_id = db.Column(db.Integer, db.ForeignKey('dashboard.id'))
    filter_col = db.Column(db.Text)
    filter_val = db.Column(db.Text)

    def __init__(self, **kwargs):
        super(DashboardFilter, self).__init__(**kwargs)
        self.form_dict = self.get_form_dict()

    def get_form_dict(self):
        form_dict = {
            'filter_col': self.filter_col,
            'filter_val': self.filter_val
        }
        self.form_dict = form_dict
        return form_dict

    def get_converted_form_dict(self):
        form_dict = {
            self.convert_string_to_list(self.filter_col)[0]:
            self.convert_string_to_list(self.filter_val)
        }
        return form_dict

    def set_from_form(self, form, current_dashboard):
        self.dashboard_id = current_dashboard.id
        self.filter_col = form['filter_col']
        self.filter_val = form['filter_val']

    @staticmethod
    def convert_string_to_list(string_value):
        val = string_value
        if val:
            val = ProcessorDatasources.convert_string_to_list(val)
        return val


class Project(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_number = db.Column(db.Text)
    initial_project_number = db.Column(db.Text)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'))
    project_name = db.Column(db.Text)
    media = db.Column(db.Boolean)
    creative = db.Column(db.Boolean)
    date_opened = db.Column(db.Date)
    flight_start_date = db.Column(db.Date)
    flight_end_date = db.Column(db.Date)
    exhibit = db.Column(db.Text)
    sow_received = db.Column(db.Text)
    billing_dates = db.Column(db.Text)
    notes = db.Column(db.Text)
    processor_associated = db.relationship(
        'Processor', secondary=project_number_processor,
        primaryjoin=(project_number_processor.c.project_id == id),
        secondaryjoin="project_number_processor.c.processor_id == Processor.id",
        backref=db.backref('project_number_processor', lazy='dynamic'),
        lazy='dynamic')


class ProjectNumberMax(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    max_number = db.Column(db.Integer)


class Tutorial(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    description = db.Column(db.Text)
    tutorial_stage = db.relationship('TutorialStage',
                                     backref='tutorial', lazy='dynamic')

    def get_url(self):
        return url_for('tutorials.get_tutorial', tutorial_name=self.name,
                       tutorial_level=0)

    def get_progress(self, user_id):
        cu = User.query.get(user_id)
        total_stages = len(self.tutorial_stage.all())
        completed_stages = len(cu.tutorial_stages_completed.filter_by(
            tutorial_id=self.id).all())
        return int((completed_stages / total_stages) * 100)

    def get_stages(self):
        total_stages = self.tutorial_stage.all()
        return total_stages


class TutorialStage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tutorial_id = db.Column(db.Integer, db.ForeignKey('tutorial.id'))
    tutorial_level = db.Column(db.Integer)
    header = db.Column(db.Text)
    sub_header = db.Column(db.Text)
    message = db.Column(db.Text)
    alert = db.Column(db.Text)
    alert_level = db.Column(db.Text)
    image = db.Column(db.Text)
    question = db.Column(db.Text)
    question_answers = db.Column(db.Text)
    correct_answer = db.Column(db.Text)

    def get_question_answers(self):
        return self.question_answers.split('|')

    def get_message_split_on_newline(self):
        return self.message.split('\n')
