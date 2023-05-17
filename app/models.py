import rq
import ast
import jwt
import pytz
import json
import time
import redis
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from datetime import time as datetime_time
from hashlib import md5
from flask import current_app, url_for, request
from flask_login import UserMixin, current_user
from flask_babel import _
import processor.reporting.utils as utl
import processor.reporting.vmcolumns as vmc
import processor.reporting.dictcolumns as dctc
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


project_number_plan = db.Table(
    'project_number_plan',
    db.Column('project_id', db.Integer, db.ForeignKey('project.id')),
    db.Column('plan_id', db.Integer, db.ForeignKey('plan.id'))
)


processor_plan = db.Table(
    'processor_plan',
    db.Column('processor_id', db.Integer, db.ForeignKey('processor.id')),
    db.Column('plan_id', db.Integer, db.ForeignKey('plan.id'))
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
    conversation = db.relationship(
        'Conversation', foreign_keys='Conversation.user_id', backref='user',
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
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))

    def __repr__(self):
        return '<Post {}>'.format(self.body)

    def processor_run_success(self):
        return self.body[-17:] == 'finished running.'

    def processor_run_failed(self):
        return self.body[-11:] == 'run failed.'

    @staticmethod
    def get_posts_for_objects(cur_obj, fix_id, current_page, object_name,
                              note_id=None):
        if object_name == 'plan':
            route_prefix = 'plan.'
        else:
            route_prefix = 'main.'
        page = request.args.get('page', 1, type=int)
        post_filter = {'{}_id'.format(object_name): cur_obj.id}
        if fix_id:
            post_filter['request_id'] = fix_id
        if note_id:
            post_filter['note_id'] = note_id
        query = Post.query
        for attr, value in post_filter.items():
            query = query.filter(getattr(Post, attr) == value)
        posts = (query.
                 order_by(Post.timestamp.desc()).
                 paginate(page, 5, False))
        next_url = url_for(route_prefix + current_page, page=posts.next_num,
                           object_name=cur_obj.name) if posts.has_next else None
        prev_url = url_for(route_prefix + current_page, page=posts.prev_num,
                           object_name=cur_obj.name) if posts.has_prev else None
        return posts, next_url, prev_url


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
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))

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

    @staticmethod
    def get_table_name_to_task_dict():
        task_dict = {}
        for db_model in [Processor, Plan, Uploader]:
            new_dict = db_model.get_table_name_to_task_dict()
            task_dict = {**task_dict, **new_dict}
        return task_dict


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

    @staticmethod
    def get_client_view_selector(current_view='Clients'):
        view_selector = [{'view': 'Clients', 'active': False,
                          'value': 'main.clients'},
                         {'view': 'Project Numbers', 'active': False,
                          'value': 'main.project_numbers'}]
        for v in view_selector:
            if v['view'] == current_view:
                v['active'] = True
        return view_selector

    @staticmethod
    def get_model_name_list():
        return ['client']

    @staticmethod
    def get_name_list(parameter='clientname'):
        a = ProcessorAnalysis.query.filter_by(
            processor_id=23, key='database_cache', parameter=parameter,
            filter_col='').order_by(ProcessorAnalysis.date).first()
        df = pd.read_json(a.data)
        df = df[df[vmc.impressions] > 0].sort_values(vmc.impressions,
                                                     ascending=False)
        df = df.to_dict(orient='records')
        return df

    @staticmethod
    def get_default_name():
        return ['Liquid Advertising']

    def set_from_form(self, form, parent_model):
        self.name = form['name']


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

    @staticmethod
    def get_parent():
        return Client

    @staticmethod
    def get_model_name_list():
        return ['product']

    @staticmethod
    def get_name_list():
        return Client.get_name_list('productname')

    def set_from_form(self, form, parent_model):
        self.name = form['name']
        self.client_id = parent_model.id

    @staticmethod
    def get_default_name():
        return ['Liquid Advertising']


class Campaign(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    product_id = db.Column(db.Integer, db.ForeignKey('product.id'))
    processor = db.relationship('Processor', backref='campaign', lazy='dynamic')
    uploader = db.relationship('Uploader', backref='campaign', lazy='dynamic')
    plan = db.relationship('Plan', backref='campaign', lazy='dynamic')

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

    @staticmethod
    def get_parent():
        return Product

    @staticmethod
    def get_model_name_list():
        return ['campaign']

    @staticmethod
    def get_name_list():
        return Client.get_name_list('campaignname')

    def set_from_form(self, form, parent_model):
        self.name = form['name']
        self.product_id = parent_model.id

    @staticmethod
    def get_default_name():
        return ['Liquid Advertising']


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
    plans = db.relationship(
        'Plan', secondary=processor_plan,
        primaryjoin=(processor_plan.c.processor_id == id),
        secondaryjoin="processor_plan.c.plan_id == Plan.id",
        backref=db.backref('processor_plan', lazy='dynamic'),
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

    def get_request_task_in_progress(self, name, description):
        return Task.query.filter_by(name=name, processor=self,
                                    complete=False, description=description
                                    ).first()

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

    def get_requests_processor_analysis(self, analysis_key):
        return self.processor_analysis.filter_by(key=analysis_key).first()

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

    def get_object_function_call(self):
        return {'object_name': self.name}

    @staticmethod
    def get_plan_properties():
        return ['Add Account Types', 'Plan Net', 'Package Capping',
                'Plan As Datasource', 'Add Fees']

    def get_plan_kwargs(self, object_name, request_flow=True, cur_form=None):
        form_description = """
        Upload current media plan and view properties of the plan.
        The file should have type '.xlsx'.  
        There should be a tab in the file called 'Media Plan'.  
        The column names in the tab 'Media Plan' should be on row 3.  
        It will specifically look for columns titled 'Partner Name' 
        and 'Campaign Phase (If Needed) '
        """
        if request_flow:
            buttons = 'ProcessorRequest'
        else:
            buttons = 'Processor'
        kwargs = self.get_current_processor(
            object_name, current_page='edit_processor_plan', edit_progress=50,
            edit_name='Plan', buttons=buttons,
            form_title='PLAN', form_description=form_description)
        kwargs['form'] = cur_form
        plan_properties = [x for x in Processor.get_plan_properties()
                           if x != 'Package Capping']
        kwargs['form'].plan_properties.data = plan_properties
        return kwargs

    def get_current_processor(
            self, object_name, current_page, edit_progress=0,
            edit_name='Page', buttons=None, fix_id=None, note_id=None,
            form_title=None, form_description=None):
        cur_proc = Processor.query.filter_by(name=object_name).first_or_404()
        cur_user = User.query.filter_by(id=current_user.id).first_or_404()
        posts, next_url, prev_url = Post().get_posts_for_objects(
            cur_obj=cur_proc, fix_id=fix_id, current_page=current_page,
            object_name='processor', note_id=note_id)
        api_imports = {0: {'All': 'import'}}
        for idx, (k, v) in enumerate(vmc.api_translation.items()):
            api_imports[idx + 1] = {k: v}
        run_links = self.get_processor_run_links()
        edit_links = self.get_processor_edit_links()
        output_links = self.get_processor_output_links()
        request_links = self.get_processor_request_links(cur_proc.name)
        walk = Walkthrough().get_walk_questions(edit_name)
        object_function_call = cur_proc.get_object_function_call()
        args = dict(object=cur_proc, processor=cur_proc,
                    posts=posts.items, title=_('Processor'),
                    object_name=cur_proc.name, user=cur_user,
                    edit_progress=edit_progress, edit_name=edit_name,
                    api_imports=api_imports,
                    object_function_call=object_function_call,
                    run_links=run_links, edit_links=edit_links,
                    output_links=output_links, request_links=request_links,
                    next_url=next_url, prev_url=prev_url,
                    walkthrough=walk, form_title=form_title,
                    form_description=form_description)
        args['buttons'] = self.get_navigation_buttons(buttons)
        return args

    @staticmethod
    def get_processor_run_links():
        run_links = {}
        for idx, run_arg in enumerate(
                (('full', 'Runs all processor modes from import to export.'),
                 ('import', 'Runs api import. A modal will '
                            'popup specifying a specific API or all.'),
                 ('basic', 'Runs regular processor that cleans all data '
                           'and generates Raw Data Output.'),
                 ('export', 'Runs export to database and tableau refresh.'),
                 ('update',
                  'RARELY NEEDED - Runs processor update on vendormatrix '
                  'and dictionaries based on code changes.'))):
            run_link = dict(title=run_arg[0].capitalize(),
                            tooltip=run_arg[1])
            run_links[idx] = run_link
        return run_links

    @staticmethod
    def get_processor_edit_links():
        edit_links = {}
        edits = (
            ('Vendormatrix',
             'Directly edit the vendormatrix. '
             'This is the config file for all datasources.'),
            ('Translate',
             'Directly edit the translation config. This config '
             'file changes a dictionary value into another.'),
            ('Constant',
             'Directly edit the constant config. This config file '
             'sets a dictionary value for all dictionaries in '
             'the processor instance.'),
            ('Relation',
             'Directly edit relation config. This config file '
             'specifies relations between dictionary values. '))
        for idx, edit_file in enumerate(edits):
            edit_links[idx] = dict(title=edit_file[0],
                                   nest=[], tooltip=edit_file[1])
            if edit_file[0] == 'Relation':
                edit_links[idx]['nest'] = ['Campaign', 'Targeting', 'Creative',
                                           'Vendor', 'Country', 'Serving',
                                           'Copy']
        return edit_links

    @staticmethod
    def get_processor_output_links():
        output_links = {}
        cols = [vmc.output_file, vmc.vendorkey, dctc.VEN, dctc.TAR, dctc.CRE,
                dctc.COP, dctc.BM, dctc.SRV]
        for idx, col in enumerate(cols):
            title = col.replace('mp', '').replace('.csv', '').replace(' ', '')
            tooltip = 'View modal table of data grouped by {}.'.format(col)
            if col == vmc.output_file:
                tooltip = 'Downloads the {}'.format(col)
            output_links[idx] = dict(title=title, nest=[], tooltip=tooltip)
        return output_links

    @staticmethod
    def get_processor_request_links(object_name):
        run_links = {
            0: {'title': 'View Initial Request',
                'href': url_for('main.edit_request_processor',
                                object_name=object_name),
                'tooltip': 'View/Edits the initial request that made this '
                           'processor instance. This will not change '
                           'anything unless the processor is rebuilt.'},
            1: {'title': 'Request Data Fix',
                'href': url_for('main.edit_processor_request_fix',
                                object_name=object_name),
                'tooltip': 'Request a fix for the current processor data '
                           'set, including changing values, adding'
                           ' files etc.'},
            2: {'title': 'Request Duplication',
                'href': url_for('main.edit_processor_duplication',
                                object_name=object_name),
                'tooltip': 'Duplicates current processor instance based on'
                           ' date to use new instance going forward.'},
            3: {'title': 'Request Dashboard',
                'href': url_for('main.processor_dashboard_create',
                                object_name=object_name),
                'tooltip': 'Create a dashboard in the app that queries the'
                           ' database based on this processor instance.'}
        }
        return run_links

    @staticmethod
    def get_navigation_buttons(buttons=None):
        if buttons == 'ProcessorRequest':
            buttons = [{'Basic': ['main.edit_request_processor']},
                       {'Plan': ['main.edit_processor_plan']},
                       {'Accounts': ['main.edit_processor_account']},
                       {'Fees': ['main.edit_processor_fees']},
                       {'Conversions': ['main.edit_processor_conversions']},
                       {'Finish': ['main.edit_processor_finish']}]
        elif buttons == 'ProcessorRequestFix':
            buttons = [{'New Fix': ['main.edit_processor_request_fix']},
                       {'Submit Fixes': ['main.edit_processor_submit_fix']},
                       {'All Fixes': ['main.edit_processor_all_fix']}]
        elif buttons == 'ProcessorNote':
            buttons = [{'New Note': ['main.edit_processor_note']},
                       {'All Notes': ['main.edit_processor_all_notes']},
                       {'Automatic Notes': ['main.edit_processor_auto_notes']}]
        elif buttons == 'ProcessorDuplicate':
            buttons = [{'Duplicate': ['main.edit_processor_duplication']}]
        elif buttons == 'ProcessorDashboard':
            buttons = [{'Create': ['main.processor_dashboard_create']},
                       {'View All': ['main.processor_dashboard_all']}]
        elif buttons == 'UploaderDCM':
            buttons = [{'Basic': ['main.edit_uploader']},
                       {'Campaign': ['main.edit_uploader_campaign_dcm']},
                       {'Adset': ['main.edit_uploader_adset_dcm']},
                       {'Ad': ['main.edit_uploader_ad_dcm']}]
        elif buttons == 'UploaderFacebook':
            buttons = [{'Basic': ['main.edit_uploader']},
                       {'Campaign': ['main.edit_uploader_campaign']},
                       {'Adset': ['main.edit_uploader_adset']},
                       {'Creative': ['main.edit_uploader_creative']},
                       {'Ad': ['main.edit_uploader_ad']}]
        elif buttons == 'UploaderAdwords':
            buttons = [{'Basic': ['main.edit_uploader']},
                       {'Campaign': ['main.edit_uploader_campaign_aw']},
                       {'Adset': ['main.edit_uploader_adset_aw']},
                       {'Ad': ['main.edit_uploader_ad_aw']}]
        elif buttons == 'Plan':
            buttons = [{'Basic': ['plan.edit_plan']},
                       {'Topline': ['plan.topline']},
                       {'SOW': ['plan.edit_sow']},
                       {'Plan Rules': ['plan.plan_rules']}]
        else:
            buttons = [
                {'Basic': ['main.edit_processor', 'list-ol']},
                {'Plan': ['main.edit_processor_plan_normal', 'plane']},
                {'Import': ['main.edit_processor_import', 'file-import']},
                {'Clean': ['main.edit_processor_clean', 'soap']},
                {'Export': ['main.edit_processor_export', 'file-export']},
                {'Bill': ['main.edit_processor_billing', 'money-bill']}]
        new_buttons = []
        for button in buttons:
            new_button = {}
            for k, v in button.items():
                new_button[k] = {'route': v[0], 'icon': ''}
                if len(v) > 1:
                    new_button[k]['icon'] = v[1]
                new_buttons.append(new_button)
        return new_buttons

    @staticmethod
    def get_model_name_list():
        return ['processor']

    def get_table_elem(self):
        return ''

    @staticmethod
    def get_table_name_to_task_dict():
        arg_trans = {'Translate': '.get_translation_dict',
                     'Vendormatrix': '.get_vendormatrix',
                     'Constant': '.get_constant_dict',
                     'Relation': '.get_relational_config',
                     'OutputData': '.get_data_tables',
                     'toplineMetrics': '.get_processor_topline_metrics',
                     'TestConnection': '.test_api_connection',
                     'dictionary_order': '.get_dict_order',
                     'change_dictionary_order': '.get_change_dict_order',
                     'raw_data': '.get_raw_data',
                     'download_raw_data': '.get_raw_data',
                     'download_pacing_data': '.get_processor_pacing_metrics',
                     'dictionary': '.get_dictionary',
                     'delete_dict': '.delete_dict',
                     'rate_card': '.get_rate_card',
                     'edit_conversions': '.get_processor_conversions',
                     'data_sources': '.get_processor_sources',
                     'imports': '.get_processor_sources',
                     'import_config': '.get_import_config_file',
                     'all_processors': '.get_all_processors',
                     'raw_file_comparison': '.get_raw_file_comparison',
                     'request_table': '.get_request_table',
                     'quick_fix': '.apply_quick_fix',
                     'check_processor_plan': '.check_processor_plan',
                     'apply_processor_plan': '.apply_processor_plan',
                     'get_plan_property': '.get_plan_property',
                     'screenshot': '.get_screenshot_table',
                     'screenshotImage': '.get_screenshot_image',
                     'notesTable': '.get_notes_table',
                     'Pacing Table': '.get_processor_pacing_metrics',
                     'Daily Pacing': '.get_daily_pacing',
                     'datasource_table': '.get_processor_data_source_table',
                     'singleNoteTable': '.get_single_notes_table',
                     'billingTable': '.get_billing_table',
                     'billingInvoice': '.get_billing_invoice'}
        return arg_trans


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
    account_filter = db.Column(db.Text)
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

    def __lt__(self, other):
        return self.id < other.id

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
            'campaign_id': self.campaign_id
        }
        return form_dict

    def set_from_form(self, form, current_processor):
        self.processor_id = current_processor.id
        self.key = form['key']
        self.account_id = form['account_id']
        self.campaign_id = form['campaign_id']

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
    vendor = db.Column(db.Text)
    country = db.Column(db.Text)
    environment = db.Column(db.Text)
    kpi = db.Column(db.Text)
    start_date = db.Column(db.Date, default=datetime.utcnow)
    end_date = db.Column(db.Date, default=datetime.utcnow)
    dimensions = db.Column(db.Text)
    data = db.Column(db.JSON)
    header = db.Column(db.Text)
    note_type = db.Column(db.Text)
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

    def get_table_dict(self):
        table_dict = {
            'created_at': self.created_at,
            'processor_name': self.processor.name if self.processor else '',
            'username': self.user.username,
            'note_text': self.note_text,
            'vendor': self.vendor,
            'country': self.country,
            'environment': self.environment,
            'kpi': self.kpi,
            'start_date': self.start_date,
            'end_date': self.end_date
        }
        return table_dict

    def to_dict(self, include_data=True):
        cols_exclude = ['created_at', 'fixed_time', 'complete', 'processor']
        if not include_data:
            cols_exclude += ['data']
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and 'id' not in k
                     and k not in cols_exclude])

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

    @staticmethod
    def get_navigation_buttons():
        buttons = [{'Basic': 'main.edit_uploader'},
                   {'Campaign': 'main.edit_uploader_campaign'},
                   {'Adset': 'main.edit_uploader_adset'},
                   {'Creative': 'main.edit_uploader_creative'},
                   {'Ad': 'main.edit_uploader_ad'}]
        return buttons

    @staticmethod
    def get_table_name_to_task_dict():
        arg_trans = {}
        for x in ['Uploader', 'Campaign', 'Adset', 'Ad', 'Creator',
                  'uploader_full_relation', 'edit_relation', 'name_creator',
                  'uploader_current_name', 'uploader_creative_files',
                  'upload_filter', 'match_table']:
            arg_trans[x] = '.get_uploader_file'
        return arg_trans


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
        lazy='dynamic', viewonly=True)


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

    @staticmethod
    def get_tutorial_homepage_url():
        return url_for('tutorials.tutorial')

    def get_progress(self, user_id):
        cu = User.query.get(user_id)
        total_stages = len(self.tutorial_stage.all())
        completed_stages = len(cu.tutorial_stages_completed.filter_by(
            tutorial_id=self.id).all())
        return int((completed_stages / total_stages) * 100)

    def get_stages(self):
        total_stages = self.tutorial_stage.all()
        return total_stages

    def tutorial_completed(self, tutorial_user):
        last_stage = len(self.tutorial_stage.all())
        user_complete = tutorial_user.tutorial_stages_completed.filter_by(
            tutorial_level=last_stage-1, tutorial_id=self.id).first()
        return user_complete


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
        message_split = [
            {'msg': x, 'bold': True if x and x[0].isdigit() else False}
            for x in self.message.split('\n')]
        return message_split

    @staticmethod
    def create_dict(tutorial_level=0, header='', sub_header='',
                    message='', alert='', alert_level='', image='', question='',
                    question_answers='', correct_answer=''):
        stage = {
            'tutorial_level': tutorial_level,
            'header': header,
            'sub_header': sub_header,
            'message': message,
            'alert': alert,
            'alert_level': alert_level,
            'image': image,
            'question': question,
            'question_answers': question_answers,
            'correct_answer': str(correct_answer)
        }
        return stage


class Walkthrough(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    edit_name = db.Column(db.Text)
    title = db.Column(db.Text)
    walkthrough_slides = db.relationship(
        'WalkthroughSlide', backref='walkthrough', lazy='dynamic')

    @staticmethod
    def generate_slide_dict(text, show_me='', data=''):
        slide = {'text': text}
        if show_me:
            slide['show_me'] = show_me
        if data:
            slide['data'] = data
        return slide

    def get_walk_questions(self, edit_name):
        w = []
        if not edit_name:
            all_walk = Walkthrough.query.all()
        else:
            all_walk = Walkthrough.query.filter_by(edit_name=edit_name).all()
        if all_walk:
            for walk in all_walk:
                walk_slides = walk.walkthrough_slides.order_by(
                    WalkthroughSlide.slide_number)
                w.append({'title': walk.title,
                          'slides': [
                              self.generate_slide_dict(x.slide_text,
                                                       x.show_me_element,
                                                       x.get_data())
                              for x in walk_slides]})
        return w


class WalkthroughSlide(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    walkthrough_id = db.Column(db.Integer, db.ForeignKey('walkthrough.id'))
    slide_number = db.Column(db.Integer)
    slide_text = db.Column(db.Text)
    show_me_element = db.Column(db.Text)
    data = db.Column(db.Text)

    def get_data(self):
        string_value = ''
        if self.data:
            string_value = ast.literal_eval(self.data)
        return string_value


class Plan(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    description = db.Column(db.String(128))
    client_requests = db.Column(db.Text)
    restrictions = db.Column(db.Text)
    objective = db.Column(db.Text)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    total_budget = db.Column(db.Numeric)
    tasks = db.relationship('Task', backref='plan', lazy='dynamic')
    posts = db.relationship('Post', backref='plan', lazy='dynamic')
    rate_card_id = db.Column(db.Integer, db.ForeignKey('rate_card.id'))
    campaign_id = db.Column(db.Integer, db.ForeignKey('campaign.id'))
    processor_associated = db.relationship(
        'Processor', secondary=processor_plan,
        primaryjoin=(processor_plan.c.plan_id == id),
        secondaryjoin="processor_plan.c.processor_id == Processor.id",
        backref=db.backref('processor_plan', lazy='dynamic'),
        lazy='dynamic')
    projects = db.relationship(
        'Project', secondary=project_number_plan,
        primaryjoin=(project_number_plan.c.plan_id == id),
        secondaryjoin="project_number_plan.c.project_id == Project.id",
        backref=db.backref('project_number_plan', lazy='dynamic'),
        lazy='dynamic')
    phases = db.relationship('PlanPhase', backref='plan', lazy='dynamic')

    @staticmethod
    def get_output_links():
        output_links = {}
        for idx, out_file in enumerate(
                (('SOW', 'Downloads the SOW.'),
                 ('ToplineDownload', 'Downloads the Topline.'))):
            output_links[idx] = dict(title=out_file[0], nest=[],
                                     tooltip=out_file[1])
        return output_links

    def get_current_plan(self, object_name=None, current_page=None,
                         edit_progress=0, edit_name='Page', buttons=None):
        output_links = self.get_output_links()
        kwargs = dict(title=_('Plan'), object_name=object_name,
                      object_function_call={'object_name': object_name},
                      edit_progress=edit_progress, edit_name=edit_name,
                      output_links=output_links)
        if object_name:
            cur_obj = Plan.query.filter_by(name=object_name).first_or_404()
            kwargs['object'] = cur_obj
            kwargs['buttons'] = Processor.get_navigation_buttons('Plan')
            posts, next_url, prev_url = Post.get_posts_for_objects(
                cur_obj, None, current_page, 'plan')
            kwargs['posts'] = posts.items
            kwargs['next_url'] = next_url
            kwargs['prev_url'] = prev_url
        else:
            kwargs['object'] = Plan()
            kwargs['object_name'] = ''
        return kwargs

    def get_url(self):
        return url_for('plan.edit_plan', object_name=self.name)

    def get_last_post(self):
        return self.posts.order_by(Post.timestamp.desc()).first()

    def launch_task(self, name, description, running_user, *args, **kwargs):
        rq_job = current_app.task_queue.enqueue('app.tasks' + name,
                                                self.id, running_user,
                                                *args, **kwargs)
        task = Task(id=rq_job.get_id(), name=name, description=description,
                    user_id=self.user_id, plan_id=self.id)
        db.session.add(task)
        return task

    def get_tasks_in_progress(self):
        return Task.query.filter_by(plan=self, complete=False).all()

    def get_task_in_progress(self, name):
        return Task.query.filter_by(name=name, plan=self,
                                    complete=False).first()

    @staticmethod
    def get_model_name_list():
        return ['plan', 'topline']

    @staticmethod
    def get_children():
        return PlanPhase

    @staticmethod
    def get_parent():
        return Campaign

    def set_from_form(self, form, parent_model, current_user_id):
        self.name = form['name']
        self.campaign_id = parent_model.id
        self.user_id = current_user_id
        self.start_date = utl.check_dict_for_key(
            form, 'start_date', datetime.today().date())
        self.end_date = utl.check_dict_for_key(
            form, 'end_date', datetime.today().date() + timedelta(days=7))
        self.total_budget = utl.check_dict_for_key(form, 'total_budget', 0)

    def check(self):
        client_check = Client.query.filter_by(name=self.name).first()
        return client_check

    @staticmethod
    def get_first_unique_name(name):
        name_exists = Plan.query.filter_by(name=name).first()
        if name_exists:
            for x in range(100):
                new_name = '{}_{}'.format(name, x)
                name_exists = Plan.query.filter_by(name=new_name).first()
                if not name_exists:
                    name = new_name
                    break
        return name

    def get_table_elem(self):
        elem = """
            <div id="msgTableElem">
            <div id='Topline' data-title="Plan" 
                    data-object_name="{}" data-edit_name="Topline">
            </div></div>""".format(self.name)
        return elem

    @staticmethod
    def get_table_name_to_task_dict():
        arg_trans = {
            'SOW': '.get_sow',
            'Topline': '.get_topline',
            'ToplineDownload': '.download_topline'
        }
        return arg_trans


class Sow(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_name = db.Column(db.String, index=True)
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))
    project_contact = db.Column(db.String)
    date_submitted = db.Column(db.Date)
    liquid_contact = db.Column(db.String)
    liquid_project = db.Column(db.Integer)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    client_name = db.Column(db.String)
    campaign = db.Column(db.String)
    address = db.Column(db.String)
    phone = db.Column(db.String)
    fax = db.Column(db.String)
    total_project_budget = db.Column(db.Numeric)
    ad_serving = db.Column(db.Numeric)


class PlanPhase(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    partners = db.relationship('Partner', backref='plan', lazy='dynamic')

    def get_form_dict(self):
        form_dict = {
            'name': self.name,
            'start_date': datetime.strftime(self.start_date, '%Y-%m-%d'),
            'end_date': datetime.strftime(self.end_date, '%Y-%m-%d')
        }
        return form_dict

    def set_from_form(self, form, current_plan):
        self.plan_id = current_plan.id
        if 'phaseSelect' in form:
            form_name = 'phaseSelect'
        else:
            form_name = 'name'
        self.name = form[form_name]
        self.start_date = utl.check_dict_for_key(
            form, 'start_date', datetime.today().date())
        self.end_date = utl.check_dict_for_key(
            form, 'end_date', datetime.today().date() + timedelta(days=7))

    @staticmethod
    def get_name_list():
        return ['launch', 'pre-launch', 'prelaunch', 'pre-order', 'preorder']

    @staticmethod
    def get_children():
        return Partner


class Partner(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    plan_phase_id = db.Column(db.Integer, db.ForeignKey('plan_phase.id'))
    total_budget = db.Column(db.Numeric)
    estimated_cpm = db.Column(db.Numeric)
    estimated_cpc = db.Column(db.Numeric)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    partner_type = db.Column(db.String(128))
    cplpv = db.Column(db.Numeric)
    cpbc = db.Column(db.Numeric)
    cpv = db.Column(db.Numeric)
    cpcv = db.Column(db.Numeric)
    placements = db.relationship('PartnerPlacements',
                                 backref='partner', lazy='dynamic')

    def get_form_dict(self, cur_phase=None):
        form_dict = {
            'partner_type': self.partner_type,
            'partner': self.name,
            'total_budget': self.total_budget,
            'cpm': self.estimated_cpm,
            'cpc': self.estimated_cpc,
            'cplpv': self.cplpv,
            'cpbc': self.cpbc,
            'cpv': self.cpv,
            'cpcv': self.cpcv,
            'start_date': datetime.strftime(self.start_date, '%Y-%m-%d'),
            'end_date': datetime.strftime(self.end_date, '%Y-%m-%d')
        }
        if cur_phase:
            form_dict['Phase'] = cur_phase.name
        return form_dict

    def set_from_form(self, form, current_plan):
        for k in list(form):
            if 'Select' in k:
                form[k.replace('Select', '')] = form[k]
        self.plan_phase_id = current_plan.id
        self.name = form['partner']
        self.partner_type = form['partner_type']
        self.estimated_cpm = form['cpm']
        self.estimated_cpc = form['cpc']
        self.cplpv = form['cplpv']
        self.cpbc = form['cpbc']
        self.cpv = form['cpv']
        self.cpcv = form['cpcv']
        self.start_date = utl.check_dict_for_key(
            form, 'start_date', datetime.today().date())
        self.end_date = utl.check_dict_for_key(
            form, 'end_date', datetime.today().date() + timedelta(days=7))
        self.total_budget = utl.check_dict_for_key(form, 'total_budget', 0)

    @staticmethod
    def get_name_list(parameter='vendorname|vendortypename'):
        a = ProcessorAnalysis.query.filter_by(
            processor_id=23, key='database_cache',
            parameter=parameter).order_by(ProcessorAnalysis.date).first()
        df = pd.read_json(a.data)
        df = df[df['impressions'] > 0].sort_values('impressions',
                                                   ascending=False)
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
        partner_name = 'partner'
        partner_type_name = 'partner_type'
        df = df.rename(columns={
            'vendorname': partner_name, 'vendortypename': partner_type_name})
        partner_list = df.to_dict(orient='records')
        partner_type_list = pd.DataFrame(
            df[partner_type_name].unique()).rename(
            columns={0: partner_type_name}).to_dict(orient='records')
        return partner_list, partner_type_list


class PartnerPlacements(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, index=True)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    partner_id = db.Column(db.Integer, db.ForeignKey('partner.id'))


class PlanRule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, index=True)
    order = db.Column(db.Integer)
    type = db.Column(db.String(128))
    rule_info = db.Column(db.JSON)


class Conversation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Chat(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    text = db.Column(db.Text)
    response = db.Column(db.Text)
    html_response = db.Column(db.Text)
    conversation_id = db.Column(db.Integer, db.ForeignKey('conversation.id'))
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])
