import rq
import os
import ast
import jwt
import pytz
import json
import time
import redis
import itertools
import numpy as np
import pandas as pd
from sqlalchemy import or_
from datetime import datetime, timedelta
from datetime import time as datetime_time
from hashlib import md5
from flask import current_app, url_for, request
from flask_login import UserMixin, current_user
from flask_babel import _
import processor.reporting.utils as utl
import processor.reporting.vmcolumns as vmc
import processor.reporting.dictcolumns as dctc
import processor.reporting.models as prc_model
import processor.reporting.calc as cal
import uploader.upload.creator as cre
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
    processor_reports = db.relationship('ProcessorReports',
                                        backref='user', lazy='dynamic')
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
            current_app.config['SECRET_KEY'], algorithm='HS256')

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
    return db.session.get(User, int(user_id))


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
        if object_name == Project.__table__.name:
            plan_ids = [plan.id for plan in cur_obj.plan_associated]
            processor_ids = [processor.id for processor in
                             cur_obj.processor_associated]
            query = Post.query.filter(
                or_(
                    Post.plan_id.in_(plan_ids),
                    Post.processor_id.in_(processor_ids)
                )
            )
            object_name = cur_obj.project_number
        else:
            object_name = cur_obj.name
            for attr, value in post_filter.items():
                query = query.filter(getattr(Post, attr) == value)
        posts = (query.
                 order_by(Post.timestamp.desc()).
                 paginate(page=page, per_page=5, error_out=False))
        next_url = url_for(route_prefix + current_page, page=posts.next_num,
                           object_name=object_name) if posts.has_next else None
        prev_url = url_for(route_prefix + current_page, page=posts.prev_num,
                           object_name=object_name) if posts.has_prev else None
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
    total_time = db.Column(db.Numeric)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'),
                             index=True)
    uploader_id = db.Column(db.Integer, db.ForeignKey('uploader.id'))
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))

    def get_rq_job(self):
        try:
            rq_job = rq.job.Job.fetch(self.id, connection=current_app.redis)
        except (redis.exceptions.RedisError, rq.exceptions.NoSuchJobError):
            return None
        return rq_job

    def get_progress(self):
        job = self.get_rq_job()
        return job.meta.get('progress', 0) if job is not None else 100

    def wait_for_job(self, loops=100):
        for x in range(loops):
            if self.get_progress() == 100:
                return True
            else:
                time.sleep(.1)
        return False

    def check_return_value(self, job, force_return):
        if force_return and job and not job.result:
            for x in range(50):
                job = self.get_rq_job()
                if job.result:
                    break
                else:
                    time.sleep(.1)
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
    def get_name_list(parameter='clientname', min_impressions=0):
        df = []
        a = ProcessorAnalysis.query.filter_by(
            processor_id=23, key='database_cache', parameter=parameter,
            filter_col='').order_by(ProcessorAnalysis.date).first()
        if a:
            df = pd.read_json(a.data)
            df = df[df[vmc.impressions] > min_impressions].sort_values(
                vmc.impressions, ascending=False)
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
    project = db.relationship('Project', backref='campaign', lazy='dynamic')

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

    def set_from_form(self, form, current_rate_card):
        for col in [Rates.adserving_fee, Rates.reporting_fee]:
            col = col.name
            val = form[col]
            if val == 'None' or val == '':
                val = 0
            setattr(self, col, float(val))
        self.type_name = form[Rates.type_name.name]
        self.rate_card_id = current_rate_card.id


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
    processor_reports = db.relationship('ProcessorReports',
                                        backref='processor', lazy='dynamic')
    dashboard = db.relationship(
        'Dashboard', backref='processor', lazy='dynamic')
    projects = db.relationship(
        'Project', secondary=project_number_processor,
        primaryjoin=(project_number_processor.c.processor_id == id),
        secondaryjoin="project_number_processor.c.project_id == Project.id",
        back_populates='processor_associated', lazy='dynamic')
    plans = db.relationship(
        'Plan', secondary=processor_plan,
        primaryjoin=(processor_plan.c.processor_id == id),
        secondaryjoin="processor_plan.c.plan_id == Plan.id",
        back_populates='processor_associated', lazy='dynamic')

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
        if not start_date:
            start_date = today
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

    def get_all_dashboards(self, report=False):
        if report:
            dashboards = self.dashboard.filter_by(
                include_in_report=report).order_by(
                Dashboard.created_at.desc()).all()
        else:
            dashboards = self.dashboard.order_by(
                Dashboard.created_at.desc()).all()
        return dashboards

    def get_url(self):
        return url_for('main.processor_page', object_name=self.name)

    def get_follow_url(self):
        return url_for('main.follow_processor', object_name=self.name)

    def get_unfollow_url(self):
        return url_for('main.unfollow_processor', object_name=self.name)

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

    def is_brandtracker(self):
        campaign = Campaign.query.filter_by(id=self.campaign_id).first()
        if not campaign:
            return False
        return campaign.name == 'BRANDTRACKER'

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
                       {'Accounts': ['main.edit_processor_accounts']},
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
                       {'Automatic Notes': ['main.edit_processor_auto_notes']},
                       {'Report Builder': ['main.edit_processor_report_builder']
                        }]
        elif buttons == 'ProcessorDuplicate':
            buttons = [{'Duplicate': ['main.edit_processor_duplication']}]
        elif buttons == 'ProcessorDashboard':
            buttons = [{'Create': ['main.processor_dashboard_create']},
                       {'View All': ['main.processor_dashboard_all']}]
        elif buttons == 'UploaderDCM':
            buttons = [
                {'Basic': ['main.edit_uploader', 'upload']},
                {'Campaign': ['main.edit_uploader_campaign_dcm',
                              'champagne-glasses']},
                {'Adset': ['main.edit_uploader_adset_dcm', 'bullseye']},
                {'Ad': ['main.edit_uploader_ad_dcm', 'rectangle-ad']}]
        elif buttons == 'UploaderFacebook':
            buttons = [
                {'Basic': ['main.edit_uploader', 'upload']},
                {'Campaign': ['main.edit_uploader_campaign',
                              'champagne-glasses']},
                {'Adset': ['main.edit_uploader_adset', 'bullseye']},
                {'Creative': ['main.edit_uploader_creative', 'palette']},
                {'Ad': ['main.edit_uploader_ad', 'rectangle-ad']}]
        elif buttons == 'UploaderAdwords':
            buttons = [
                {'Basic': ['main.edit_uploader', 'upload']},
                {'Campaign': ['main.edit_uploader_campaign_aw',
                              'champagne-glasses']},
                {'Adset': ['main.edit_uploader_adset_aw', 'bullseye']},
                {'Ad': ['main.edit_uploader_ad_aw', 'rectangle-ad']}]
        elif buttons == 'Plan':
            buttons = [{'Checklist': ['plan.checklist', 'list-check']},
                       {'Basic': ['plan.edit_plan', 'list-ol']},
                       {'Fees': ['plan.fees', 'comment-dollar']},
                       {'Topline': ['plan.topline', 'calendar']},
                       {'SOW': ['plan.edit_sow', 'file-signature']},
                       {'PlanRules': ['plan.plan_rules', 'ruler']},
                       {'RFP': ['plan.rfp', 'file-contract']},
                       {'PlanPlacements': ['plan.plan_placements', 'table']},
                       {'Specs': ['plan.specs', 'glasses']},
                       {'Contacts': ['plan.contacts', 'address-book']},
                       {'Calc': ['plan.calc', 'calculator']}]
        elif buttons == 'Research':
            buttons = [{'Basic': ['plan.edit_research', 'list-ol']},
                       {'CompetitiveSpend': [
                           'plan.competitive_spend', 'list-check']},
                       {'Brandtracker': [
                           'plan.edit_brandtracker', 'list-check']}]
        elif buttons == Project.__table__.name:
            buttons = [
                {'Basic': ['main.project_edit', 'list-ol']},
                {'Bill': ['main.project_billing', 'money-bill']}]
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
        return ['processor', 'report', 'data']

    @staticmethod
    def base_get_table_elem(table_name, db_model, obj_name):
        if db_model:
            db_model = db_model.__table__.name.capitalize()
        elem = """
            <div class="msgTableElem">
            <div id='{}' data-title="{}" 
                    data-object_name="{}" data-edit_name="{}">
            </div></div>""".format(
            table_name, db_model, obj_name, table_name)
        return elem

    def get_table_elem(self, table_name):
        elem = self.base_get_table_elem(table_name, Processor, self.name)
        return elem

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
                     'quick_fix': '.apply_quick_fix',
                     'check_processor_plan': '.check_processor_plan',
                     'apply_processor_plan': '.apply_processor_plan',
                     'get_plan_property': '.get_plan_property',
                     'screenshot': '.get_screenshot_table',
                     'screenshotImage': '.get_screenshot_image',
                     'notesTable': '.get_notes_table',
                     'pacingMetrics': '.get_processor_pacing_metrics',
                     'datasource_table': '.get_processor_data_source_table',
                     'singleNoteTable': '.get_single_notes_table',
                     'billingTable': '.get_billing_table',
                     'billingInvoice': '.get_billing_invoice',
                     'brandtracker_imports': '.get_brandtracker_imports',
                     'request_table': '.get_request_table',
                     'downloadTable': '.download_table',
                     'totalMetrics': '.get_processor_total_metrics',
                     'pacingAlertCount': '.get_pacing_alert_count',
                     'dash_placeholderMetrics': '.get_raw_file_data_table',
                     'oldFilePlotMetrics': '.get_raw_file_data_table',
                     'newFilePlotMetrics': '.get_raw_file_data_table',
                     'deltaFilePlotMetrics': '.get_raw_file_delta_table',
                     'dailyMetricsNotes': '.get_processor_daily_notes',
                     'pacingAlerts': '.get_pacing_alerts',
                     'ProjectNumber': '.get_project_number',
                     'projectObjects': '.get_project_objects',
                     'ProjectNumbers': '.get_project_numbers',
                     'getLog': '.get_logfile'}
        return arg_trans

    def get_import_form_dicts(self, reverse_sort_apis=False):
        imp_dict = []
        proc_imports = self.processor_datasources.all()
        if reverse_sort_apis:
            proc_imports.sort(reverse=True)
        for imp in proc_imports:
            if imp.name is not None:
                form_dict = imp.get_import_form_dict()
                imp_dict.append(form_dict)
        return imp_dict

    @staticmethod
    def get_children():
        return ProcessorDatasources

    def get_current_children(self):
        return self.processor_datasources.all()

    def get_example_prompt(self):
        r = 'Download data for the {} {}'.format(
            Processor.__table__.name, self.name)
        r = Uploader.wrap_example_prompt(r)
        return r

    @staticmethod
    def get_create_args_from_other(other_obj):
        args = Uploader.get_create_args_from_other(other_obj)
        return args

    def create_object(self, media_plan_data, is_df=False, current_user_id=None):
        import app.utils as app_utl
        if current_user_id:
            cu = db.session.get(User, current_user_id)
        else:
            cu = current_user
        app_utl.check_and_add_media_plan(
            media_plan_data, self, object_type=Uploader,
            current_user=cu, is_df=is_df)
        return True

    @staticmethod
    def get_base_path():
        return current_app.config['BASE_PROCESSOR_PATH']


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
    name = db.Column(db.Text, index=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    key = db.Column(db.String(64))
    account_id = db.Column(db.Text)
    account_filter = db.Column(db.Text)
    start_date = db.Column(db.Date)
    api_fields = db.Column(db.String(128))
    vendor_key = db.Column(db.Text)
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

    @staticmethod
    def get_children():
        return None

    @staticmethod
    def get_current_children():
        return []


class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'),
                             index=True)
    key = db.Column(db.String(64))
    account_id = db.Column(db.Text)
    campaign_id = db.Column(db.Text)
    username = db.Column(db.Text)
    password = db.Column(db.Text)

    def get_form_dict(self):
        fd = dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                   if not k.startswith("_")])
        return fd

    def set_from_form(self, form, current_processor):
        self.processor_id = current_processor.id
        for col in self.__table__.columns:
            if col.name in form:
                setattr(self, col.name, form[col.name])

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
    link = db.Column(db.Text)
    posts = db.relationship('Post', backref='notes', lazy='dynamic')

    @property
    def name(self):
        return '{} {} {}'.format(self.header, self.note_text, self.note_type)

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

    @staticmethod
    def get_model_name_list():
        return ['note', 'mortem']

    @staticmethod
    def get_table_name_to_task_dict():
        return {}

    def get_url(self):
        if self.link:
            url = self.link
        else:
            url = url_for('main.app_help')
            cur_proc = Processor.query.get(self.processor_id)
            if cur_proc:
                url = url_for('main.edit_processor_view_note',
                              object_name=cur_proc.name, note_id=self.id)
        return url

    @staticmethod
    def get_current_children():
        return []


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

    def get_url(self, route='main.uploader_page'):
        return url_for(route, object_name=self.name)

    def get_follow_url(self):
        return self.get_url(route='main.follow_processor')

    def get_unfollow_url(self):
        return self.get_url(route='main.unfollow_processor')


    def get_object_function_call(self):
        return {'object_name': self.name}

    @staticmethod
    def get_navigation_buttons():
        buttons = Processor.get_navigation_buttons('UploaderFacebook')
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

    @staticmethod
    def get_model_name_list():
        return ['uploader']

    @staticmethod
    def get_children():
        return None

    def get_current_children(self):
        return self.uploader_objects.all()

    @staticmethod
    def get_parent():
        return Campaign

    @staticmethod
    def get_default_cols(object_level='Campaign'):
        cols = [
            cre.MediaPlan.campaign_phase, cre.MediaPlan.partner_name,
            PartnerPlacements.country.name.capitalize()]
        if object_level == 'Adset':
            as_cols = [PartnerPlacements.targeting_bucket.name,
                       PartnerPlacements.environment.name]
            cols = cols + as_cols
        return cols

    def check_base_uploader_object(self, uploader_id, object_level='Campaign',
                                   uploader_type='Facebook'):
        new_uploader = db.session.get(Uploader, uploader_id)
        upo = UploaderObjects.query.filter_by(
            uploader_id=new_uploader.id, object_level=object_level,
            uploader_type=uploader_type).first()
        if not upo:
            upo = UploaderObjects(uploader_id=new_uploader.id,
                                  object_level=object_level,
                                  uploader_type=uploader_type)
            camp_cols = self.get_default_cols()
            as_cols = self.get_default_cols('Adset')
            fb_filter = 'Facebook|Instagram|Meta|Facebook/Instagram'
            aw_filter = 'Google SEM|Search|GDN'
            if object_level == 'Campaign':
                upo.media_plan_columns = camp_cols
                upo.partner_filter = 'Facebook|Instagram'
                upo.name_create_type = 'Media Plan'
                if uploader_type == 'Facebook':
                    upo.partner_filter = fb_filter
                elif uploader_type == 'Adwords':
                    upo.partner_filter = aw_filter
                else:
                    upo.partner_filter = ''
            elif object_level == 'Adset':
                upo.media_plan_columns = as_cols
                upo.name_create_type = 'Media Plan'
                if uploader_type == 'Facebook':
                    upo.partner_filter = fb_filter
                elif uploader_type == 'Adwords':
                    upo.partner_filter = aw_filter
                else:
                    upo.partner_filter = ''
            elif object_level == 'Ad':
                upo.media_plan_columns = [cre.MediaPlan.placement_name]
                upo.name_create_type = 'Media Plan'
                if uploader_type == 'Facebook':
                    upo.partner_filter = fb_filter
                elif uploader_type == 'Adwords':
                    upo.partner_filter = aw_filter
                else:
                    upo.partner_filter = ''
            else:
                pass
            db.session.add(upo)
            db.session.commit()
        return True

    def check_relation_uploader_objects(
            self, uploader_id, object_level='Campaign',
            uploader_type='Facebook'):
        import datetime as dt
        import uploader.upload.fbapi as up_fbapi
        import uploader.upload.awapi as up_awapi
        import uploader.upload.dcapi as up_dcapi
        new_uploader = db.session.get(Uploader, uploader_id)
        upo = UploaderObjects.query.filter_by(
            uploader_id=new_uploader.id, object_level=object_level,
            uploader_type=uploader_type).first()
        constant_col = 'relation_constant'
        position_col = 'position'
        default_sd = dt.datetime.today().strftime('%m/%d/%Y')
        default_ed = (dt.datetime.today()
                      + dt.timedelta(days=7)).strftime('%m/%d/%Y')
        relation_column_names = []
        camp_cols = list(range(len(self.get_default_cols())))
        as_cols = self.get_default_cols('Adset')
        if object_level == 'Campaign':
            if uploader_type == 'Facebook':
                fb_cu = up_fbapi.CampaignUpload
                relation_column_names = {
                    fb_cu.objective: {constant_col: 'OUTCOME_TRAFFIC'},
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
            target_col = as_cols.index(PartnerPlacements.targeting_bucket.name)
            cou_col = as_cols.index(PartnerPlacements.country.name.capitalize())
            ven_col = as_cols.index(cre.MediaPlan.partner_name)
            dev_col = as_cols.index(PartnerPlacements.environment.name)
            if uploader_type == 'Facebook':
                fb_asu = up_fbapi.AdSetUpload
                relation_column_names = {
                    fb_asu.cam_name: {position_col: camp_cols},
                    fb_asu.target: {position_col: [target_col]},
                    fb_asu.country: {position_col: [cou_col]},
                    fb_asu.age_min: {constant_col: '18'},
                    fb_asu.age_max: {constant_col: '44'},
                    fb_asu.genders: {constant_col: 'M'},
                    fb_asu.device: {position_col: [dev_col]},
                    fb_asu.pubs: {position_col: [ven_col]},
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
                    dcm_asu.campaignId: {position_col: camp_cols},
                    dcm_asu.siteId: {position_col: [ven_col]},
                    dcm_asu.compatibility: {position_col: [21]},
                    dcm_asu.size: {position_col: [20]},
                    dcm_asu.paymentSource: {
                        constant_col: 'PLACEMENT_AGENCY_PAID'},
                    dcm_asu.tagFormats: {position_col: [10]},
                    dcm_asu.startDate: {constant_col: default_sd},
                    dcm_asu.endDate: {constant_col: default_ed},
                    dcm_asu.pricingType: {constant_col: 'PRICING_TYPE_CPM'},
                }
            elif uploader_type == 'Adwords':
                aw_asu = up_awapi.AdGroupUpload
                relation_column_names = {
                    aw_asu.cam_name: {position_col: camp_cols},
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
            col_order = PartnerPlacements.get_col_order()
            camp_cols = [col_order.index(PlanPhase.__table__.name),
                         col_order.index(Partner.__table__.name),
                         col_order.index(PartnerPlacements.country.name)]
            cre_col = col_order.index(PartnerPlacements.creative_line_item.name)
            cop_col = col_order.index(PartnerPlacements.copy.name)
            as_cols = [col_order.index(PartnerPlacements.targeting_bucket.name),
                       col_order.index(PartnerPlacements.environment.name)]
            as_cols = camp_cols + as_cols
            if uploader_type == 'Facebook':
                fb_adu = up_fbapi.AdUpload
                relation_column_names = {
                    fb_adu.cam_name: {position_col: camp_cols},
                    fb_adu.adset_name: {position_col: as_cols},
                    fb_adu.filename: {position_col: [cre_col]},
                    fb_adu.prom_page: {constant_col: '_'},
                    fb_adu.ig_id: {constant_col: '_'},
                    fb_adu.link: {position_col: ''},
                    fb_adu.d_link: {constant_col: 'liquidadvertising.com'},
                    fb_adu.title: {position_col: [cop_col]},
                    fb_adu.body: {position_col: [cop_col]},
                    fb_adu.desc: {position_col: [cop_col]},
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

    def create_base_uploader_objects(self, uploader_id):
        for uploader_type in ['Facebook', 'DCM', 'Adwords']:
            for obj in ['Campaign', 'Adset', 'Ad']:
                self.check_base_uploader_object(uploader_id, obj, uploader_type)
            for obj in ['Campaign', 'Adset', 'Ad']:
                self.check_relation_uploader_objects(uploader_id, obj,
                                                     uploader_type)
        return True

    @staticmethod
    def get_create_args_from_other(other_obj):
        args = None
        if other_obj.__table__.name == Plan.__table__.name:
            args = other_obj.get_placements_as_df()
            col_dict = {
                PlanPhase.__table__.name: cre.MediaPlan.campaign_phase,
                Partner.__table__.name: cre.MediaPlan.partner_name,
                PartnerPlacements.name.name: cre.MediaPlan.placement_name}
            for col in [PartnerPlacements.country.name]:
                col_dict[col] = col.capitalize()
            args = args.rename(columns=col_dict)
        return [args, True]

    def create_object(self, media_plan_data, is_df=False, current_user_id=None):
        if current_user_id:
            cu = db.session.get(User, current_user_id)
        else:
            cu = current_user
        import app.utils as app_utl
        self.create_base_uploader_objects(self.id)
        new_path = app_utl.create_local_path(self)
        self.local_path = new_path
        db.session.commit()
        post_body = 'Create Uploader {}...'.format(self.name)
        self.launch_task('.create_uploader', post_body,
                         cu.id, current_app.config['BASE_UPLOADER_PATH'])
        creation_text = ('Uploader {} was requested for creation.'
                         ''.format(self.name))
        post = Post(body=creation_text, author=cu, uploader_id=self.id)
        db.session.add(post)
        db.session.commit()
        app_utl.check_and_add_media_plan(
            media_plan_data, self, object_type=Uploader,
            current_user=cu, is_df=is_df)
        if is_df or media_plan_data:
            self.media_plan = True
            db.session.commit()

    def get_table_elem(self, table_name):
        elem = Processor.base_get_table_elem(table_name, Uploader, self.name)
        return elem

    def get_types_from_words(self, words, up_types):
        cur_up = utl.is_list_in_list(words, up_types, False, True)
        if not cur_up:
            cur_up = up_types
        return cur_up[0]

    @staticmethod
    def wrap_example_prompt(response):
        example_prompt = "<br>Ex. prompt: <div class='examplePrompt'>"
        response = '{}{}</div>'.format(example_prompt, response)
        return response

    def get_create_prompt(self, next_level='campaign'):
        response = 'Run {} {} {}'.format(
            Uploader.__table__.name, self.name, next_level)
        response = self.wrap_example_prompt(response)
        if next_level == 'campaign':
            pre = 'To be guided on uploading use the below example prompt.'
        else:
            pre = 'To upload next level: '
        response = '{}{}'.format(pre, response)
        msg = ('<br>To view a preview table of what will be '
               'uploaded  use this prompt: {}'.format(
            response.replace('Run', '')))
        response += msg
        return response

    def check_relations(self, upo, as_response=True, response=''):
        unresolved_relations = []
        for x in upo.uploader_relations:
            page_id_cols = ['adset_page_id', 'ad_page_id']
            is_page_id = x.impacted_column_name in page_id_cols
            if is_page_id and x.relation_constant == '_':
                response = 'Change {} {} {} {} to 12345'.format(
                    self.name, Uploader.__table__.name,
                    UploaderRelations.relation_constant.name,
                    x.impacted_column_name)
                response = self.wrap_example_prompt(response)
                response = '{}{}'.format(x.impacted_column_name, response)
                unresolved_relations.append(x)
            elif (not x.relation_constant and x.unresolved_relations
                  and x.unresolved_relations != '0'):
                response += x.get_table_elem()
                unresolved_relations.append(x)
        if as_response:
            return response
        else:
            return unresolved_relations

    def run_object(self, words, with_run=True):
        response = ''
        uploader_types = ['facebook', 'dcm', 'adwords']
        cur_type = self.get_types_from_words(words, uploader_types)
        object_levels = ['campaign', 'adset', 'ad']
        cur_level = self.get_types_from_words(words, object_levels)
        upo = self.uploader_objects.filter_by(
            uploader_type=cur_type.capitalize(),
            object_level=cur_level.capitalize()).first()
        if upo.uploader_type == 'Facebook' and not self.fb_account_id:
            response = 'Change {} {} {} to 12345'.format(
                self.name, Uploader.__table__.name, Uploader.fb_account_id.name)
            response = self.wrap_example_prompt(response)
            response = '{}{}'.format(Uploader.fb_account_id.name, response)
        else:
            response = self.check_relations(upo, response=response)
        if response:
            pre = 'The following must be filled in prior to running:<br>'
            response = pre + response
        else:
            cur_idx = object_levels.index(cur_level)
            if cur_idx < (len(object_levels) - 1):
                next_level = object_levels[cur_idx + 1]
                response = self.get_create_prompt(next_level)
            else:
                response = 'All objects uploaded/uploading!'
            if with_run:
                run_resp = self.run(cur_type.capitalize(),
                                    cur_level.capitalize())
                response = '{} {}'.format(response, run_resp)
        return response

    def run(self, uploader_type, object_level):
        msg_text = 'Creating and uploading {} for {}.'.format(
            object_level, Uploader.__table__.name)
        self.launch_task(
            '.uploader_create_and_upload_objects', _(msg_text),
            running_user=current_user.id, object_level=object_level,
            uploader_type=uploader_type)
        db.session.commit()
        return msg_text

    def get_example_prompt(self):
        r = self.get_create_prompt(Uploader)
        return r

    @staticmethod
    def get_omit_cols():
        return [Uploader.media_plan.name]


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

    @property
    def name(self):
        return '{} {}'.format(self.uploader_type, self.object_level)

    @property
    def campaign(self):
        return 'Campaign'

    @property
    def ad_set(self):
        return 'Adset'

    @property
    def ad(self):
        return 'Ad'

    @staticmethod
    def string_to_list(string_value):
        return UploaderRelations.convert_string_to_list(string_value)

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])

    @staticmethod
    def get_children():
        return UploaderRelations

    def get_current_children(self):
        return self.uploader_relations.all()


class UploaderRelations(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    uploader_objects_id = db.Column(db.Integer,
                                    db.ForeignKey('uploader_objects.id'))
    impacted_column_name = db.Column(db.Text)
    relation_constant = db.Column(db.Text)
    position = db.Column(db.Text)
    unresolved_relations = db.Column(db.Text)

    @property
    def name(self):
        return '{}'.format(self.impacted_column_name)

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

    def get_table_elem(self):
        table_name = 'edit_relation'
        elem = """
                <div class="msgTableElem">
                <div>{}</div>
                <div id='{}' data-title="Uploader" 
                        data-object_name="{}" data-edit_name="{}"
                        data-vendor_key="{}" data-uploader_type="{}">
                </div></div>""".format(
            self.impacted_column_name.upper(), table_name,
            self.uploader_objects.uploader.name,
            self.uploader_objects.object_level,  self.impacted_column_name,
            self.uploader_objects.uploader_type)
        return elem


class ProcessorAnalysis(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'),
                             index=True)
    key = db.Column(db.Text, index=True)
    data = db.Column(db.JSON)
    message = db.Column(db.Text)
    date = db.Column(db.Date, index=True)
    parameter = db.Column(db.Text, index=True)
    parameter_2 = db.Column(db.Text)
    filter_col = db.Column(db.Text)
    filter_val = db.Column(db.Text)
    split_col = db.Column(db.Text)

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k not in [
                         'id', 'processor_id', 'date']])


class ProcessorReports(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    report = db.Column(db.JSON)
    report_name = db.Column(db.String(128), index=True)
    report_date = db.Column(db.Date, default=datetime.utcnow().date())
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k not in [
                         'id', 'processor_id']])


class Dashboard(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    chart_type = db.Column(db.Text)
    dimensions = db.Column(db.Text)
    metrics = db.Column(db.Text)
    default_view = db.Column(db.Text)
    tab = db.Column(db.Text)
    include_in_report = db.Column(db.Boolean)
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
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
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
    tasks = db.relationship('Task', backref='project', lazy='dynamic')
    campaign_id = db.Column(db.Integer, db.ForeignKey('campaign.id'))
    processor_associated = db.relationship(
        'Processor', secondary=project_number_processor,
        primaryjoin=(project_number_processor.c.project_id == id),
        secondaryjoin="project_number_processor.c.processor_id == Processor.id",
        back_populates='projects', lazy='dynamic')
    plan_associated = db.relationship(
        'Plan', secondary=project_number_plan,
        primaryjoin=(project_number_plan.c.project_id == id),
        secondaryjoin="project_number_plan.c.plan_id == Plan.id",
        back_populates='projects', lazy='dynamic')

    @property
    def name(self):
        return '{} {}'.format(self.project_number, self.project_name)

    @staticmethod
    def find_campaign(cur_obj):
        if not cur_obj.campaign_id:
            c = Campaign.query.filter_by(name=cur_obj.project_name).first()
            if not c:
                name = Client.get_default_name()[0]
                cli = Client(name=name).check_and_add()
                pro = Product(name=name, client_id=cli.id).check_and_add()
                c = Campaign(name=name, product_id=pro.id).check_and_add()
            cur_obj.campaign_id = c.id
            db.session.commit()
        return cur_obj

    def get_current_project(self, object_name=None, current_page=None,
                            edit_progress=0, edit_name='Page',
                            buttons=None, form_title=None,
                            form_description=None):
        if not buttons:
            buttons = Project.__table__.name
        kwargs = dict(title=_(Project.__table__.name.capitalize()),
                      object_name=object_name,
                      object_function_call={'object_name': object_name},
                      edit_progress=edit_progress, edit_name=edit_name,
                      form_title=form_title, form_description=form_description)
        if object_name:
            cur_obj = Project.query.filter_by(
                project_number=object_name).first_or_404()
            cur_obj = self.find_campaign(cur_obj)
            kwargs['object'] = cur_obj
            kwargs['buttons'] = Processor.get_navigation_buttons(buttons)
            posts, next_url, prev_url = Post.get_posts_for_objects(
                cur_obj, None, current_page,
                object_name=Project.__table__.name)
            kwargs['posts'] = posts.items
            kwargs['next_url'] = next_url
            kwargs['prev_url'] = prev_url
        else:
            kwargs['object'] = Project()
            kwargs['object_name'] = ''
        return kwargs

    @staticmethod
    def get_model_name_list():
        return ['project']

    @staticmethod
    def get_model_omit_search_list():
        return ['youtube']

    @staticmethod
    def get_children():
        return None

    @staticmethod
    def get_current_children():
        return []

    @staticmethod
    def get_parent():
        return Campaign

    @staticmethod
    def get_table_name_to_task_dict():
        return {}

    def get_url(self, route='main.project_number_page'):
        url = ''
        if self and self.name:
            url = url_for(route, object_name=self.name)
        return url

    def get_follow_url(self):
        return self.get_url(route='main.follow_processor')

    def get_unfollow_url(self):
        return self.get_url(route='main.unfollow_processor')

    def get_example_prompt(self):
        prompts = ['Project number 206196?']
        r = ''.join(Uploader.wrap_example_prompt(x) for x in prompts)
        return r

    def to_dict(self):
        return self.get_form_dict()

    def get_form_dict(self):
        date_cols = [Project.flight_start_date.name,
                     Project.flight_end_date.name]
        c = Campaign.query.filter_by(name=self.project_name).first()
        data = {
            Project.project_number.name: self.project_number,
            Project.project_name.name: self.project_name,
            Project.flight_start_date.name: self.flight_start_date,
            Project.flight_end_date.name: self.flight_end_date,
            Processor.__tablename__: len(self.processor_associated.all()),
            Plan.__tablename__: len(self.plan_associated.all()),
            Processor.campaign_id.name: c.id if c else None,
            Plan.user_id.name: self.user_id,
            Project.campaign_id.name: self.campaign_id}
        for x in date_cols:
            key_name = x.replace('flight_', '')
            data[key_name] = data.pop(x)
            if not data[key_name]:
                data[key_name] = datetime.today()
        return data

    def launch_task(self, name, description, running_user, *args, **kwargs):
        rq_job = current_app.task_queue.enqueue('app.tasks' + name,
                                                self.id, running_user,
                                                *args, **kwargs)
        task = Task(id=rq_job.get_id(), name=name, description=description,
                    user_id=self.user_id, project_id=self.id)
        db.session.add(task)
        return task

    @staticmethod
    def get_first_unique_name(name):
        name = Plan.get_first_unique_name(name, Project)
        return name

    def set_from_form(self, form, parent_model, current_user_id):
        for col in self.__table__.columns:
            if col.name in form:
                setattr(self, col.name, form[col.name])
        self.campaign_id = parent_model.id
        self.user_id = current_user_id

    def get_table_elem(self, table_name=''):
        elem = Processor.base_get_table_elem(table_name, Project, self.name)
        return elem

    def get_tasks_in_progress(self):
        return Task.query.filter_by(project=self, complete=False).all()

    def get_task_in_progress(self, name):
        return Task.query.filter_by(name=name, project=self,
                                    complete=False).first()


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
        p = 0
        if total_stages:
            p = int((completed_stages / total_stages) * 100)
        return p

    def get_stages(self):
        total_stages = self.tutorial_stage.all()
        return total_stages

    def tutorial_completed(self, tutorial_user):
        last_stage = len(self.tutorial_stage.all())
        user_complete = tutorial_user.tutorial_stages_completed.filter_by(
            tutorial_level=last_stage - 1, tutorial_id=self.id).first()
        return user_complete

    @staticmethod
    def get_all_tutorial_sheets():
        g_n = 'Glossary of Advertising and Gaming Abbreviations'
        g = (None, 'Glossary', g_n)
        ts_n = 'Time Savers - Software Helpers'
        ts_n_2 = ts_n.split(' - ')[0]
        ts = ('1QbKl6SSgm1DG7pYpXO76gGiuPQ5cV3umYgePkHwKf8Y', ts_n_2, ts_n)
        ai_n = 'AI - Playbook - Market'
        ai = ('139kGYyzlioabc1DlrH9ncyEhhQCk8DQl65ra1adLKXc',ai_n, ai_n)
        e_n = 'Effective RF Planning Model'
        e = ('1NCetqvNW4-UqJ5utk537dQi78L1K_yUIYm3cE7RVO-c', e_n, e_n)
        c_n = 'App - Chat - Capabilities'
        c = ('1Qeqf5CvvgCDRrtwpmO9MS4RBBbE36MOv5W92UOGnYQY', c_n, c_n)
        sheet_list = [g, ts, ai, e, c]
        return sheet_list


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

    @property
    def name(self):
        return '{} {} {}'.format(self.header, self.sub_header, self. message)

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

    @staticmethod
    def get_model_name_list():
        return ['tutorial', 'help', 'define']

    def get_url(self):
        return url_for('main.app_help') + '#{}'.format(self.header)

    @staticmethod
    def get_table_name_to_task_dict():
        return {}

    @staticmethod
    def get_current_children():
        return []

    def get_example_prompt(self):
        prompts = ['Define cpa.', 'How do I upload a raw file?',
                   'Best sites for for an mmorpg free trial?']
        r = ''.join(Uploader.wrap_example_prompt(x) for x in prompts)
        return r


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

    @property
    def name(self):
        walk = Walkthrough.query.get(self.walkthrough_id)
        return '{} {}'.format(walk.title, self.slide_text)

    def get_data(self):
        string_value = ''
        if self.data:
            string_value = ast.literal_eval(self.data)
        return string_value

    @staticmethod
    def get_model_name_list():
        return ['walkthrough']

    @staticmethod
    def get_table_name_to_task_dict():
        return {}

    @staticmethod
    def get_url():
        return url_for('main.app_help')

    @staticmethod
    def get_current_children():
        return []


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
    digital_agency_fees = db.Column(db.Numeric)
    trad_agency_fees = db.Column(db.Numeric)
    dcm_service_fees = db.Column(db.Numeric)
    tasks = db.relationship('Task', backref='plan', lazy='dynamic')
    posts = db.relationship('Post', backref='plan', lazy='dynamic')
    rate_card_id = db.Column(db.Integer, db.ForeignKey('rate_card.id'))
    campaign_id = db.Column(db.Integer, db.ForeignKey('campaign.id'))
    processor_associated = db.relationship(
        'Processor', secondary=processor_plan,
        primaryjoin=(processor_plan.c.plan_id == id),
        secondaryjoin="processor_plan.c.processor_id == Processor.id",
        lazy='dynamic', back_populates='plans')
    projects = db.relationship(
        'Project', secondary=project_number_plan,
        primaryjoin=(project_number_plan.c.plan_id == id),
        secondaryjoin="project_number_plan.c.project_id == Project.id",
        lazy='dynamic', back_populates='plan_associated')
    phases = db.relationship('PlanPhase', backref='plan', lazy='dynamic')
    rules = db.relationship('PlanRule', backref='plan', lazy='dynamic')

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
                         edit_progress=0, edit_name='Page', buttons='Plan'):
        output_links = self.get_output_links()
        kwargs = dict(title=_('Plan'), object_name=object_name,
                      object_function_call={'object_name': object_name},
                      edit_progress=edit_progress, edit_name=edit_name,
                      output_links=output_links,
                      buttons=Processor.get_navigation_buttons(buttons))
        if object_name:
            cur_obj = Plan.query.filter_by(name=object_name).first_or_404()
            kwargs['object'] = cur_obj
            posts, next_url, prev_url = Post.get_posts_for_objects(
                cur_obj, None, current_page, 'plan')
            kwargs['posts'] = posts.items
            kwargs['next_url'] = next_url
            kwargs['prev_url'] = prev_url
        else:
            kwargs['object'] = Plan()
            kwargs['object_name'] = ''
            kwargs['object_function_call']['object_name'] = ''
        return kwargs

    def get_url(self, route='plan.edit_plan'):
        url = ''
        if self and self.name:
            url = url_for(route, object_name=self.name)
        return url

    def get_follow_url(self):
        return self.get_url(route='main.follow_processor')

    def get_unfollow_url(self):
        return self.get_url(route='main.unfollow_processor')

    def get_last_post(self):
        posts = []
        if self and self.name:
            posts = self.posts.order_by(Post.timestamp.desc()).first()
        return posts

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

    def get_current_children(self):
        return self.phases.all()

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
    def get_first_unique_name(name, db_model=None):
        if not db_model:
            db_model = Plan
        name_exists = db_model.query.filter_by(name=name).first()
        if name_exists:
            for x in range(100):
                new_name = '{}_{}'.format(name, x)
                name_exists = db_model.query.filter_by(name=new_name).first()
                if not name_exists:
                    name = new_name
                    break
        return name

    def get_table_elem(self, table_name=''):
        if not table_name:
            table_name = 'Topline'
        elem = Processor.base_get_table_elem(table_name, Plan, self.name)
        return elem

    @staticmethod
    def get_table_name_to_task_dict():
        arg_trans = {
            'SOW': '.get_sow',
            'Topline': '.get_topline',
            'ToplineDownload': '.download_topline',
            'PlanRules': '.get_plan_rules',
            'PlanPlacements': '.get_plan_placements',
            'RFP': '.get_rfp',
            'Specs': '.get_specs',
            'Contacts': '.get_contacts',
            'Calc': '.get_plan_calc',
            'Checklist': '.get_checklist'
        }
        return arg_trans

    def get_placements_as_df(self):
        data = [x.get_form_dict() for x in self.get_placements()]
        df = pd.DataFrame(data)
        cols = PartnerPlacements.get_col_order()
        cols = cols + [x for x in df.columns if x not in cols]
        for col in cols:
            if col not in df.columns:
                df[col] = ''
        df = df[cols]
        return df

    def get_partners(self):
        data = []
        for plan_phase in self.phases.all():
            for plan_part in plan_phase.partners.all():
                data.append(plan_part)
        return data

    def get_placements(self):
        data = []
        cur_partners = self.get_partners()
        for cur_partner in cur_partners:
            places = [x for x in cur_partner.placements]
            data.extend(places)
        return data

    @staticmethod
    def get_create_args_from_other(other_obj):
        return []

    def create_object(self, args):
        return True

    def to_dict(self):
        return dict(
            [(k.name, getattr(self, k.name)) for k in Plan.__table__.columns
             if not k.name.startswith("_") and k.name != 'id'])

    def get_create_prompt(self, wrap_html=True):
        return_prompts = []
        prompt_list = [
            'download sow',
            'change budget for partner_name to new_budget',
            'create an uploader',
            'change {} to num'.format(PartnerPlacements.environment.name)]
        p = ''
        for x in prompt_list:
            x = '{} {} {}'.format(Plan.__table__.name, self.name, x)
            if wrap_html:
                x = Uploader.wrap_example_prompt(x)
                p += x
            else:
                return_prompts.append(x)
                p = return_prompts
        return p

    @staticmethod
    def get_large_create_prompt(
            prompt_dict=False, partner=None, spend=None, environments=None,
            creative=None, targeting=None, copy=None):
        if not partner:
            partner = ['Facebook']
        if not spend:
            spend = ['20K']
        if not environments:
            environments = ['Mobile', 'Desktop']
        if not creative:
            creative = ['Ciri', 'Yen', 'Geralt', 'Triss']
        if not targeting:
            targeting = ['Aaa', 'Jrpg', 'Mmorpg']
        if not copy:
            copy = ['X', 'Y']
        pairs = {PartnerPlacements.targeting_bucket.name: targeting,
                 PartnerPlacements.creative_line_item.name: creative,
                 PartnerPlacements.copy.name: copy}
        x = 'Create a plan with '
        for idx, part in enumerate(partner):
            x += '{} {}.  '.format(part, spend[idx])
        x += 'Split US 50% UK 30% CA 20%. '
        x += '{}. '.format(', '.join(environments))
        prompt_param_dict = {
            Partner.__table__.name: partner,
            Partner.total_budget.name: spend}
        for k, v in pairs.items():
            x += '{} {}.  '.format(
                k.split('_')[0].capitalize(), ', '.join(v))
            prompt_param_dict[k] = v
        if prompt_dict:
            prompt_param_dict['message'] = x
            x = prompt_param_dict
        return x

    def get_create_from_another_prompt(self):
        partner = ['Facebook', 'YouTube']
        spend = ['$17,000', '$33,000']
        sd = ['10/5/2023']
        ed = ['11/16/2023']
        targeting = ['Persona/Atlus', 'RPG/JRPG', 'Strategy/Tactical']
        creative = ['15s', '30s', '60s', 'Full']
        copy = ['Copy 01', 'Copy 02', 'Copy 03']
        environments = ['Cross Device']
        prompt_dict = self.get_large_create_prompt(
            True, partner, spend, environments, creative, targeting,
            copy)
        for pair in [(sd, Plan.start_date), (ed, Plan.end_date)]:
            k = ' '.join(x.capitalize() for x in pair[1].name.split('_'))
            prompt_dict[pair[1].name] = pair[0]
            prompt_dict['message'] += '{} is {}.  '.format(k, pair[0][0])
        t = PartnerPlacements.targeting_bucket.name.split('_')[0].capitalize()
        prompt_dict['message'] = prompt_dict['message'].replace(t, 'Audiences')
        prompt_dict['message'] += 'From {} 12345'.format(Project.__table__.name)
        return prompt_dict

    def get_example_prompt(self):
        r = self.get_create_prompt(Plan)
        x = self.get_large_create_prompt()
        r += Uploader.wrap_example_prompt(x)
        x = self.get_create_from_another_prompt(Plan)
        r += Uploader.wrap_example_prompt(x['message'])
        return r

    def launch_placement_task(self, new_g_children, words, message,
                              brand_new_ids=None, current_user_id=None):
        cu = current_user
        if not cu:
            cu = db.session.get(User, current_user_id)
        total_db = pd.DataFrame()
        msg_text = 'Checking plan placements.'
        self.launch_task(
            '.plan_check_placements', msg_text,
            running_user=cu.id, words=words, total_db=total_db,
            new_g_children=new_g_children, message=message,
            brand_new_ids=brand_new_ids)
        db.session.commit()
        return True

    @staticmethod
    def get_mock_plan(basedir='', check_for_plan=False, app_cols=False):
        file_name = os.path.join(basedir, 'mediaplan.xlsx')
        if check_for_plan and os.path.exists(file_name):
            df = utl.import_read_csv(file_name)
        elif app_cols:
            cols = PartnerPlacements.get_plan_col_translation()
            row = ['test{}'.format(x) for x in range(10)]
            data = {col.name: row for col in cols}
            df = pd.DataFrame(data)
            df[PartnerPlacements.total_budget.name] = 100
            date_cols = [PartnerPlacements.start_date.name,
                         PartnerPlacements.end_date.name]
            for col in date_cols:
                df[col] = datetime.today().date()
        else:
            data = {
                'Campaign Phase': ['Launch', 'Launch', 'Launch', 'Launch'],
                'Partner Name': ['Facebook', 'Facebook', 'Facebook',
                                 'Facebook'],
                'Country': ['US', 'US', 'CA', 'CA'],
                'Environment': ['Mobile', 'Desktop', 'Mobile', 'Desktop'],
                'Targeting': ['Target A', 'Target B', 'Target A',
                              'Target B'],
                'Creative': ['Creative 1', 'Creative 2', 'Creative 1',
                             'Creative 2'],
                'Copy': ['Copy 1', 'Copy 2', 'Copy 1', 'Copy 2'],
                'Serving': [
                    vmc.clicks, vmc.clicks, vmc.clicks, vmc.impressions],
                'Net Cost': [12000, 11000, 13000, 10000]
            }
            df = pd.DataFrame(data)
        return df


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

    def create_from_plan(self, cur_plan):
        self.plan_id = cur_plan.id
        self.project_name = cur_plan.name
        self.date_submitted = datetime.today().date()
        liquid_contact = db.session.get(User, cur_plan.user_id)
        if not liquid_contact:
            liquid_contact = current_user
        self.liquid_contact = liquid_contact.username
        cur_proj = cur_plan.projects.first()
        self.liquid_project = cur_proj if cur_proj else 0
        self.start_date = cur_plan.start_date
        self.end_date = cur_plan.end_date
        self.client_name = cur_plan.campaign.product.client.name
        self.campaign = cur_plan.campaign.name
        self.address = '138 Eucalyptus Drive, El Segundo, CA 90245'
        self.phone = '310.450.2653'
        self.fax = '310.450.2658'
        self.total_project_budget = cur_plan.total_budget
        if cur_plan.total_budget:
            budget_val = cur_plan.total_budget
        else:
            budget_val = 0
        self.ad_serving = float(budget_val) * .007


class InsertionOrder(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    partner_id = db.Column(db.Integer, db.ForeignKey('partner.id'))
    insertion_order = db.Column(db.String)
    project_number = db.Column(db.Integer)
    document_date = db.Column(db.String)
    billing_contact = db.Column(db.String)
    attn = db.Column(db.String)
    media_representative = db.Column(db.String)
    publisher_contact = db.Column(db.String)
    publisher_contact_email = db.Column(db.String)
    client = db.Column(db.String)
    agency_contact = db.Column(db.String)
    agency_contact_email = db.Column(db.String)
    campaign_name = db.Column(db.String)

    def create_from_plan(self, cur_plan):
        self.id = cur_plan.id
        self.insertion_order = cur_plan.insertion_order
        self.project_number = cur_plan.project_number
        self.document_date = cur_plan.document_date
        self.billing_contact = cur_plan.billing_contact
        self.attn = cur_plan.attn
        self.media_representative = cur_plan.media_representative
        self.publisher_contact = cur_plan.publisher_contact
        self.publisher_contact_email = cur_plan.publisher_contact_email
        self.client = cur_plan.client
        self.agency_contact = cur_plan.agency_contact
        self.agency_contact_email = cur_plan.agency_contact_email
        self.campaign_name = cur_plan.campaign_name


class PlanPhase(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    partners = db.relationship('Partner', backref='plan', lazy='dynamic')
    unique_name = True

    def get_form_dict(self):
        sd = self.start_date if self.start_date else datetime.today()
        ed = self.end_date if self.end_date else datetime.today()
        form_dict = {
            'id': self.id,
            'name': self.name,
            'start_date': datetime.strftime(sd, '%Y-%m-%d'),
            'end_date': datetime.strftime(ed, '%Y-%m-%d')
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
        if 'id' in form and form['id'] != 'undefined':
            self.id = form['id']

    @staticmethod
    def get_name_list():
        return ['launch', 'pre-launch', 'prelaunch', 'pre-order', 'preorder',
                'announce', 'sale']

    def get_current_children(self):
        return self.partners.all()

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
    rules = db.relationship('PlanRule', backref='partner', lazy='dynamic')
    rfp = db.relationship('Rfp', backref='partner', lazy='dynamic')
    specs = db.relationship('Specs', backref='p_partner', lazy='dynamic')
    contacts = db.relationship('Contacts', backref='p_partner', lazy='dynamic')
    insertion_order = db.relationship('InsertionOrder', backref='p_partner',
                                      lazy='dynamic')
    unique_name = True

    def get_form_dict(self, cur_phase=None):
        form_dict = {
            'id': self.id,
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
        self.name = form[Partner.__table__.name]
        omit_cols = [Partner.name.name]
        cols = [x for x in self.__table__.columns if x.name not in omit_cols]
        est_val = Partner.estimated_cpc.name.split('_')[0]
        for col in cols:
            search_name = col.name
            missing_val = 'None'
            is_key_col = col.primary_key or col.foreign_keys
            if isinstance(col.type, db.Numeric) or is_key_col:
                missing_val = 0
            elif isinstance(col.type, db.Date):
                missing_val = datetime.today().date()
                if col.name == Partner.end_date.name:
                    missing_val += timedelta(days=7)
            if est_val in search_name and search_name not in form:
                search_name = search_name.replace('{}_'.format(est_val), '')
            new_val = utl.check_dict_for_key(form, search_name, missing_val)
            if new_val == 'undefined':
                new_val = missing_val
            if not is_key_col or (
                    is_key_col and int(new_val) != int(missing_val)):
                setattr(self, col.name, new_val)

    @staticmethod
    def get_name_list(parameter='vendorname|vendortypename'):
        ven_col = prc_model.Vendor.vendorname.name
        vty_col = prc_model.Vendortype.vendortypename.name
        imp_col = prc_model.Event.impressions.name
        cli_col = prc_model.Event.clicks.name
        net_col = prc_model.Event.netcost.name
        lp_col = prc_model.Event.landingpage.name
        vew_col = prc_model.Event.videoviews.name
        vhu_col = prc_model.Event.videoviews100.name
        bc_col = prc_model.Event.buttonclick.name
        cpm_col = 'CPM'
        cpc_col = 'CPC'
        clp_col = 'CPLPV'
        cpb_col = 'CPBC'
        cpv_col = 'CPV'
        a = ProcessorAnalysis.query.filter_by(
            processor_id=23, key='database_cache',
            parameter=parameter).order_by(ProcessorAnalysis.date).first()
        if a:
            df = pd.read_json(a.data)
        else:
            def_dict = {
                ven_col: ['Facebook'],
                vty_col: ['Social']
            }
            event_cols = [imp_col, net_col, cli_col, lp_col, vew_col, vhu_col,
                          bc_col, clp_col, cpb_col, cpv_col, cpm_col, cpc_col]
            for col in event_cols:
                def_dict[col] = [1]
            df = pd.DataFrame(def_dict)
        df = df[df[imp_col] > 0].sort_values(imp_col, ascending=False)
        df['cpm'] = (df[net_col] / (df[imp_col] / 1000)).round(2)
        df['cpc'] = (df[net_col] / df[cli_col]).round(2)
        df[clp_col.lower()] = df[clp_col].round(2)
        df['Landing Page'] = df[lp_col]
        df[cpb_col.lower()] = df[cpb_col].round(2)
        df['Button Clicks'] = df[bc_col]
        df['Views'] = df[vew_col]
        df[cpv_col.lower()] = df[cpv_col].round(2)
        df['Video Views 100'] = df[vhu_col]
        df['cpcv'] = (df[net_col] / df[vhu_col]).round(2)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna(0)
        df = df[[ven_col, vty_col, 'cpm', 'cpc',
                 clp_col.lower(), cpb_col.lower(), cpv_col.lower(), 'cpcv']]
        partner_name = Partner.__table__.name
        partner_type_name = Partner.partner_type.name
        df = df.rename(columns={
            ven_col: partner_name, vty_col: partner_type_name})
        omit_list = ['Full']
        df = df[~df[partner_name].isin(omit_list)]
        partner_list = df.to_dict(orient='records')
        partner_type_list = pd.DataFrame(
            df[partner_type_name].unique()).rename(
            columns={0: partner_type_name}).to_dict(orient='records')
        return partner_list, partner_type_list

    @staticmethod
    def get_children():
        return PartnerPlacements

    def get_current_children(self):
        c = (self.placements.all() + self.rules.all() + self.rfp.all() +
             self.specs.all() + self.contacts.all())
        return c

    @staticmethod
    def get_metric_cols():
        p = Partner
        cpm = p.estimated_cpm.name.split('_')[1]
        cpc = p.estimated_cpc.name.split('_')[1]
        form_cols = [p.total_budget.name, cpm, cpc, p.cplpv.name, p.cpbc.name,
                     p.cpv.name, p.cpcv.name]
        def_metric_cols = [cpm, vmc.impressions, cpc, vmc.clicks]
        metric_cols = def_metric_cols + [
            p.cplpv.name, vmc.landingpage, p.cpbc.name, vmc.btnclick,
            p.cpv.name, vmc.views, p.cpcv.name, vmc.views100]
        return form_cols, metric_cols, def_metric_cols


class RfpFile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    name = db.Column(db.Text)
    placements = db.relationship('Rfp', backref='rfp_file', lazy='dynamic')
    specs = db.relationship('Specs', backref='rfp_file', lazy='dynamic')
    contacts = db.relationship('Contacts', backref='rfp_file', lazy='dynamic')

    @staticmethod
    def create_name(df):
        cols = Rfp.column_translation()
        return '|'.join(sorted(list(df[cols[Rfp.partner_name.name]].unique())))


class Rfp(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    partner_name = db.Column(db.Text)
    package_name_description = db.Column(db.Text)
    placement_name_description = db.Column(db.Text)
    ad_size_wxh = db.Column(db.Text)
    ad_type = db.Column(db.Text)
    device = db.Column(db.Text)
    country = db.Column(db.Text)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    buy_model = db.Column(db.Text)
    planned_impressions = db.Column(db.Integer)
    planned_units = db.Column(db.Text)
    cpm_cost_per_unit = db.Column(db.Numeric)
    planned_net_cost = db.Column(db.Numeric)
    planned_sov = db.Column(db.Numeric)
    reporting_source = db.Column(db.Text)
    ad_serving_type = db.Column(db.Text)
    targeting = db.Column(db.Text)
    placement_phase = db.Column(db.Text)
    placement_objective = db.Column(db.Text)
    kpi = db.Column(db.Text)
    sizmek_id = db.Column(db.Text)
    rfp_file_id = db.Column(db.Integer, db.ForeignKey('rfp_file.id'))
    partner_id = db.Column(db.Integer, db.ForeignKey('partner.id'))

    @staticmethod
    def column_translation():
        original_column_names = {
            Rfp.partner_name.name: 'Partner Name',
            Rfp.package_name_description.name: 'Package Name/Description',
            Rfp.placement_name_description.name: 'Placement Name/Description',
            Rfp.ad_size_wxh.name: 'Ad Size \n(WxH)',
            Rfp.ad_type.name: 'Ad Type',
            Rfp.device.name: 'Device',
            Rfp.country.name: 'Country',
            Rfp.start_date.name: 'Start Date',
            Rfp.end_date.name: 'End Date',
            Rfp.buy_model.name: 'Buy Model',
            Rfp.planned_impressions.name: 'Planned Impressions',
            Rfp.planned_units.name: 'Planned Units \n(eg. CPV, CPE, CPI)',
            Rfp.cpm_cost_per_unit.name: 'CPM / Cost Per Unit',
            Rfp.planned_net_cost.name: 'Planned Net Cost',
            Rfp.planned_sov.name: 'Planned SOV %',
            Rfp.reporting_source.name: 'Reporting Source',
            Rfp.ad_serving_type.name: 'Ad Serving Type',
            Rfp.targeting.name: 'Targeting ',
            Rfp.placement_phase.name: 'Placement Phase\n(If Needed) ',
            Rfp.placement_objective.name: 'Placement Objective\n(If Needed) ',
            Rfp.kpi.name: 'KPI \n(If Needed) ',
            Rfp.sizmek_id.name: 'Sizmek ID \n(Optional) '
        }
        return original_column_names

    def set_from_form(self, form, current_object):
        for col in self.__table__.columns:
            if col.name in form:
                setattr(self, col.name, form[col.name])

    def get_form_dict(self):
        fd = dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                   if not k.startswith("_") and k != 'id'])
        return fd


    @property
    def name(self):
        cols = self.column_translation()
        n = ' '.join(['{}'.format(getattr(self, key)) for key in cols.keys()])
        return n

    @staticmethod
    def get_model_name_list():
        return ['rfp', 'proposal']

    @staticmethod
    def get_children():
        return None

    @staticmethod
    def get_current_children():
        return []

    @staticmethod
    def get_parent():
        return RfpFile

    @staticmethod
    def get_table_name_to_task_dict():
        return {}

    def get_url(self):
        plan = db.session.get(Plan, self.rfp_file.plan_id)
        return url_for('plan.rfp', object_name=plan.name)

    def get_example_prompt(self):
        prompts = ['Get me an rfp for Paramount.']
        r = ''.join(Uploader.wrap_example_prompt(x) for x in prompts)
        return r

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])


class Specs(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    due_date = db.Column(db.Text)
    partner = db.Column(db.Text)
    file_name = db.Column(db.Text)
    media_type = db.Column(db.Text)
    environment = db.Column(db.Text)
    dimensions = db.Column(db.Text)
    file_type = db.Column(db.Text)
    file_size = db.Column(db.Text)
    key_art_direction = db.Column(db.Text)
    messaging = db.Column(db.Text)
    weight = db.Column(db.Text)
    audience = db.Column(db.Text)
    phase_cta = db.Column(db.Text)
    loops_animation = db.Column(db.Text)
    aspect_ratio = db.Column(db.Text)
    video_length = db.Column(db.Text)
    file_formats = db.Column(db.Text)
    max_video_size_mb = db.Column(db.Text)
    optimal_video_load_size_mb = db.Column(db.Text)
    frame_rate = db.Column(db.Text)
    bitrate = db.Column(db.Text)
    hd_sd = db.Column(db.Text)
    notes = db.Column(db.Text)
    rfp_file_id = db.Column(db.Integer, db.ForeignKey('rfp_file.id'))
    partner_id = db.Column(db.Integer, db.ForeignKey('partner.id'))

    @staticmethod
    def column_translation():
        opt_size = 'Optimal Video Load Size (MB)'
        original_column_names = {
            Specs.due_date.name: 'Due Date',
            Specs.partner.name: 'Partner',
            Specs.file_name.name: 'File Name',
            Specs.media_type.name: 'Media Type',
            Specs.environment.name: 'Environment',
            Specs.dimensions.name: 'Dimensions',
            Specs.file_type.name: 'File Type',
            Specs.file_size.name: 'File Size',
            Specs.key_art_direction.name: 'Key Art Direction',
            Specs.messaging.name: 'Messaging',
            Specs.weight.name: 'Weight',
            Specs.audience.name: 'Audience',
            Specs.phase_cta.name: 'Phase/CTA',
            Specs.loops_animation.name: 'Loops/Animation',
            Specs.aspect_ratio.name: 'Aspect Ratio',
            Specs.video_length.name: 'Video Length',
            Specs.file_formats.name: 'File Formats',
            Specs.max_video_size_mb.name: 'Max Video Size (MB)',
            Specs.optimal_video_load_size_mb.name: opt_size,
            Specs.frame_rate.name: 'Frame Rate',
            Specs.bitrate.name: 'Bitrate',
            Specs.hd_sd.name: 'HD/SD',
            Specs.notes.name: 'Notes'
        }
        return original_column_names

    def set_from_form(self, form, current_object):
        for col in self.__table__.columns:
            if col.name in form:
                setattr(self, col.name, form[col.name])

    def get_form_dict(self):
        fd = dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                   if not k.startswith("_") and k != 'id'])
        return fd

    @property
    def name(self):
        cols = self.column_translation()
        n = ' '.join(['{}'.format(getattr(self, key)) for key in cols.keys()])
        return n

    @staticmethod
    def get_model_name_list():
        return ['specs', 'spec', 'specifications']

    @staticmethod
    def get_children():
        return None

    @staticmethod
    def get_current_children():
        return []

    @staticmethod
    def get_parent():
        return RfpFile

    @staticmethod
    def get_table_name_to_task_dict():
        return {}

    def get_url(self):
        plan = db.session.get(Plan, self.rfp_file.plan_id)
        return url_for('plan.specs', object_name=plan.name)

    def get_example_prompt(self):
        prompts = ['What are the specs for Paramount?']
        r = ''.join(Uploader.wrap_example_prompt(x) for x in prompts)
        return r

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])


class Contacts(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date_submitted = db.Column(db.Date)
    partner_name = db.Column(db.Text)
    sales_representative = db.Column(db.Text)
    phone1 = db.Column(db.Text)  # Phone for Sales
    email_address1 = db.Column(db.Text)  # Email Address for Sales
    am_traffic_contact = db.Column(db.Text)
    phone2 = db.Column(db.Text)  # Phone for AM/Traffic
    email_address2 = db.Column(db.Text)  # Email Address for AM/Traffic
    director = db.Column(db.Text)
    associate_media_director = db.Column(db.Text)
    supervisor = db.Column(db.Text)
    associate_strategist = db.Column(db.Text)
    contact_title = db.Column(db.Text)
    rfp_file_id = db.Column(db.Integer, db.ForeignKey('rfp_file.id'))
    partner_id = db.Column(db.Integer, db.ForeignKey('partner.id'))

    @staticmethod
    def column_translation():
        email_sales = 'Email Address (Sales Representative)'
        original_column_names = {
            Contacts.partner_name.name: 'Partner Name',
            Contacts.sales_representative.name: 'Sales Representative',
            Contacts.phone1.name: 'Phone (Sales Representative)',
            Contacts.email_address1.name: email_sales,
            Contacts.am_traffic_contact.name: 'AM/Traffic Contact',
            Contacts.phone2.name: 'Phone (AM/Traffic Contact)',
            Contacts.email_address2.name: 'Email Address (AM/Traffic Contact)',
            Contacts.director.name: 'Director',
            Contacts.associate_media_director.name: 'Associate Media Director',
            Contacts.supervisor.name: 'Supervisor',
            Contacts.associate_strategist.name: 'Associate Strategist'
        }
        return original_column_names

    def set_from_form(self, form, current_object):
        for col in self.__table__.columns:
            if col.name in form:
                setattr(self, col.name, form[col.name])

    def get_form_dict(self):
        fd = dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                   if not k.startswith("_") and k != 'id'])
        return fd

    @property
    def name(self):
        cols = self.column_translation()
        n = ' '.join(['{}'.format(getattr(self, key)) for key in cols.keys()])
        return n

    @staticmethod
    def get_model_name_list():
        return ['contact', 'email', 'phone']

    @staticmethod
    def get_children():
        return None

    @staticmethod
    def get_current_children():
        return []

    @staticmethod
    def get_parent():
        return RfpFile

    @staticmethod
    def get_table_name_to_task_dict():
        return {}

    def get_url(self):
        plan = db.session.get(Plan, self.rfp_file.plan_id)
        return url_for('plan.contacts', object_name=plan.name)

    def get_example_prompt(self):
        prompts = ['Who is the contact for Paramount?']
        r = ''.join(Uploader.wrap_example_prompt(x) for x in prompts)
        return r

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])


class PartnerPlacements(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, index=True)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    budget = db.Column(db.Text)
    country = db.Column(db.Text)
    targeting_bucket = db.Column(db.Text)
    creative_line_item = db.Column(db.Text)
    copy = db.Column(db.Text)
    retailer = db.Column(db.Text)
    buy_model = db.Column(db.Text)
    buy_rate = db.Column(db.Text)
    serving = db.Column(db.Text)
    ad_rate = db.Column(db.Text)
    reporting_rate = db.Column(db.Text)
    kpi = db.Column(db.Text)
    data_type_1 = db.Column(db.Text)
    service_fee_rate = db.Column(db.Text)
    verification_rate = db.Column(db.Text)
    reporting_source = db.Column(db.Text)
    environment = db.Column(db.Text)
    size = db.Column(db.Text)
    ad_type = db.Column(db.Text)
    placement_description = db.Column(db.Text)
    package_description = db.Column(db.Text)
    media_channel = db.Column(db.Text)
    total_budget = db.Column(db.Numeric)
    cpm = db.Column(db.Numeric)
    cpc = db.Column(db.Numeric)
    cplpv = db.Column(db.Numeric)
    cpbc = db.Column(db.Numeric)
    cpv = db.Column(db.Numeric)
    cpcv = db.Column(db.Numeric)
    partner_id = db.Column(db.Integer, db.ForeignKey('partner.id'))
    unique_name = True

    @property
    def plan_phase(self):
        part = db.session.get(Partner, self.partner_id)
        phase = db.session.get(PlanPhase, part.plan_phase_id)
        return phase.name

    @staticmethod
    def get_cols_for_db():
        cols = ['vendorname', 'countryname', 'targetingbucketname',
                'creativelineitemname', 'copyname', 'retailername',
                'buymodelname', 'servingname', 'kpiname', 'datatype1name',
                'environmentname', 'adsizename', 'adtypename',
                'placementdescriptionname', 'packagedescriptionname',
                'mediachannelname']
        return cols

    @staticmethod
    def get_plan_col_translation():
        p = PartnerPlacements
        data_dict = {
            p.budget: [cre.MediaPlan.campaign_id],
            Partner.__table__: [cre.MediaPlan.partner_name],
            p.country: [cre.MediaPlan.country_name],
            p.targeting_bucket: [cre.MediaPlan.targeting],
            p.creative_line_item: [cre.MediaPlan.creative],
            p.copy: [cre.MediaPlan.copy],
            p.retailer: [cre.MediaPlan.data_source],
            p.buy_model: [cre.MediaPlan.buy_model],
            p.buy_rate: [cre.MediaPlan.buy_rate],
            p.start_date: [cre.MediaPlan.start_date],
            p.serving: [cre.MediaPlan.serving],
            p.ad_rate: [cre.MediaPlan.ad_rate],
            p.reporting_rate: [cre.MediaPlan.report_rate],
            p.kpi: [cre.MediaPlan.kpi],
            p.data_type_1: [cre.MediaPlan.placement_objective],
            PlanPhase.__table__: [cre.MediaPlan.placement_phase],
            p.service_fee_rate: [cre.MediaPlan.service_fee_rate],
            p.verification_rate: [cre.MediaPlan.verification_rate],
            p.reporting_source: [cre.MediaPlan.reporting_source],
            p.environment: [cre.MediaPlan.device],
            p.size: [cre.MediaPlan.ad_size],
            p.ad_type: [cre.MediaPlan.ad_type],
            p.placement_description: [cre.MediaPlan.placement_description],
            p.package_description: [cre.MediaPlan.package_description],
            p.media_channel: [cre.MediaPlan.creative_description],
            p.total_budget: [vmc.cost, dctc.PNC]}
        cols = p.get_cols_for_db()
        omit_cols = [p.budget, p.buy_rate, p.start_date, p.ad_rate,
                     p.reporting_rate, PlanPhase.__table__, p.service_fee_rate,
                     p.verification_rate, p.reporting_source, p.total_budget]
        omit_cols = [x.name for x in omit_cols]
        use_cols = [x for x in data_dict if x.name not in omit_cols]
        for idx, col in enumerate(use_cols):
            col_to_add = cols[idx]
            data_dict[col].append(col_to_add)
        return data_dict

    @staticmethod
    def translate_plan_names(df):
        col_dict = PartnerPlacements.get_plan_col_translation()
        col_translation_dict = {}
        df_cols = {}
        for x in df.columns:
            df_cols[x] = x
            df_cols[x.replace(' ', '').replace('\n', '')] = x
        for k, v in col_dict.items():
            if k.name in df.columns:
                continue
            for v_col in v:
                v_cols = [v_col, '{}\n(If Needed) '.format(v_col)]
                v_cols += [x.replace(' ', '').replace('\n', '') for x in v_cols]
                pot_cols = [x for x in v_cols if x in df_cols]
                if pot_cols:
                    pot_cols = df_cols[pot_cols[0]]
                    col_translation_dict[pot_cols] = k.name
                    break
        if col_translation_dict:
            df = df.rename(columns=col_translation_dict)
        return df

    @staticmethod
    def get_col_order(for_loop=False, as_string=True):
        p = PartnerPlacements
        cols = [
            p.budget, Partner.__table__, p.country, p.targeting_bucket,
            p.creative_line_item, p.copy, p.retailer, p.buy_model, p.buy_rate,
            p.start_date, p.serving, p.ad_rate, p.reporting_rate, p.kpi,
            p.data_type_1, PlanPhase.__table__, p.service_fee_rate,
            p.verification_rate, p.reporting_source, p.environment, p.size,
            p.ad_type, p.placement_description, p.package_description,
            p.media_channel]
        if for_loop:
            front_cols = [p.country.name, p.environment.name]
            del_cols = [Partner.__table__.name, PlanPhase.__table__.name,
                        p.start_date, p.end_date]
            for idx, x in enumerate(cols):
                for fc in front_cols:
                    if x.name == fc:
                        cols.insert(0, cols.pop(idx))
                for dc in del_cols:
                    if x.name == dc:
                        cols.pop(idx)
        if as_string:
            cols = [x.name for x in cols]
        else:
            other_names = {p.targeting_bucket.name: ['audience']}
            omit_names = {p.environment.name: ['a']}
            for idx, col in enumerate(cols):
                name_types = [('other_names', other_names),
                              ('omit_names', omit_names)]
                for name_type in name_types:
                    val = []
                    if col.name in name_type[1].keys():
                        val = name_type[1][col.name]
                    cols[idx].__dict__[name_type[0]] = val
        return cols

    @staticmethod
    def get_reporting_db_df(vendor_names=None):
        cols = PartnerPlacements.get_cols_for_db()
        total_db = pd.DataFrame()
        base_plan = Plan.query.filter_by(name='Base Plan').first()
        if base_plan:
            total_db = base_plan.get_placements_as_df()
            total_db[vmc.impressions] = 100
        else:
            for col in cols:
                total_db[col] = ['None']
        return total_db

    @staticmethod
    def get_col_names(col):
        str_name = col.name
        col_names = str_name.split('_')
        db_col = '{}name'.format(''.join(col_names))
        col_names = col_names + [str_name, db_col] + col.__dict__['other_names']
        col_names = [x for x in col_names if not x.isdigit()]
        col_names += ['{}s'.format(x) for x in col_names]
        return str_name, db_col, col_names

    @staticmethod
    def find_name_from_message(message, name):
        message = message.split(' ')
        if '/' in name:
            name = name.replace(' / ', '/')
        for idx, m in enumerate(message):
            next_m = None
            if not (idx + 1) >= len(message):
                next_m = message[idx + 1]
            for delim in [',', '.']:
                m = m.replace(delim, '')
                if next_m:
                    next_m = next_m.replace(delim, '')
            if m.lower() == name.lower():
                name = m
                break
            elif next_m:
                if m.lower() + next_m.lower() == name:
                    name = '{} {}'.format(m, next_m)
                    break
        return name

    @staticmethod
    def get_default_value_for_col(parent, total_db, db_col, str_name, plan_id):
        new_rule = None
        if total_db.empty:
            return new_rule
        ven_col = prc_model.Vendor.vendorname.name
        if ven_col not in total_db.columns:
            ven_col = Partner.__table__.name
        filtered_df = total_db[total_db[ven_col] == parent.name]
        exclude_values = [0, 'None', None, '0', '0.0', 0.0]
        cur_col = [x for x in [db_col, str_name] if x in filtered_df.columns]
        if cur_col:
            cur_col = cur_col[0]
            mask = ~filtered_df[cur_col].isin(exclude_values)
            filtered_df = filtered_df[mask]
            if not filtered_df.empty:
                grouped = filtered_df.groupby(cur_col)[vmc.impressions].sum()
                if not grouped.empty:
                    g_max = grouped.idxmax()
                    if '_' in g_max:
                        g_max = g_max.split('_')[0]
                    rule_info = {g_max: 1}
                    new_rule = PlanRule(
                        place_col=str_name, rule_info=rule_info, type='Create',
                        partner_id=parent.id, plan_id=plan_id)
                    db.session.add(new_rule)
                    db.session.commit()
        return new_rule

    def check_single_col_from_words(self, col, parent, plan_id, words,
                                    total_db, min_impressions, new_rules,
                                    message=''):
        str_name, db_col, col_names = self.get_col_names(col)
        old_rule = PlanRule.query.filter_by(
            place_col=str_name, partner_id=parent.id,
            plan_id=plan_id).first()
        if old_rule and old_rule.type != 'Create':
            return new_rules, words
        name_in_list = utl.is_list_in_list(col_names, words, False, True)
        if name_in_list:
            cols = self.get_col_order(for_loop=True, as_string=False)
            cols = [self.get_col_names(x)[2] for x in cols]
            cols = [item for sublist in cols for item in sublist]
            date_search = 'date' in str_name
            name_list = utl.get_next_values_from_list(
                words, col_names, cols, date_search=date_search)
            if date_search:
                name_list = [
                    PartnerPlacements.fix_date_from_words(str_name, x, True)
                    for x in name_list]
            name_list = [{db_col: x} for x in name_list]
        else:
            name_list = Client.get_name_list(db_col, min_impressions)
            if name_list:
                name_list = utl.get_dict_values_from_list(words, name_list,
                                                          True)
        cur_part = db.session.get(Partner, parent.id)
        ps = Partner.query.filter_by(plan_phase_id=cur_part.plan_phase_id).all()
        ps = [x.name.lower() for x in ps] + col.__dict__['omit_names']
        name_list = [x for x in name_list if x[db_col] not in ps]
        if old_rule:
            comp_list = [x[db_col] for x in name_list]
            if isinstance(old_rule.rule_info, str):
                old_rule.rule_info = json.loads(old_rule.rule_info)
            for x in old_rule.rule_info:
                if x not in comp_list:
                    name_list.append({db_col: x})
        if name_list:
            rule_info = {}
            rem_percent = 1
            name_no_number = []
            words_to_remove = [x[db_col].lower() for x in name_list]
            for x in name_list:
                name = x[db_col]
                if message:
                    name = self.find_name_from_message(message, name)
                num = None
                if name.lower() in words:
                    num = utl.get_next_number_from_list(
                        words, name.lower(), '')
                    words_to_remove += [num]
                if num:
                    if num not in words:
                        name_no_number.append(name)
                        continue
                    idx = words.index(num)
                    num_mult = .01
                    if (idx > 0) and (words[idx - 1] == '.') and len(num) == 1:
                        num_mult = .1
                    num = float(num) * num_mult
                    if num > rem_percent:
                        num = rem_percent
                    rem_percent -= num
                    rule_info[name] = num
                    words.pop(idx)
                else:
                    name_no_number.append(name)
            for x in name_no_number:
                rule_info[x] = rem_percent / len(name_no_number)
            total_rule = 0
            for k, v in rule_info.items():
                total_rule += v
            if total_rule != 1:
                for k in rule_info:
                    if total_rule == 0:
                        rule_info[k] = 1 - rule_info[k]
                        total_rule = rule_info[k]
                    rule_info[k] = rule_info[k] * (1 / total_rule)
            if old_rule:
                old_rule.rule_info = rule_info
                db.session.commit()
            else:
                old_rule = PlanRule(place_col=str_name, rule_info=rule_info,
                                    partner_id=parent.id, plan_id=plan_id,
                                    type='Create')
                db.session.add(old_rule)
                db.session.commit()
            new_rules.append(old_rule)
            if name_in_list:
                words_to_remove = words_to_remove + name_in_list
            for word_to_remove in words_to_remove:
                if word_to_remove and word_to_remove in words:
                    words.pop(words.index(word_to_remove.lower()))
        elif not name_list and not old_rule:
            old_rule = self.get_default_value_for_col(
                parent, total_db, db_col, str_name, plan_id)
        if not old_rule:
            old_rule = PlanRule(
                place_col=str_name, rule_info={'': 1},
                partner_id=parent.id, plan_id=plan_id, type='Create')
            db.session.add(old_rule)
            db.session.commit()
        return new_rules, words

    def check_gg_children(self, parent_id, words, total_db, msg_text,
                          message='', running_user_id=None):
        if current_user:
            running_user_id = current_user.id
        parent = db.session.get(Partner, parent_id)
        g_parent = db.session.get(PlanPhase, parent.plan_phase_id)
        gg_parent = db.session.get(Plan, g_parent.plan_id)
        gg_parent.launch_task(
            '.check_plan_gg_children', msg_text,
            running_user=running_user_id, parent_id=parent_id, words=words,
            total_db=total_db, message=message)
        db.session.commit()
        return 'True'

    def check_col_in_words(self, words, parent_id, total_db=pd.DataFrame(),
                           message=''):
        response = ''
        if not words:
            words = []
        parent = db.session.get(Partner, parent_id)
        g_parent = db.session.get(PlanPhase, parent.plan_phase_id)
        plan_id = g_parent.plan_id
        cols = self.get_col_order(for_loop=True, as_string=False)
        min_impressions = 50000000
        new_rules = []
        if total_db.empty:
            total_db = self.get_reporting_db_df()
        parent_names = [x.name.lower() for x in g_parent.get_current_children()]
        words = [x for x in words if x not in parent_names]
        for col in cols:
            new_rules, words = PartnerPlacements().check_single_col_from_words(
                col, parent, plan_id, words, total_db,
                min_impressions, new_rules, message)
        if new_rules:
            response = 'Added rules for {} for columns {}'.format(
                parent.name, ','.join([x.place_col for x in new_rules]))
        return response

    @staticmethod
    def fix_date_from_words(col, val, to_str=False):
        sd_name = PartnerPlacements.start_date.name
        ed_name = PartnerPlacements.end_date.name
        col_name = col
        if hasattr(col, 'name'):
            col_name = col.name
        if col_name in [sd_name, ed_name]:
            if not val:
                val = datetime.today()
            elif isinstance(val, str) or isinstance(val, int):
                if isinstance(val, int):
                    val = str(val)
                for bad_str in ['date', ' ', 'is', 'and']:
                    val = val.replace(bad_str, '')
                if 'end' in val:
                    val = val.split('end')
                    if col_name == sd_name:
                        val = val[0]
                    else:
                        val = val[1]
                val = val.strip()
                val = utl.string_to_date(val)
            if isinstance(val, str) or str(val) == 'NaT':
                val = datetime.today()
            if to_str:
                val = datetime.strftime(val, '%Y%m%d')
        return val

    def create_placement_name(self, place_dict, parent):
        col_order = self.get_col_order()
        placement_name = []
        for col in col_order:
            if col == Partner.__table__.name:
                val = parent.name
            elif col == PlanPhase.__table__.name:
                val = parent.plan.name
            elif col in place_dict:
                val = place_dict[col]
                val = self.fix_date_from_words(col, val, to_str=True)
            else:
                val = ''
            placement_name.append(str(val))
        placement_name = '_'.join(placement_name)
        return placement_name

    @staticmethod
    def get_combos_from_rules(parent_id):
        rules = PlanRule.query.filter_by(partner_id=parent_id,
                                         type='Create').all()
        rule_dict = {
            x.place_col: json.loads(x.rule_info)
            if not isinstance(x.rule_info, dict) else x.rule_info
            for x in rules}
        keys = [list(x.keys()) for x in rule_dict.values()]
        combos = list(itertools.product(*keys))
        return combos, rule_dict

    @staticmethod
    def apply_lookup_rules(parent_id, combos, rule_dict):
        rules = PlanRule.query.filter_by(partner_id=parent_id,
                                         type='Lookup').all()
        df = pd.DataFrame(combos, columns=list(rule_dict.keys()))
        for rule in rules:
            rule_info = rule.rule_info
            if not isinstance(rule_info, dict):
                rule_info = json.loads(rule_info)
            for col_name, rule_details in rule_info.items():
                rule_dict[rule.place_col] = {}
                if isinstance(rule_details, (int, float)):
                    continue
                for old_col_val, new_col_vals in rule_details.items():
                    tdf = df[df[col_name] == old_col_val]
                    df = df[df[col_name] != old_col_val]
                    rule_percent = 1 / len(new_col_vals)
                    for new_col_val in new_col_vals:
                        tdf[rule.place_col] = new_col_val
                        df = pd.concat([df, tdf])
                        rule_dict[rule.place_col][new_col_val] = rule_percent
        combos = df.values.tolist()
        return combos, rule_dict

    @staticmethod
    def sync_partner_budget(data, parent_id):
        cur_partner = db.session.get(Partner, parent_id)
        partner_budget = float(cur_partner.total_budget)
        total = sum(float(x[PartnerPlacements.total_budget.name]) for x in data)
        if total != partner_budget:
            if total:
                ratio = partner_budget / total
            else:
                ratio = partner_budget / len(data)
            for d in data:
                if total:
                    new_budget = float(d[PartnerPlacements.total_budget.name])
                    new_budget *= ratio
                else:
                    new_budget = ratio
                d[PartnerPlacements.total_budget.name] = new_budget
        return data

    @staticmethod
    def apply_manual_rules(parent_id, data):
        current_app.logger.info('Apply rules to {}: {}'.format(parent_id, data))
        rules = PlanRule.query.filter_by(partner_id=parent_id).all()
        delete_names = []
        data_with_id = {}
        data_without_id = []
        for d in data:
            if 'id' in d:
                data_with_id[d['id']] = d
            else:
                data_without_id.append(d)
        for rule in rules:
            if rule.type == 'update':
                cur_id = rule.rule_info['id']
                new_val = rule.rule_info['val']
                if cur_id and cur_id in data_with_id:
                    data_with_id[cur_id][rule.place_col] = new_val
            elif rule.type == 'add':
                for place_dict in rule.rule_info:
                    cur_id = place_dict.get('id')
                    cur_place = db.session.get(PartnerPlacements, cur_id)
                    data_with_id[cur_id] = cur_place.get_form_dict()
            elif rule.type == 'delete':
                delete_names.extend([x['name'] for x in rule.rule_info])
        if delete_names:
            data_with_id = {k: v for k, v in data_with_id.items() if
                            v['name'] not in delete_names}
            data_without_id = [x for x in data_without_id if
                               x['name'] not in delete_names]
        data = list(data_with_id.values()) + data_without_id
        data = PartnerPlacements.sync_partner_budget(data, parent_id)
        return data

    @staticmethod
    def get_values_from_partner(temp_dict, cur_partner):
        p = PartnerPlacements
        cols = [p.cpm, p.cpc, p.cplpv, p.cpbc, p.cpv, p.cpcv]
        for col in cols:
            if col.name not in temp_dict or not temp_dict[col.name]:
                col_name = col.name
                if col_name in [p.cpm.name, p.cpc.name]:
                    pre = Partner.estimated_cpm.name.split('_')[0]
                    col_name = '{}_{}'.format(pre, col_name)
                temp_dict[col.name] = cur_partner.__dict__[col_name]
        return temp_dict

    @staticmethod
    def get_rate_card(parent):
        cur_phase = db.session.get(PlanPhase, parent.plan_phase_id)
        cur_plan = db.session.get(Plan, cur_phase.plan_id)
        rate_card = db.session.get(RateCard, cur_plan.rate_card_id)
        rates = {}
        if rate_card:
            rates = rate_card.rates.all()
            rates = {x.type_name: (x.adserving_fee, x.reporting_fee)
                     for x in rates}
        return rates

    def add_rates_from_card(self, temp_dict, rates=None, parent=None):
        if PartnerPlacements.serving.name in temp_dict:
            if not rates and parent:
                rates = self.get_rate_card(parent)
            serve_type = temp_dict[PartnerPlacements.serving.name]
            if serve_type in rates:
                ad_rate = rates[serve_type][0]
                temp_dict[PartnerPlacements.ad_rate.name] = ad_rate
                rep_rate = rates[serve_type][1]
                temp_dict[PartnerPlacements.reporting_rate.name] = rep_rate
        return temp_dict

    def create_from_rules(self, parent_id, current_user_id):
        parent = db.session.get(Partner, parent_id)
        rates = self.get_rate_card(parent)
        parent_budget = float(parent.total_budget)
        combos, rule_dict = self.get_combos_from_rules(parent_id)
        combos, rule_dict = self.apply_lookup_rules(parent_id, combos,
                                                    rule_dict)
        data = []
        for combo in combos:
            temp_dict = {}
            value_product = 1
            for i, key in enumerate(combo):
                col_name = list(rule_dict.keys())[i]
                try:
                    value = rule_dict[col_name][key]
                except KeyError as e:
                    continue
                temp_dict[col_name] = key
                value_product *= value
            place_budget = float(value_product) * float(parent_budget)
            temp_dict[Plan.total_budget.name] = place_budget
            temp_dict[PartnerPlacements.start_date.name] = parent.start_date
            temp_dict[PartnerPlacements.end_date.name] = parent.end_date
            temp_dict[PartnerPlacements.partner_id.name] = parent.id
            temp_dict = self.get_values_from_partner(temp_dict, parent)
            for col in self.__table__.columns:
                if col.name in temp_dict and temp_dict[col.name]:
                    temp_dict[col.name] = PartnerPlacements.fix_date_from_words(
                        col, temp_dict[col.name])
            temp_dict = PartnerPlacements().add_rates_from_card(
                temp_dict, rates=rates)
            placement_name = PartnerPlacements().create_placement_name(
                temp_dict, parent)
            temp_dict[PartnerPlacements.name.name] = placement_name
            cur_placement = PartnerPlacements.query.filter_by(
                name=placement_name, partner_id=parent_id).first()
            if cur_placement:
                temp_dict[PartnerPlacements.id.name] = cur_placement.id
            data.append(temp_dict)
        data = self.apply_manual_rules(parent_id, data)
        from app.utils import set_db_values
        set_db_values(parent_id, current_user_id, form_sources=data,
                      table=PartnerPlacements, parent_model=Partner)
        return data

    @staticmethod
    def calculate_metrics(fd, cur_phase):
        form_cols, metric_cols, def_metric_cols = Partner.get_metric_cols()
        cost = fd[PartnerPlacements.total_budget.name]
        if not cost:
            cost = 0
        for idx, col in enumerate(metric_cols):
            if col not in fd.keys():
                cost_per = fd[metric_cols[idx - 1]]
                val = 0
                if cost_per:
                    val = float(cost) / float(cost_per)
                if col == vmc.impressions:
                    val *= 1000
                fd[col] = val
        plan_cols = [(Plan.digital_agency_fees, cal.AGENCY_FEES)]
        place_cols = [(PartnerPlacements.ad_rate, vmc.AD_COST),
                      (PartnerPlacements.reporting_rate, vmc.REP_COST)]
        if cur_phase:
            cur_plan = db.session.get(Plan, cur_phase.plan_id)
            for col in plan_cols + place_cols:
                rate_col = col[0].name
                cost_col = col[1]
                cur_dict = cur_plan.__dict__ if col in plan_cols else fd
                val = cur_dict[rate_col]
                if not val:
                    val = 0
                fd[rate_col] = val
                mult_col = cost
                if col in place_cols:
                    is_click = fd[PartnerPlacements.serving.name]
                    is_click = is_click and vmc.clicks[:-1] in is_click
                    if is_click:
                        mult_col = fd[vmc.clicks]
                    else:
                        mult_col = fd[vmc.impressions] / 1000
                try:
                    new_val = float(mult_col) * float(val)
                except ValueError as e:
                    new_val = 0
                fd[cost_col] = new_val
        fd[cal.TOTAL_COST] = float(cost)
        for col in plan_cols + place_cols:
            fd[cal.TOTAL_COST] += float(fd[col[1]])
        return fd

    def get_form_dict(self):
        fd = dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                   if not k.startswith("_")])
        cur_partner = ''
        cur_phase = ''
        if self.partner_id:
            cur_partner = db.session.get(Partner, self.partner_id)
            fd = self.get_values_from_partner(fd, cur_partner)
            if cur_partner:
                cur_phase = db.session.get(PlanPhase, cur_partner.plan_phase_id)
                fd = self.add_rates_from_card(fd, parent=cur_partner)
                fd = self.calculate_metrics(fd, cur_phase)
                cur_phase = cur_phase.name
            cur_partner = cur_partner.name
        fd[Partner.__table__.name] = cur_partner
        fd[PlanPhase.__table__.name] = cur_phase
        return fd

    def set_from_form(self, form, current_object):
        for col in self.__table__.columns:
            if col.name in form and col.name != PartnerPlacements.id.name:
                val = form[col.name]
                if isinstance(col.type, db.Date) and isinstance(val, str):
                    val = utl.string_to_date(val)
                elif isinstance(col.type, db.Numeric) and val == '':
                    val = 0
                setattr(self, col.name, val)
        if current_object:
            self.partner_id = current_object.id

    @staticmethod
    def get_current_children():
        return []

    @staticmethod
    def set_from_another(cur_rfp):
        p = PartnerPlacements
        placement_dict = {
            p.budget.name: '',
            p.start_date.name: cur_rfp.start_date,
            p.end_date.name: cur_rfp.end_date,
            p.country.name: cur_rfp.country,
            p.targeting_bucket.name: cur_rfp.targeting,
            p.creative_line_item.name: '',
            p.copy.name: '',
            p.retailer.name: '',
            p.buy_model.name: cur_rfp.buy_model,
            p.buy_rate.name: cur_rfp.cpm_cost_per_unit,
            p.serving.name: cur_rfp.ad_serving_type,
            p.ad_rate.name: '',
            p.reporting_rate.name: '',
            p.kpi.name: cur_rfp.kpi,
            p.data_type_1.name: '',
            p.service_fee_rate.name: '',
            p.verification_rate.name: '',
            p.reporting_source.name: cur_rfp.reporting_source,
            p.environment.name: cur_rfp.device,
            p.size.name: cur_rfp.ad_size_wxh,
            p.ad_type.name: cur_rfp.ad_type,
            p.placement_description.name: cur_rfp.placement_name_description,
            p.package_description.name: cur_rfp.package_name_description,
            p.media_channel.name: '',
            p.total_budget.name: cur_rfp.planned_net_cost,
            p.partner_id.name: cur_rfp.partner_id
        }
        return placement_dict


class PlanRule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, index=True)
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))
    partner_id = db.Column(db.Integer, db.ForeignKey('partner.id'))
    place_col = db.Column(db.Text)
    order = db.Column(db.Integer)
    type = db.Column(db.String(128))
    rule_info = db.Column(db.JSON)

    def get_form_dict(self):
        fd = dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                   if not k.startswith("_")])
        cur_partner = 'ALL'
        part_cost = 0
        if self.partner_id:
            cur_partner = db.session.get(Partner, self.partner_id)
            part_cost = cur_partner.total_budget
            cur_partner = cur_partner.name
        fd[Partner.__name__] = cur_partner
        fd[Partner.total_budget.name] = part_cost
        fd.pop(PlanRule.order.name)
        return fd

    def set_from_form(self, form, current_object):
        if PlanRule.id.name in form:
            self.id = int(form[PlanRule.id.name].strip())
        self.name = form[PlanRule.name.name].strip()
        self.plan_id = int(form[PlanRule.plan_id.name].strip())
        self.partner_id = int(form[PlanRule.partner_id.name].strip())
        self.place_col = form[PlanRule.place_col.name].strip()
        self.type = form[PlanRule.type.name].strip()
        self.rule_info = form[PlanRule.rule_info.name]
        if not isinstance(self.rule_info, dict):
            self.rule_info = self.rule_info.strip()

    @staticmethod
    def get_current_children():
        return []


class PlanEffectiveness(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    factor_name = db.Column(db.Text)
    selected_val = db.Column(db.Float)
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))
    brand = "Brand Factors"
    msg = "Messaging Factors"
    media = "Media Factors"
    factors_low = "Lower Frequency"
    factors_high = "Increase Frequency"
    brand_low = [
        "Established IP", "High Category Recognition",
        "High Genre Opportunity", "Strong Community Sentiment",
        "High Game Score", "Free-to-Play", "Increasing MAUs",
        "High Marketplace SOV", "Existing / Returning Players"]
    brand_high = [
        "New IP", "Low Category Recognition",
        "Low Genre Opportunity", "Weak Community Sentiment",
        "Low Game Score", "Full Retail",
        "Decreasing MAUs", "Low Marketplace SOV",
        "New / Competitive Players"]
    msg_low = [
        "Low Complexity", "High Message Uniqueness",
        "Evergreen Campaign", "Gameplay Asset",
        "Few Message Variants (1-2)", "Fatigued Asset",
        "Large Format Assets"]
    msg_high = [
        "High Complexity", "Low Message Uniqueness",
        "Campaign Launch", "Key Art",
        "Several Message Variants (3+)",
        "New Asset", "Small Format Assets"]
    media_low = [
        "Low Environment Clutter", "Strong Contextual Alignment",
        "High Audience Attention", "Low Ad Blocking",
        "Long Flight / Evergreen", "Low Media Fragmentation",
        "Low Competitive Environment"]
    media_high = [
        "High Environment Clutter", "Low Contextual Alignment",
        "Low Audience Attention", "High Ad Blocking",
        "Burst / Launch Flighting", "High Media Fragmentation",
        "High Competitive Environment"]

    def set_from_form(self, form, current_object):
        selected_val = form['cell_pick_col'].strip()
        self.factor_name = form[PlanEffectiveness.factors_low].strip()
        self.plan_id = current_object.id
        self.selected_val = float(selected_val) if selected_val else 0


class Checklist(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'), index=True)
    name = db.Column(db.Text)
    completed_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_by = db.Column(db.Integer, db.ForeignKey('user.id'), index=True)
    complete_msg = db.Column(db.Text)
    checked_at = db.Column(db.DateTime, default=datetime.utcnow)
    checked_by = db.Column(db.Integer, db.ForeignKey('user.id'), index=True)
    checked_msg = db.Column(db.Text)


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
                     if not k.startswith("_")])


class Brandtracker(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    primary_date = db.Column(db.Date)
    comparison_date = db.Column(db.Date)
    titles = db.Column(db.Text)
    dimensions = db.relationship('BrandtrackerDimensions',
                                 backref="brandtracker", lazy='dynamic')

    def to_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_")])

    def get_dimension_form_dicts(self):
        form_dicts = []
        for comp in self.dimensions:
            form_dicts.append(comp.get_form_dict())
        return form_dicts

    @staticmethod
    def get_calculated_fields(c_str='_comparison'):
        calculated_cols = {
            'MAU Growth':
                lambda x: (x[vmc.month_avg_user]
                           - x['{}{}'.format(vmc.month_avg_user, c_str)]),
            'Twitch Concurrent Viewers Trend':
                lambda x: ((x[vmc.twitch_viewers]
                           - x['{}{}'.format(vmc.twitch_viewers, c_str)])
                           / x[vmc.twitch_viewers])
        }
        return calculated_cols


class BrandtrackerDimensions(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    dimension = db.Column(db.Text)
    brandtracker_id = db.Column(db.Integer, db.ForeignKey('brandtracker.id'))
    metric_column = db.Column(db.Text)
    weight = db.Column(db.Numeric)

    def get_form_dict(self):
        return dict([(k, getattr(self, k)) for k in self.__dict__.keys()
                     if not k.startswith("_") and k != 'id'])

    def set_from_form(self, form, current_brandtracker):
        self.dimension = form['dimension_name']
        self.brandtracker_id = current_brandtracker.id
        self.metric_column = form['data_column']
        self.weight = form['weight']


class RequestLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    method = db.Column(db.String(10))
    url = db.Column(db.Text)
    duration = db.Column(db.Float)
    status_code = db.Column(db.Integer)
    form_data = db.Column(db.JSON)
    processor_id = db.Column(db.Integer, db.ForeignKey('processor.id'))
    uploader_id = db.Column(db.Integer, db.ForeignKey('uploader.id'))
    plan_id = db.Column(db.Integer, db.ForeignKey('plan.id'))
