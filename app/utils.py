import io
import os
import json
import re
from app import db
import pandas as pd
import datetime as dt
from app.models import Task, Processor, User, Campaign, Project, Client, Product
from flask import current_app, render_template
from flask_babel import _
import uploader.upload.creator as cre


def launch_task(cur_class, name, description, running_user, task_class_args,
                *args, **kwargs):
    rq_job = current_app.task_queue.enqueue('app.tasks' + name,
                                            cur_class.id, running_user,
                                            *args, **kwargs)
    task = Task(id=rq_job.get_id(), name=name, description=description,
                user_id=cur_class.user_id, **task_class_args)
    db.session.add(task)
    return task


def get_file_in_memory_from_request(current_request, current_key):
    file = current_request.files[current_key]
    file_name = file.filename
    file_type = os.path.splitext(file_name)[1]
    mem = io.BytesIO()
    mem.write(file.read())
    mem.seek(0)
    return mem, file_name, file_type


def parse_upload_file_request(current_request, object_name=None):
    msg = 'Attempting to parse object {} with request: {}'.format(
            object_name, current_request)
    current_app.logger.info(msg)
    current_form = current_request.form.to_dict()
    current_key = list(current_form.keys())[0]
    current_form = json.loads(current_form[current_key])
    object_name = current_form['object_name']
    object_form = current_form['object_form']
    object_level = current_form['object_level']
    return current_key, object_name, object_form, object_level


def group_sql_to_dict(original_query_list, group_by='user_id'):
    from collections import defaultdict
    groups = defaultdict(list)
    for obj in original_query_list:
        groups[obj.__dict__[group_by]].append(obj)
    return groups


def get_col_from_serialize_dict(data, col_name):
    col_keys = [k for k, v in data.items() if v == col_name and 'name' in k]
    col_val_keys = [x.replace('name', 'value') for x in col_keys]
    col_vals = [v for k, v in data.items() if k in col_val_keys]
    return col_vals


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


def get_sd_ed_in_dict(dict_to_add, sd_ed_value):
    date_list = sd_ed_value.split(' to ')
    sd = date_list[0]
    ed = date_list[1]
    dict_to_add['start_date'] = dt.datetime.strptime(sd, '%Y-%m-%d')
    dict_to_add['end_date'] = dt.datetime.strptime(ed, '%Y-%m-%d')
    return dict_to_add


def sync_new_form_data_with_database(form_dict, old_db_items, db_model,
                                     relation_db_item, form_search_name='name'):
    if old_db_items:
        for p in old_db_items:
            new_p = [x for x in form_dict if p.name == x[form_search_name]]
            if new_p:
                new_p = new_p[0]
                p.set_from_form(form=new_p, current_plan=relation_db_item)
                db.session.commit()
                form_dict = [x for x in form_dict
                             if p.name != x[form_search_name]]
            else:
                db.session.delete(p)
    for p in form_dict:
        new_p = db_model()
        new_p.set_from_form(form=p, current_plan=relation_db_item)
        db.session.add(new_p)
        db.session.commit()


def convert_file_to_df(current_file):
    df = pd.read_csv(current_file)
    return df


def parse_filter_dict_from_clients(processors, seven_days_ago, current_request):
    current_filters = json.loads(current_request.form['filter_dict'])
    filter_types = [
        ('username', User, 'username', Processor.user_id),
        ('campaign', Campaign, 'name', Processor.campaign_id),
        ('processor', Processor, 'name', Processor.id),
        ('project', Project, 'project_number', Processor.projects),
        ('client', Client, 'name', ''),
        ('product', Product, 'name', '')]
    live = [x for x in current_filters if 'live' in x.keys()]
    if live and live[0]['live']:
        processors = processors.filter(
            Processor.end_date > seven_days_ago.date())
    for filter_type in filter_types:
        filt_name = filter_type[0]
        db_model = filter_type[1]
        db_attr = filter_type[2]
        proc_rel = filter_type[3]
        cur_filter = [x for x in current_filters if filt_name in x.keys()]
        if cur_filter and cur_filter[0][filt_name] and processors:
            cur_list = cur_filter[0][filt_name]
            if filt_name == 'project':
                cur_list = [x.split('_')[0] for x in cur_list]
            user_list = []
            for x in cur_list:
                query = db_model.query.filter(getattr(db_model, db_attr) == x)
                if query and query.first():
                    user_list.append(query.first().id)
            if filt_name == 'client':
                processors = [
                    x for x in processors if
                    x.campaign.product.client_id in user_list]
            elif filt_name == 'product':
                processors = [
                    x for x in processors if
                    x.campaign.product_id in user_list]
            elif filt_name == 'project':
                processors = [x for x in processors
                              if any(e in [y.id for y in x.projects]
                                     for e in user_list)]
            else:
                processors = processors.filter(proc_rel.in_(user_list))
    return processors


def get_processor_user_map(processors):
    new_list = group_sql_to_dict(processors, group_by='user_id')
    new_list = list(new_list.values())
    new_list.sort(key=len, reverse=True)
    for u in new_list:
        cu = u[0].user
        """
        cu.ppd = '{0:.0f}'.format(cu.posts.filter(
            Post.timestamp > seven_days_ago.date()).count() / 7)
        """
        if cu.id in [3, 5, 7, 9, 10, 11, 51, 63, 66, 76, 88, 93]:
            cu.data = True
        else:
            cu.data = False
        for p in u:
            """
            p.ppd = '{0:.0f}'.format(p.posts.filter(
                Post.timestamp > seven_days_ago.date()).count() / 7)
            """
            cu.live = 0
            cu.upcoming = 0
            cu.completed = 0
            if p.start_date and p.start_date > dt.datetime.today().date():
                p.status = 'Upcoming'
                cu.upcoming += 1
            elif p.end_date and p.end_date < dt.datetime.today().date():
                p.status = 'Completed'
                cu.completed += 1
            elif not p.end_date or not p.start_date:
                cu.status = 'Missing start/end date'
            else:
                cu.live += 1
                p.status = 'Live'
    current_users = User.query.order_by(User.username).all()
    projects = Project.query.order_by(Project.project_name).all()
    processor_html = render_template('processor_user_map.html',
                                     processors=new_list,
                                     current_users=current_users,
                                     project_numbers=projects)
    return processor_html


def get_processor_client_directory(processors):
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
    return clients_html


def convert_media_plan_to_df(current_file):
    df = pd.DataFrame()
    try:
        mp = cre.MediaPlan(current_file)
        df = mp.df
    except KeyError as e:
        msg = 'Try again with correct columns file was missing: {}'.format(e)
        current_app.logger.warning(msg)
    return df


def check_and_add_media_plan(media_plan_data, processor_to_edit,
                             object_type=Processor, current_user=None):
    if media_plan_data:
        if not current_user:
            current_user = User.query.get(processor_to_edit.user_id)
        df = convert_media_plan_to_df(media_plan_data)
        msg_text = ('Attempting to save media plan for processor: {}'
                    ''.format(processor_to_edit.name))
        processor_to_edit.launch_task(
            '.save_media_plan', _(msg_text),
            running_user=current_user.id,
            media_plan=df, object_type=object_type)
        db.session.commit()


def remove_special_characters(string):
    result = re.sub(r'[^a-zA-Z0-9 ]', '', string)
    result = re.sub(r'\s', '', result)
    return result
