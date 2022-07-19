import io
import os
import json
from app import db
import datetime as dt
from app.models import Task
from flask import current_app


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


def parse_upload_file_request(current_request):
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
