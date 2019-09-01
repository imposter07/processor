import os
import sys
import json
import time
import shutil
import pandas as pd
from flask import render_template
from rq import get_current_job
from app import create_app, db
from app.email import send_email
from app.models import User, Post, Task, Processor, Message, ProcessorImports

app = create_app()
app.app_context().push()


def _set_task_progress(progress):
    job = get_current_job()
    if job:
        job.meta['progress'] = progress
        job.save_meta()
        task = Task.query.get(job.get_id())
        task.user.add_notification('task_progress', {'task_id': job.get_id(),
                                                     'progress': progress})
        if progress >= 100:
            task.complete = True
        db.session.commit()


def export_posts(user_id):
    try:
        user = User.query.get(user_id)
        _set_task_progress(0)
        data = []
        i = 0
        total_posts = user.posts.count()
        for post in user.posts.order_by(Post.timestamp.asc()):
            data.append({'body': post.body,
                         'timestamp': post.timestamp.isoformat() + 'Z'})
            time.sleep(5)
            i += 1
            _set_task_progress(100 * i // total_posts)
        send_email('[LQApp] Your blog posts',
                   sender=app.config['ADMINS'][0], recipients=[user.email],
                   text_body=render_template('email/export_posts.txt',
                                             user=user),
                   html_body=render_template('email/export_posts.html',
                                             user=user),
                   attachments=[('posts.json', 'application/json',
                                 json.dumps({'posts': data}, indent=4))],
                   sync=True)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def adjust_path(path):
    for x in [['S:', '/mnt/s'], ['C:', '/mnt/c'], ['c:', '/mnt/c'],
              ['\\', '/']]:
        path = path.replace(x[0], x[1])
    return path


def processor_post_message(proc, usr, text):
    msg = Message(author=usr, recipient=usr, body=text)
    db.session.add(msg)
    usr.add_notification('unread_message_count', usr.new_messages())
    post = Post(body=text, author=usr, processor_id=proc.id)
    db.session.add(post)
    db.session.commit()


def run_processor(processor_id, current_user_id, processor_args):
    try:
        processor_to_run = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        _set_task_progress(0)
        file_path = adjust_path(processor_to_run.local_path)
        from processor.main import main
        os.chdir(file_path)
        if processor_args:
            main(processor_args)
        else:
            main()
        msg_text = ("{} finished running.".format(processor_to_run.name))
        processor_post_message(processor_to_run, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def copy_tree_no_overwrite(old_path, new_path):
    for file_name in os.listdir(old_path):
        old_file = os.path.join(old_path, file_name)
        new_file = os.path.join(new_path, file_name)
        if os.path.isfile(old_file):
            if os.path.exists(new_file):
                continue
            else:
                shutil.copy(old_file, new_file)
        elif os.path.isdir(old_file):
            if not os.path.exists(new_file):
                os.mkdir(new_file)
            copy_tree_no_overwrite(old_file, new_file)


def create_processor(processor_id, current_user_id, base_path):
    try:
        new_processor = Processor.query.get(processor_id)
        user_create = User.query.get(current_user_id)
        old_path = adjust_path(base_path)
        new_path = adjust_path(new_processor.local_path)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        copy_tree_no_overwrite(old_path, new_path)
        msg_text = "Processor was created."
        processor_post_message(new_processor, user_create, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def get_processor_imports(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        old_imports = ProcessorImports.query.filter_by(
            processor_id=cur_processor.id).all()
        user_that_ran = User.query.get(current_user_id)
        _set_task_progress(0)
        if old_imports:
            for imp in old_imports:
                db.session.delete(imp)
            db.session.commit()
        processor_path = adjust_path(cur_processor.local_path)
        from processor.reporting.vendormatrix import ImportConfig
        os.chdir(processor_path)
        ic = ImportConfig()
        current_imports = ic.get_current_imports(matrix=True)
        for imp in current_imports:
            proc_import = ProcessorImports()
            proc_import.set_from_processor(imp, cur_processor)
            db.session.add(proc_import)
        db.session.commit()
        msg_text = "Processor imports refreshed."
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
        db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def set_processor_imports(processor_id, current_user_id, form_imports):
    try:
        cur_processor = Processor.query.get(processor_id)
        old_imports = ProcessorImports.query.filter_by(
            processor_id=cur_processor.id).all()
        user_that_ran = User.query.get(current_user_id)
        _set_task_progress(0)
        proc_imports = []
        for imp in form_imports:
            proc_import = ProcessorImports()
            proc_import.set_from_form(imp, cur_processor)
            proc_imports.append(proc_import)
        for imp in old_imports:
            if imp not in proc_imports:
                db.session.delete(imp)
        for imp in proc_imports:
            if imp not in old_imports:
                db.session.add(imp)
        db.session.commit()
        processor_dicts = [x.get_processor_dict() for x in proc_imports]
        processor_path = adjust_path(cur_processor.local_path)
        from processor.reporting.vendormatrix import ImportConfig
        os.chdir(processor_path)
        ic = ImportConfig()
        ic.add_and_remove_from_vm(processor_dicts , matrix=True)
        msg_text = "Processor imports set."
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
        db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def refresh_datasources(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        old_imports = ProcessorImports.query.filter_by(
            processor_id=cur_processor.id).all()
        user_that_ran = User.query.get(current_user_id)
        _set_task_progress(0)
        if old_imports:
            for imp in old_imports:
                db.session.delete(imp)
            db.session.commit()
        processor_path = adjust_path(cur_processor.local_path)
        from processor.reporting.vendormatrix import ImportConfig
        os.chdir(processor_path)
        ic = ImportConfig()
        current_imports = ic.get_current_imports(matrix=True)
        for imp in current_imports:
            proc_import = ProcessorImports()
            proc_import.set_from_processor(imp, cur_processor)
            db.session.add(proc_import)
        db.session.commit()
        msg_text = "Processor imports refreshed."
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
        db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def get_data_sources(processor_id, current_user_id, local_path):
    import processor.reporting.vendormatrix as vm
    import processor.reporting.vmcolumns as vmc
    os.chdir(adjust_path(local_path))
    matrix = vm.VendorMatrix()
    data_sources = matrix.get_data_sources()
    data_sources = [{
        'vendor_key': x.key,
        'full_placement_columns': x.p[vmc.fullplacename],
        'placement_columns': x.p[vmc.placement],
        'auto_dictionary_placement': x.p[vmc.autodicplace],
        'auto_dictionary_order': x.p[vmc.autodicord],
        'active_metrics': x.get_active_metrics(),
        'vm_rules': x.vm_rules} for x in data_sources]
    _set_task_progress(100)
    return data_sources


def get_data_tables(processor_id, current_user_id, local_path):
    file_name = os.path.join(adjust_path(local_path), 'Raw Data Output.csv')
    df = pd.read_csv(file_name)
    metrics = ['Impressions', 'Clicks', 'Net Cost', 'Planned Net Cost',
               'Net Cost Final']
    tables = [
        df.groupby(['mpCampaign', 'mpVendor', 'Vendor Key'])[metrics].sum(),
        df.groupby(['mpCampaign', 'mpVendor', 'mpCreative'])[metrics].sum()]
    _set_task_progress(100)
    return tables


def get_dict_order(processor_id, current_user_id, local_path, vk):
    import processor.reporting.vendormatrix as vm
    os.chdir(adjust_path(local_path))
    matrix = vm.VendorMatrix()
    data_source = matrix.get_data_source(vk)
    tables = [data_source.get_dict_order_df().head()]
    _set_task_progress(100)
    return tables
