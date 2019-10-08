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
from app.models import User, Post, Task, Processor, Message, \
    ProcessorDatasources

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


def get_processor_sources(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        old_imports = ProcessorDatasources.query.filter_by(
            processor_id=cur_processor.id).all()
        user_that_ran = User.query.get(current_user_id)
        _set_task_progress(0)
        if old_imports:
            for imp in old_imports:
                db.session.delete(imp)
            db.session.commit()
        import processor.reporting.vendormatrix as vm
        processor_path = adjust_path(cur_processor.local_path)
        os.chdir(processor_path)
        matrix = vm.VendorMatrix()
        data_sources = matrix.get_all_data_sources()
        for source in data_sources:
            proc_import = ProcessorDatasources()
            proc_import.set_from_processor(source, cur_processor)
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
        old_imports = ProcessorDatasources.query.filter_by(
            processor_id=cur_processor.id).all()
        user_that_ran = User.query.get(current_user_id)
        _set_task_progress(0)
        proc_imports = []
        for imp in form_imports:
            proc_import = ProcessorDatasources()
            proc_import.set_from_form(imp, cur_processor)
            proc_imports.append(proc_import)
        for imp in old_imports:
            if imp not in proc_imports:
                db.session.delete(imp)
        for imp in proc_imports:
            if imp not in old_imports:
                db.session.add(imp)
        db.session.commit()
        processor_dicts = [x.get_import_processor_dict() for x in proc_imports]
        processor_path = adjust_path(cur_processor.local_path)
        from processor.reporting.vendormatrix import ImportConfig
        os.chdir(processor_path)
        ic = ImportConfig()
        ic.add_and_remove_from_vm(processor_dicts, matrix=True)
        msg_text = "Processor imports set."
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
        db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def set_data_sources(processor_id, current_user_id, form_sources):
    try:
        cur_processor = Processor.query.get(processor_id)
        old_sources = ProcessorDatasources.query.filter_by(
            processor_id=cur_processor.id).all()
        user_that_ran = User.query.get(current_user_id)
        import processor.reporting.vmcolumns as vmc
        _set_task_progress(0)
        for source in form_sources:
            ds = [x for x in old_sources if 'original_vendor_key' in source and
                  x.vendor_key == source['original_vendor_key']][0]
            if ds:
                ds.set_from_form(source, cur_processor)
                db.session.commit()
        sources = ProcessorDatasources.query.filter_by(
            processor_id=cur_processor.id).all()
        sources = [x.get_datasource_for_processor() for x in sources]
        for idx, source in enumerate(sources):
            sources[idx]['original_vendor_key'] = [
                x for x in form_sources
                if x['vendor_key'] == source[vmc.vendorkey]
            ][0]['original_vendor_key']
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        matrix.set_data_sources(sources)
        msg_text = "Processor datasources set."
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def get_data_tables(processor_id, current_user_id):
    cur_processor = Processor.query.get(processor_id)
    file_name = os.path.join(adjust_path(cur_processor.local_path),
                             'Raw Data Output.csv')
    df = pd.read_csv(file_name)
    metrics = ['Impressions', 'Clicks', 'Net Cost', 'Planned Net Cost',
               'Net Cost Final']
    tables = [
        df.groupby(['mpCampaign', 'mpVendor', 'Vendor Key'])[metrics].sum(),
        df.groupby(['mpCampaign', 'mpVendor', 'mpCreative'])[metrics].sum()]
    _set_task_progress(100)
    return tables


def get_dict_order(processor_id, current_user_id, vk):
    cur_processor = Processor.query.get(processor_id)
    import processor.reporting.vendormatrix as vm
    os.chdir(adjust_path(cur_processor.local_path))
    matrix = vm.VendorMatrix()
    data_source = matrix.get_data_source(vk)
    tables = [data_source.get_dict_order_df().head().T]
    _set_task_progress(100)
    return tables


def delete_dict(processor_id, current_user_id, vk):
    cur_processor = Processor.query.get(processor_id)
    import processor.reporting.utils as utl
    import processor.reporting.vmcolumns as vmc
    import processor.reporting.vendormatrix as vm
    os.chdir(adjust_path(cur_processor.local_path))
    matrix = vm.VendorMatrix()
    data_source = matrix.get_data_source(vk)
    try:
        os.remove(os.path.join(utl.dict_path, data_source.p[vmc.filenamedict]))
    except FileNotFoundError as e:
        app.logger.warning('File not found error: {}'.format(e))
    matrix = vm.VendorMatrix()
    data_source = matrix.get_data_source(vk)
    tables = [data_source.get_dict_order_df().head()]
    _set_task_progress(100)
    return tables

def get_translation_dict(processor_id, current_user_id):
    cur_processor = Processor.query.get(processor_id)
    import processor.reporting.dictionary as dct
    import processor.reporting.dictcolumns as dctc
    os.chdir(adjust_path(cur_processor.local_path))
    tc = dct.DictTranslationConfig()
    tc.read(dctc.filename_tran_config)
    _set_task_progress(100)
    return [tc.df]
