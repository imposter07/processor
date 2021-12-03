import os
import sys
import json
import time
import copy
import shutil
import itertools
import pandas as pd
import numpy as np
from datetime import datetime
from flask import render_template
from rq import get_current_job
from app import create_app, db
from app.email import send_email
from app.models import User, Post, Task, Processor, Message, \
    ProcessorDatasources, Uploader, Account, RateCard, Rates, Conversion, \
    TaskScheduler, Requests, UploaderObjects, UploaderRelations, \
    ProcessorAnalysis, Project, ProjectNumberMax, Client, Product, Campaign, \
    Tutorial, TutorialStage, Walkthrough, WalkthroughSlide

app = create_app()
app.app_context().push()


def _set_task_progress(progress, attempt=1):
    try:
        job = get_current_job()
        if job:
            job.meta['progress'] = progress
            job.save_meta()
            task = Task.query.get(job.get_id())
            task.user.add_notification(
                'task_progress', {'task_id': job.get_id(),
                                  'progress': progress})
            if progress >= 100:
                task.complete = True
            db.session.commit()
    except:
        attempt += 1
        if attempt > 10:
            app.logger.error('Unhandled exception', exc_info=sys.exc_info())
        else:
            db.session.rollback()
            _set_task_progress(progress, attempt)


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
            i += 1
            _set_task_progress(100 * i // total_posts)
        send_email('[Liquid App] Your blog posts',
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


def get_processor_and_user_from_id(processor_id, current_user_id):
    processor_to_run = Processor.query.get(processor_id)
    user_that_ran = User.query.get(current_user_id)
    return processor_to_run, user_that_ran


def processor_post_message(proc, usr, text, run_complete=False,
                           request_id=False, object_name='Processor'):
    try:
        if len(text) > 139:
            msg_body = text[:139]
        else:
            msg_body = text
        msg = Message(author=usr, recipient=usr, body=msg_body)
        db.session.add(msg)
        usr.add_notification('unread_message_count', usr.new_messages())
        if object_name == 'Uploader':
            post = Post(body=text, author=usr, uploader_id=proc.id)
        else:
            post = Post(body=text, author=usr, processor_id=proc.id)
        if request_id:
            post.request_id = request_id
        db.session.add(post)
        db.session.commit()
        usr.add_notification(
            'task_complete', {'text': text,
                              'timestamp': post.timestamp.isoformat(),
                              'post_id': post.id})
        db.session.commit()
        if run_complete:
            proc.last_run_time = datetime.utcnow()
            db.session.commit()
    except:
        db.session.rollback()
        processor_post_message(proc, usr, text, request_id=request_id,
                               object_name=object_name)


def copy_file(old_file, new_file, attempt=1):
    try:
        shutil.copy(old_file, new_file)
    except PermissionError as e:
        app.logger.warning('Could not copy {}: '
                           '{}'.format(old_file, e))
    except OSError as e:
        attempt += 1
        if attempt > 100:
            app.logger.warning(
                'Exceeded after 100 attempts not copying {} '
                '{}'.format(old_file, e))
        else:
            app.logger.warning('Attempt {}: could not copy {} due to OSError '
                               'retrying in 60s: {}'.format(attempt, old_file,
                                                            e))
            time.sleep(60)
            copy_file(old_file, new_file, attempt=attempt)


def copy_processor_local(file_path, copy_back=False):
    if file_path[:6] == '/mnt/s':
        new_file_path = file_path.replace('/mnt/s', '/mnt/c')
        if copy_back:
            tmp_path = file_path
            file_path = new_file_path
            new_file_path = tmp_path
            from processor.main import OUTPUT_FILE
            for file_name in [OUTPUT_FILE, 'logfile.log']:
                new_file = os.path.join(new_file_path, file_name)
                old_file = os.path.join(file_path, file_name)
                copy_file(old_file, new_file)
        if not os.path.exists(new_file_path):
            os.makedirs(new_file_path)
        import processor.reporting.utils as utl
        for directory in [utl.config_path, utl.raw_path, utl.dict_path]:
            new_path = os.path.join(new_file_path, directory)
            old_path = os.path.join(file_path, directory)
            if (not copy_back and directory == utl.raw_path and
                    os.path.exists(new_path)):
                shutil.rmtree(new_path, ignore_errors=True)
            if not os.path.exists(new_path):
                os.makedirs(new_path)
            copy_tree_no_overwrite(old_path, new_path, overwrite=True)
        return new_file_path
    else:
        return file_path


def processor_failed_email(processor_id, current_user_id, exception_text):
    try:
        user_that_ran = User.query.get(current_user_id)
        cur_processor = Processor.query.get(processor_id)
        from urllib.parse import quote
        recipients = [user_that_ran]
        exception_text = '{} - {}'.format(exception_text[0], exception_text[1])
        title = '[Liquid App] Run Failed - Processor {}'.format(
            cur_processor.name)
        if user_that_ran.id != cur_processor.user.id:
            recipients = [user_that_ran, cur_processor.user]
        for user in recipients:
            send_email(title,
                       sender=app.config['ADMINS'][0],
                       recipients=[user.email],
                       text_body=render_template(
                           'email/processor_run_failed.txt', user=user,
                           processor_name=cur_processor.name,
                           exception_text=exception_text),
                       html_body=render_template(
                           'email/processor_run_failed.html', user=user,
                           processor_name=cur_processor.name,
                           exception_text=exception_text),
                       sync=True)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def run_processor(processor_id, current_user_id, run_args):
    try:
        processor_to_run, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        post_body = ('Running {} for processor: {}...'.format(
            run_args, processor_to_run.name))
        processor_post_message(processor_to_run, user_that_ran, post_body)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        _set_task_progress(0)
        old_file_path = adjust_path(processor_to_run.local_path)
        file_path = copy_processor_local(old_file_path)
        from processor.main import main
        os.chdir(file_path)
        if run_args:
            main(run_args)
        else:
            main()
        copy_processor_local(old_file_path, copy_back=True)
        if 'analyze' in run_args:
            os.chdir(cur_path)
            update_analysis_in_db(processor_id, current_user_id)
            update_automatic_requests(processor_id, current_user_id)
        msg_text = ("{} finished running.".format(processor_to_run.name))
        processor_post_message(proc=processor_to_run, usr=user_that_ran,
                               text=msg_text, run_complete=True)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        processor_to_run = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        msg_text = ("{} run failed.".format(processor_to_run.name))
        processor_post_message(processor_to_run, user_that_ran, msg_text)
        processor_failed_email(processor_id, current_user_id, sys.exc_info())
        return False


def list_files(path, attempt=1):
    try:
        file_list = os.listdir(path)
    except OSError as e:
        attempt += 1
        if attempt > 100:
            app.logger.warning(
                'Exceeded after 100 attempts could not list files '
                '{}'.format(path, e))
            file_list = []
        else:
            app.logger.warning('Attempt {}: could list files due to OSError '
                               'retrying in 60s: {}'.format(attempt, e))
            time.sleep(60)
            file_list = list_files(path, attempt)
    return file_list


def copy_tree_no_overwrite(old_path, new_path, first_run=True, overwrite=False):
    old_files = os.listdir(old_path)
    for idx, file_name in enumerate(old_files):
        if first_run:
            _set_task_progress(int((int(idx) / int(len(old_files))) * 100))
        old_file = os.path.join(old_path, file_name)
        new_file = os.path.join(new_path, file_name)
        if os.path.isfile(old_file):
            if os.path.exists(new_file) and not overwrite:
                continue
            else:
                copy_file(old_file, new_file)
        elif os.path.isdir(old_file):
            if not os.path.exists(new_file):
                os.mkdir(new_file)
            copy_tree_no_overwrite(old_file, new_file, first_run=False,
                                   overwrite=overwrite)


def write_translational_dict(processor_id, current_user_id, new_data):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        tc = dct.DictTranslationConfig()
        df = pd.read_json(new_data)
        df = df.drop('index', axis=1)
        df = df[[dctc.DICT_COL_NAME, dctc.DICT_COL_VALUE, dctc.DICT_COL_NVALUE,
                 dctc.DICT_COL_FNC, dctc.DICT_COL_SEL]]
        df = df.replace('NaN', '')
        tc.write(df, dctc.filename_tran_config)
        msg_text = ('{} processor translational dict was updated.'
                    ''.format(cur_processor.name))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def set_initial_constant_file(cur_processor):
    import processor.reporting.dictionary as dct
    import processor.reporting.dictcolumns as dctc
    os.chdir(adjust_path(cur_processor.local_path))
    dcc = dct.DictConstantConfig(None)
    dcc.read_raw_df(dctc.filename_con_config)
    for col in [(dctc.CLI, cur_processor.campaign.product.client.name),
                (dctc.PRN, cur_processor.campaign.product.name),
                (dctc.AGF, cur_processor.digital_agency_fees)]:
        idx = dcc.df[dcc.df[dctc.DICT_COL_NAME] == col[0]].index[0]
        if ((dcc.df.loc[idx, dctc.DICT_COL_VALUE] == 'None') or
                (col[0] == dctc.AGF and cur_processor.digital_agency_fees)):
            dcc.df.loc[idx, dctc.DICT_COL_VALUE] = col[1]
    dcc.write(dcc.df, dctc.filename_con_config)


def create_processor(processor_id, current_user_id, base_path):
    try:
        new_processor, user_create = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        old_path = adjust_path(base_path)
        new_path = adjust_path(new_processor.local_path)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        copy_tree_no_overwrite(old_path, new_path)
        set_initial_constant_file(new_processor)
        msg_text = "Processor {} was created.".format(new_processor.name)
        processor_post_message(new_processor, user_create, msg_text)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def add_data_sources_from_processor(cur_processor, data_sources, attempt=1):
    proc_sources = []
    for source in data_sources:
        proc_source = ProcessorDatasources()
        proc_source.set_from_processor(source, cur_processor)
        proc_source.get_full_dict()
        proc_sources.append(proc_source)
    for source in proc_sources:
        old_source = ProcessorDatasources.query.filter_by(
            processor_id=source.processor_id,
            vendor_key=source.vendor_key).first()
        if old_source:
            for k, v in source.full_dict.items():
                if hasattr(old_source, k) and getattr(old_source, k) != v:
                    setattr(old_source, k, v)
        else:
            db.session.add(source)
    old_sources = ProcessorDatasources.query.filter_by(
        processor_id=cur_processor.id).all()
    proc_source_vks = [x.vendor_key for x in proc_sources]
    for source in old_sources:
        if source.vendor_key not in proc_source_vks:
            db.session.delete(source)
    try:
        db.session.commit()
    except:
        attempt += 1
        if attempt > 20:
            app.logger.error('Unhandled exception - Processor {} User {}'
                             ''.format(cur_processor.id, cur_processor.user.id),
                             exc_info=sys.exc_info())
        else:
            db.session.rollback()
            add_data_sources_from_processor(cur_processor, data_sources,
                                            attempt)


def get_processor_sources(processor_id, current_user_id):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        _set_task_progress(0)
        import processor.reporting.vendormatrix as vm
        os.chdir('processor')
        default_param_ic = vm.ImportConfig(matrix=True)
        processor_path = adjust_path(cur_processor.local_path)
        os.chdir(processor_path)
        matrix = vm.VendorMatrix()
        data_sources = matrix.get_all_data_sources(
            default_param=default_param_ic)
        add_data_sources_from_processor(cur_processor, data_sources)
        msg_text = "Processor {} sources refreshed.".format(cur_processor.name)
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
        db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def set_processor_imports(processor_id, current_user_id, form_imports,
                          set_in_db=True):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        _set_task_progress(0)
        if set_in_db:
            from app.main.routes import set_processor_imports_in_db
            processor_dicts = set_processor_imports_in_db(
                processor_id, form_imports)
        else:
            processor_dicts = form_imports
        full_processor_dicts = copy.deepcopy(processor_dicts)
        for processor_dict in processor_dicts:
            if 'raw_file' in processor_dict:
                processor_dict.pop('raw_file', None)
        processor_path = adjust_path(cur_processor.local_path)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.vendormatrix as vm
        from processor.reporting.vendormatrix import ImportConfig
        os.chdir('processor')
        default_param_ic = ImportConfig(matrix=True)
        os.chdir(processor_path)
        ic = ImportConfig(
            default_param_ic=default_param_ic, base_path=cur_path)
        ic.add_and_remove_from_vm(processor_dicts, matrix=True)
        matrix = vm.VendorMatrix()
        for processor_dict in full_processor_dicts:
            if 'raw_file' in processor_dict:
                vk = processor_dict[vmc.vendorkey]
                if not vk:
                    vk = 'API_{}'.format(processor_dict['Key'])
                    if processor_dict['name']:
                        vk = '{}_{}'.format(vk, processor_dict['name'])
                data_source = matrix.get_data_source(vk=vk)
                data_source.write(processor_dict['raw_file'])
        msg_text = "Processor {} imports set.".format(cur_processor.name)
        processor_post_message(cur_processor, user_that_ran, msg_text)
        os.chdir(cur_path)
        get_processor_sources(processor_id, current_user_id)
        _set_task_progress(100)
        db.session.commit()
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def set_data_sources(processor_id, current_user_id, form_sources):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        old_sources = ProcessorDatasources.query.filter_by(
            processor_id=cur_processor.id).all()
        import processor.reporting.vmcolumns as vmc
        _set_task_progress(0)
        for source in form_sources:
            ds = [x for x in old_sources if 'original_vendor_key' in source and
                  x.vendor_key == source['original_vendor_key']]
            if ds:
                ds = ds[0]
                ds.set_from_form(source, cur_processor)
                db.session.commit()
        sources = ProcessorDatasources.query.filter_by(
            processor_id=cur_processor.id).all()
        sources = [x.get_datasource_for_processor() for x in sources]
        for idx, source in enumerate(sources):
            form_source = [
                x for x in form_sources
                if x['vendor_key'] == source[vmc.vendorkey]]
            if len(form_source) > 0:
                sources[idx]['original_vendor_key'] = form_source[0][
                    'original_vendor_key']
            else:
                sources[idx][
                    'original_vendor_key'] = sources[idx][vmc.vendorkey]
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        matrix.set_data_sources(sources)
        msg_text = "Processor {} datasources set.".format(cur_processor.name)
        if len(form_sources) == 1:
            name = form_sources[0]['vendor_key']
            msg_text += "  {} was saved.".format(name)
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_file_in_memory(tables):
    import io
    import zipfile
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode='w') as f:
        data = zipfile.ZipInfo('raw.csv')
        data.date_time = time.localtime(time.time())[:6]
        data.compress_type = zipfile.ZIP_DEFLATED
        f.writestr(data, data=tables.to_csv())
    mem.seek(0)
    return mem


def get_data_tables(processor_id, current_user_id, parameter=None,
                    dimensions=None, metrics=None):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.utils as utl
        if not cur_processor.local_path:
            _set_task_progress(100)
            return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]
        file_name = os.path.join(adjust_path(cur_processor.local_path),
                                 'Raw Data Output.csv')
        _set_task_progress(15)
        tables = utl.import_read_csv(file_name)
        _set_task_progress(30)
        if not metrics:
            metrics = ['Impressions', 'Clicks', 'Net Cost', 'Planned Net Cost',
                       'Net Cost Final']
        param_translate = {
            'Vendor': ['mpCampaign', 'mpVendor', 'Vendor Key'],
            'Target': ['mpCampaign', 'mpVendor', 'Vendor Key', 'mpTargeting'],
            'Creative': ['mpCampaign', 'mpVendor', 'Vendor Key', 'mpCreative'],
            'Copy': ['mpCampaign', 'mpVendor', 'Vendor Key', 'mpCopy'],
            'BuyModel': ['mpCampaign', 'mpVendor', 'Vendor Key', 'mpBuy Model',
                         'mpBuy Rate', 'mpPlacement Date'],
            'FullOutput': []
        }
        if parameter:
            parameter = param_translate[parameter]
        elif dimensions:
            parameter = dimensions
        else:
            parameter = []
        if parameter and not tables.empty:
            tables = [tables.groupby(parameter)[metrics].sum()]
        elif parameter and tables.empty:
            tables = [tables]
        else:
            mem = get_file_in_memory(tables)
            tables = [mem]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} Parameter {}'.format(
                processor_id, current_user_id, parameter),
            exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_dict_order(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        tables = [data_source.get_dict_order_df().head().T.reset_index()]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def delete_dict(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.utils as utl
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        try:
            os.remove(os.path.join(utl.dict_path,
                                   data_source.p[vmc.filenamedict]))
        except FileNotFoundError as e:
            app.logger.warning('File not found error: {}'.format(e))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        if vk == 'Plan Net':
            message = ('THE PLANNED NET DICTIONARY WAS DELETED.  '
                       'REPOPULATE MANUALLY OR FROM MEDIA PLAN')
            tables = [pd.DataFrame([{'Result': message}])]
        else:
            tables = [data_source.get_dict_order_df().head()]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_raw_data(processor_id, current_user_id, vk, parameter=None):
    try:
        cur_processor = Processor.query.get(processor_id)
        _set_task_progress(20)
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        _set_task_progress(40)
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        _set_task_progress(60)
        tables = data_source.get_raw_df()
        if parameter:
            tables = get_file_in_memory(tables)
        tables = [tables]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def write_raw_data(processor_id, current_user_id, new_data, vk, mem_file=False,
                   new_name=False, file_type='.csv'):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        import processor.reporting.utils as utl
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.vendormatrix as vm
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        os.chdir('processor')
        default_param_ic = vm.ImportConfig(matrix=True)
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        if not vk:
            new_ds = ProcessorDatasources()
            new_ds.key = 'Rawfile'
            new_ds.name = new_name
            new_ds.processor_id = cur_processor.id
            db.session.add(new_ds)
            db.session.commit()
            proc_dict = [new_ds.get_import_processor_dict()]
            ic = vm.ImportConfig(matrix=True, default_param_ic=default_param_ic)
            vk = ic.add_imports_to_vm(proc_dict)[0]
            matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        current_file_type = os.path.splitext(
            data_source.p[vmc.filename_true])[1]
        if file_type != current_file_type:
            idx = matrix.vm_df[matrix.vm_df[vmc.vendorkey] == vk].index
            new_name = data_source.p[vmc.filename].replace(
                current_file_type, file_type)
            matrix.vm_change(index=idx, col=vmc.filename, new_value=new_name)
            matrix.write()
            matrix = vm.VendorMatrix()
            data_source = matrix.get_data_source(vk)
        utl.dir_check(utl.raw_path)
        if mem_file:
            new_data.seek(0)
            file_name = data_source.p[vmc.filename_true].replace(
                file_type, 'TMP{}'.format(file_type))
            with open(file_name, 'wb') as f:
                shutil.copyfileobj(new_data, f, length=131072)
        else:
            df = pd.read_json(new_data)
            df = df.drop('index', axis=1)
            df = df.replace('NaN', '')
            data_source.write(df)
        msg_text = ('{} processor raw_data: {} was updated.'
                    ''.format(cur_processor.name, vk))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        os.chdir(cur_path)
        get_processor_sources(processor_id, current_user_id)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())


def get_dictionary(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.dictionary as dct
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        dic = dct.Dict(data_source.p[vmc.filenamedict])
        tables = [dic.data_dict]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def write_dictionary(processor_id, current_user_id, new_data, vk):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        import processor.reporting.utils as utl
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.dictionary as dct
        import processor.reporting.vendormatrix as vm
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        dic = dct.Dict(data_source.p[vmc.filenamedict])
        df = pd.read_json(new_data)
        if df.empty:
            try:
                os.remove(os.path.join(utl.dict_path,
                                       data_source.p[vmc.filenamedict]))
            except FileNotFoundError as e:
                app.logger.warning('File not found error: {}'.format(e))
            msg_text = ('{} processor dictionary: {} was deleted.'
                        ''.format(cur_processor.name, vk))
            processor_post_message(cur_processor, user_that_ran, msg_text)
            _set_task_progress(100)
            return True
        if 'index' in df.columns:
            df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        if vk == vm.plan_key:
            add_cols = [x for x in df.columns if x not in dctc.PCOLS]
            df = df[dctc.PCOLS + add_cols]
        else:
            df = df[dctc.COLS]
        dic.write(df)
        msg_text = ('{} processor dictionary: {} was updated.'
                    ''.format(cur_processor.name, vk))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())


def get_translation_dict(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        tc = dct.DictTranslationConfig()
        tc.read(dctc.filename_tran_config)
        _set_task_progress(100)
        return [tc.df]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_vendormatrix(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        _set_task_progress(100)
        return [matrix.vm_df]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def write_vendormatrix(processor_id, current_user_id, new_data):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.vendormatrix as vm
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        df = pd.read_json(new_data)
        if 'index' in df.columns:
            df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        rule_cols = [x for x in df.columns
                     if x not in vmc.vmkeys + [vmc.vendorkey]]
        df = df[[vmc.vendorkey] + vmc.vmkeys + rule_cols]
        matrix.vm_df = df
        matrix.write()
        msg_text = ('{} processor vendormatrix was updated.'
                    ''.format(cur_processor.name))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        os.chdir(cur_path)
        get_processor_sources(processor_id, current_user_id)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_constant_dict(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        dcc = dct.DictConstantConfig(None)
        dcc.read_raw_df(dctc.filename_con_config)
        _set_task_progress(100)
        return [dcc.df]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def write_constant_dict(processor_id, current_user_id, new_data):
    try:
        cur_processor = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        dcc = dct.DictConstantConfig(None)
        df = pd.read_json(new_data)
        if 'index' in df.columns:
            df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        df = df[[dctc.DICT_COL_NAME, dctc.DICT_COL_VALUE,
                 dctc.DICT_COL_DICTNAME]]
        dcc.write(df, dctc.filename_con_config)
        msg_text = ('{} processor constant dict was updated.'
                    ''.format(cur_processor.name))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_relational_config(processor_id, current_user_id, parameter=None):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        rc = dct.RelationalConfig()
        rc.read(dctc.filename_rel_config)
        if not parameter:
            df = rc.df
        else:
            params = rc.get_relation_params(parameter)
            dr = dct.DictRelational(**params)
            dr.read()
            df = dr.df
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} Parameter {}'.format(
                processor_id, current_user_id, parameter),
            exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def write_relational_config(processor_id, current_user_id, new_data,
                            parameter=None):
    try:
        cur_processor = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        rc = dct.RelationalConfig()
        df = pd.read_json(new_data)
        if 'index' in df.columns:
            df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        if not parameter:
            df = df[[dctc.RK, dctc.FN, dctc.KEY, dctc.DEP, dctc.AUTO]]
            rc.write(df, dctc.filename_rel_config)
        else:
            rc.read(dctc.filename_rel_config)
            params = rc.get_relation_params(parameter)
            dr = dct.DictRelational(**params)
            dr.write(df)
        msg_text = ('{} processor relational dict {} was updated.'
                    ''.format(cur_processor.name,
                              parameter if parameter else 'config'))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_import_config_file(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        ic = vm.ImportConfig()
        data_source = matrix.get_data_source(vk=vk)
        f_lib = ic.set_config_file_lib(data_source.params[ic.config_file])
        config_file = ic.load_file(data_source.params[ic.config_file], f_lib)
        if vk.split('_')[1] == 'Adwords':
            df = pd.DataFrame(config_file)
        else:
            df = pd.DataFrame({'values': config_file})
        tables = [df]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def write_import_config_file(processor_id, current_user_id, new_data, vk):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        import processor.reporting.vendormatrix as vm
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        ic = vm.ImportConfig()
        data_source = matrix.get_data_source(vk)
        file_name = data_source.params[ic.config_file]
        f_lib = ic.set_config_file_lib(file_name)
        df = pd.read_json(new_data)
        df = df.set_index('index')
        df.index.name = None
        config_file = df.to_dict()
        if 'values' in config_file:
            config_file = config_file['values']

        new_file = os.path.join(ic.file_path, file_name)
        with open(new_file, 'w') as f:
            f_lib.dump(config_file, f)
        msg_text = ('{} processor config file: {} was updated.'
                    ''.format(cur_processor.name, vk))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        os.chdir(cur_path)
        get_processor_sources(processor_id, current_user_id)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())


def write_tableau_config_file(processor_id, current_user_id):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        file_name = os.path.join('config', 'tabconfig.json')
        if cur_processor.tableau_datasource:
            with open(file_name, 'r') as f:
                tab_config = json.load(f)
            tab_config['datasource'] = cur_processor.tableau_datasource
            with open(file_name, 'w') as f:
                json.dump(tab_config, f)
            msg_text = ('{} processor tableau config file was updated.'
                        ''.format(cur_processor.name))
            processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())


def full_run_processor(processor_id, current_user_id, processor_args=None):
    try:
        _set_task_progress(0)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        if not processor_args or processor_args == 'full':
            processor_args = (
                '--api all --ftp all --dbi all --exp all --tab --analyze')
        run_processor(processor_id, current_user_id, processor_args)
        os.chdir(cur_path)
        send_processor_analysis_email(processor_id, current_user_id)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_logfile(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        with open(os.path.join(adjust_path(cur_processor.local_path),
                               'logfile.log'), 'r') as f:
            log_file = f.read()
        _set_task_progress(100)
        return log_file
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_rate_card(processor_id, current_user_id, vk):
    try:
        _set_task_progress(0)
        if vk == '__None':
            rate_card = None
        else:
            rate_card = RateCard.query.filter_by(id=vk).first()
        rate_list = []
        if not rate_card:
            rate_list.append({x: 'None'
                              for x in Rates.__table__.columns.keys()
                              if 'id' not in x})
        else:
            for row in rate_card.rates:
                rate_list.append(dict((col, getattr(row, col))
                                 for col in row.__table__.columns.keys()
                                      if 'id' not in col))
        df = pd.DataFrame(rate_list)
        df = df[[Rates.type_name.name, Rates.adserving_fee.name,
                 Rates.reporting_fee.name]]
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def write_rate_card(processor_id, current_user_id, new_data, vk):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        rate_card_name = '{}|{}'.format(cur_processor.name,
                                        user_that_ran.username)
        rate_card = RateCard.query.filter_by(name=rate_card_name).first()
        if not rate_card:
            rate_card = RateCard(name=rate_card_name, owner_id=current_user_id)
            db.session.add(rate_card)
            db.session.commit()
            rate_card = RateCard.query.filter_by(name=rate_card_name).first()
        for x in rate_card.rates:
            db.session.delete(x)
        db.session.commit()
        data = json.loads(new_data)
        for x in data:
            rate = Rates(adserving_fee=float(x[Rates.adserving_fee.name]),
                         reporting_fee=float(x[Rates.reporting_fee.name]),
                         type_name=x[Rates.type_name.name],
                         rate_card_id=rate_card.id)
            db.session.add(rate)
        db.session.commit()
        msg_text = ('{} processor rate card was updated.'
                    ''.format(cur_processor.name))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())


def get_logfile_uploader(uploader_id, current_user_id):
    try:
        cur_uploader = Uploader.query.get(uploader_id)
        with open(os.path.join(adjust_path(cur_uploader.local_path),
                               'logfile.log'), 'r') as f:
            log_file = f.read()
        _set_task_progress(100)
        return log_file
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())


def create_uploader(uploader_id, current_user_id, base_path):
    try:
        new_uploader = Uploader.query.get(uploader_id)
        user_create = User.query.get(current_user_id)
        old_path = adjust_path(base_path)
        new_path = adjust_path(new_uploader.local_path)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        copy_tree_no_overwrite(old_path, new_path)
        msg_text = "Uploader was created."
        processor_post_message(new_uploader, user_create, msg_text,
                               object_name='Uploader')
        set_uploader_config_files(uploader_id, current_user_id)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        return False


def get_uploader_and_user_from_id(uploader_id, current_user_id):
    uploader_to_run = Uploader.query.get(uploader_id)
    user_that_ran = User.query.get(current_user_id)
    return uploader_to_run, user_that_ran


def parse_uploader_error_dict(uploader_id, current_user_id, error_dict):
    try:
        if not error_dict:
            return True
        uploader_to_run, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        for key in error_dict:
            if key == 'fb/campaign_upload.xlsx':
                upo = UploaderObjects.query.filter_by(
                    uploader_id=uploader_to_run.id,
                    uploader_type='Facebook',
                    object_level='Campaign').first()
            elif key == 'fb/adset_upload.xlsx':
                upo = UploaderObjects.query.filter_by(
                    uploader_id=uploader_to_run.id,
                    uploader_type='Facebook',
                    object_level='Adset').first()
            elif key == 'fb/ad_upload.xlsx':
                upo = UploaderObjects.query.filter_by(
                    uploader_id=uploader_to_run.id,
                    uploader_type='Facebook',
                    object_level='Ad').first()
            else:
                continue
            for rel_col_name in error_dict[key]:
                relation = UploaderRelations.query.filter_by(
                    uploader_objects_id=upo.id,
                    impacted_column_name=rel_col_name).first()

                relation.unresolved_relations = error_dict[key][rel_col_name]
                db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        return False


def run_uploader(uploader_id, current_user_id, run_args):
    try:
        uploader_to_run, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        post_body = ('Running {} for uploader: {}...'.format(
            run_args, uploader_to_run.name))
        processor_post_message(uploader_to_run, user_that_ran, post_body,
                               object_name='Uploader')
        _set_task_progress(0)
        file_path = adjust_path(uploader_to_run.local_path)
        from uploader.main import main
        os.chdir(file_path)
        error_dict = main(run_args)
        parse_uploader_error_dict(uploader_id, current_user_id, error_dict)
        msg_text = ("{} finished running.".format(uploader_to_run.name))
        processor_post_message(proc=uploader_to_run, usr=user_that_ran,
                               text=msg_text, run_complete=True,
                               object_name='Uploader')
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        uploader_to_run = Uploader.query.get(uploader_id)
        user_that_ran = User.query.get(current_user_id)
        msg_text = ("{} run failed.".format(uploader_to_run.name))
        processor_post_message(uploader_to_run, user_that_ran, msg_text,
                               object_name='Uploader')
        return False


def uploader_file_translation(uploader_file_name, object_level='Campaign',
                              uploader_type='Facebook'):
    base_config_path = 'config'
    base_create_path = os.path.join(base_config_path, 'create')
    if uploader_type == 'Facebook':
        uploader_type_path = 'fb'
    elif uploader_type == 'Adwords':
        uploader_type_path = 'aw'
    elif uploader_type == 'DCM':
        uploader_type_path = 'dcm'
    else:
        uploader_type_path = 'fb'
    base_fb_path = os.path.join(base_config_path, uploader_type_path)
    file_translation = {
        'Creator': os.path.join(base_create_path, 'creator_config.xlsx'),
        'uploader_creative_files': ''}
    base_create_path = os.path.join(base_create_path, uploader_type_path)
    for name in ['Campaign', 'Adset', 'Ad', 'uploader_current_name']:
        prefix = ''
        if uploader_type == 'Adwords':
            prefix = 'aw_'
            if name == 'Adset':
                name = 'Adgroup'
        if name == 'uploader_current_name':
            file_name = '{}{}'.format(prefix, object_level.lower())
        else:
            file_name = '{}{}'.format(prefix, name.lower())
        file_name = '{}_upload.xlsx'.format(file_name)
        file_translation[name] = os.path.join(base_fb_path, file_name)
    for name in ['edit_relation', 'uploader_full_relation']:
        file_name = '{}_relation.xlsx'.format(object_level.lower())
        file_translation[name] = os.path.join(base_create_path, file_name)
    for name in ['name_creator', 'upload_filter', 'match_table']:
        file_name = '{}_{}.xlsx'.format(object_level.lower(), name)
        file_translation[name] = os.path.join(base_create_path, file_name)
    return file_translation[uploader_file_name]


def get_primary_column(object_level, uploader_type='Facebook'):
    if uploader_type == 'Facebook':
        if object_level == 'Campaign':
            col = 'campaign_name'
        elif object_level == 'Adset':
            col = 'adset_name'
        elif object_level == 'Ad':
            col = 'ad_name'
        else:
            col = ''
    elif uploader_type == 'Adwords':
        if object_level == 'Campaign':
            col = 'name'
        elif object_level == 'Adset':
            col = 'name’'
        elif object_level == 'Ad':
            col = 'name'
        else:
            col = ''
    elif uploader_type == 'DCM':
        if object_level == 'Campaign':
            col = 'name'
        elif object_level == 'Adset':
            col = 'name'
        elif object_level == 'Ad':
            col = 'ad_name'
        else:
            col = ''
    else:
        col = ''
    return col


def get_current_uploader_obj_names(uploader_id, current_user_id, cur_path,
                                   file_path, file_name, object_level,
                                   uploader_type='Facebook'):
    col = get_primary_column(object_level=object_level,
                             uploader_type=uploader_type)
    os.chdir(cur_path)
    uploader_create_objects(uploader_id, current_user_id,
                            object_level=object_level,
                            uploader_type=uploader_type)
    os.chdir(file_path)
    ndf = pd.read_excel(file_name)
    df = ndf[col].str.split('_', expand=True)
    df[col] = ndf[col]
    return df


def get_uploader_relation_values_from_position(rel_pos, df, vk, object_level,
                                               uploader_type='Facebook'):
    rel_pos = [int(x) for x in rel_pos]
    col = get_primary_column(object_level, uploader_type=uploader_type)
    df = df.loc[df['impacted_column_name'] == vk]
    cdf = pd.read_excel(uploader_file_translation(object_level,
                                                  uploader_type=uploader_type))
    cdf = cdf[col].str.split('_', expand=True)
    max_split = max(x for x in cdf.columns if isinstance(x, int))
    if max(rel_pos) > max_split:
        df = pd.DataFrame([
            {'Result': 'RELATION VALUE {} IS GREATER THAN THE MAX {}.  '
                       'CHANGE RELATION POSITIONS'.format(
                            max(rel_pos), max_split)}])
        return df
    new_values = list(itertools.product(*[cdf[x].dropna().unique().tolist()
                      for x in rel_pos]))
    new_values = ['|'.join(map(str, x)) for x in new_values]
    new_values = [x for x in new_values
                  if x not in df['column_value'].unique().tolist()]
    cdf = pd.DataFrame(new_values, columns=['column_value'])
    cdf['column_name'] = '|'.join([col for x in rel_pos])
    cdf['impacted_column_name'] = vk
    if vk in ['campaign_name', 'adset_name', 'ad_name']:
        impacted_new_value = cdf['column_value'].str.replace('|', '_')
    else:
        impacted_new_value = ''
    cdf['impacted_column_new_value'] = impacted_new_value
    df = df.append(cdf, ignore_index=True, sort=False)
    df['position'] = '|'.join([str(x) for x in rel_pos])
    return df


def get_uploader_file(uploader_id, current_user_id, parameter=None, vk=None,
                      object_level='Campaign', uploader_type='Facebook'):
    try:
        uploader_to_run, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        _set_task_progress(0)
        upo = UploaderObjects.query.filter_by(
            uploader_id=uploader_to_run.id,
            uploader_type=uploader_type,
            object_level=object_level).first()
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        file_path = adjust_path(uploader_to_run.local_path)
        os.chdir(file_path)
        file_name = uploader_file_translation(
            uploader_file_name=parameter, object_level=object_level,
            uploader_type=uploader_type)
        if parameter in ['uploader_current_name']:
            df = get_current_uploader_obj_names(
                uploader_id, current_user_id, cur_path, file_path, file_name,
                object_level=object_level, uploader_type=uploader_type)
        elif parameter in ['uploader_creative_files']:
            file_names = os.listdir("./creative/")
            df = pd.DataFrame(file_names, columns=['creative_file_names'])
        else:
            df = pd.read_excel(file_name)
        if vk:
            relation = UploaderRelations.query.filter_by(
                uploader_objects_id=upo.id,
                impacted_column_name=vk).first()
            rel_pos = UploaderRelations.convert_string_to_list(
                string_value=relation.position)
            if rel_pos and rel_pos != ['']:
                df = get_uploader_relation_values_from_position(
                    rel_pos=rel_pos, df=df, vk=vk, object_level=object_level,
                    uploader_type=uploader_type)
            else:
                df = pd.DataFrame([
                    {'Result': 'RELATION DOES NOT HAVE A POSITION SET ONE ' 
                               'AND REMOVE CONSTANT'}])
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Uploader {} User {} Parameter: {} VK: {}'
            ''.format(uploader_id, current_user_id, parameter, vk),
            exc_info=sys.exc_info())
        return False


def set_uploader_config_files(uploader_id, current_user_id):
    try:
        import yaml
        import uploader.upload.fbapi as fbapi
        import uploader.upload.awapi as awapi
        import uploader.upload.dcapi as dcapi
        new_uploader = Uploader.query.get(uploader_id)
        config_dicts = [
            {'id_val': new_uploader.fb_account_id,
             'config_file_path': fbapi.config_path,
             'file_name': 'fbconfig.json', 'file_type': json,
             'file_key': 'act_id', 'id_prefix': 'act_'},
            {'id_val': new_uploader.aw_account_id,
             'config_file_path': awapi.config_path,
             'file_name': 'awconfig.yaml', 'file_type': yaml,
             'file_key': 'client_customer_id', 'id_prefix': None,
             'nested_key': 'adwords'},
            {'id_val': new_uploader.dcm_account_id,
             'config_file_path': dcapi.config_path,
             'file_name': 'dcapi.json', 'file_type': json,
             'file_key': 'act_id', 'id_prefix': None}, ]
        for config_dict in config_dicts:
            if config_dict['id_val']:
                set_uploader_config_file(uploader_id, current_user_id,
                                         **config_dict)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        return False


def set_uploader_config_file(uploader_id, current_user_id, id_val=None,
                             config_file_path=None, file_name=None,
                             file_type=None, file_key=None,
                             id_prefix=None, nested_key=None):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        _set_task_progress(0)
        file_path = adjust_path(cur_up.local_path)
        os.chdir(file_path)
        with open(os.path.join(config_file_path, file_name), 'r') as f:
            config_file = file_type.load(f)
        if id_prefix:
            new_account_id_value = id_prefix + id_val
        else:
            new_account_id_value = id_val
        if nested_key:
            config_file[nested_key][file_key] = new_account_id_value
        else:
            config_file[file_key] = new_account_id_value
        with open(os.path.join(config_file_path, file_name), 'w') as f:
            file_type.dump(config_file, f)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        return False


def write_uploader_file(uploader_id, current_user_id, new_data, parameter=None,
                        vk=None, mem_file=False, object_level='Campaign',
                        uploader_type='Facebook'):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        _set_task_progress(0)
        import uploader.upload.utils as utl
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        os.chdir(adjust_path(cur_up.local_path))
        file_name = uploader_file_translation(
            uploader_file_name=parameter, object_level=object_level,
            uploader_type=uploader_type)
        if mem_file:
            new_data.seek(0)
            with open(file_name, 'wb') as f:
                shutil.copyfileobj(new_data, f, length=131072)
        else:
            df = pd.read_json(new_data)
            if 'index' in df.columns:
                df = df.drop('index', axis=1)
            df = df.replace('NaN', '')
            if vk:
                odf = pd.read_excel(file_name)
                odf = odf.loc[odf['impacted_column_name'] != vk]
                df = odf.append(df, ignore_index=True, sort=False)
            utl.write_df(df, file_name)
        msg_text = ('{} uploader {} was updated.'
                    ''.format(file_name, cur_up.name))
        processor_post_message(cur_up, user_that_ran, msg_text,
                               object_name='Uploader')
        os.chdir(cur_path)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())


def set_object_relation_file(uploader_id, current_user_id,
                             object_level='Campaign', uploader_type='Facebook'):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        up_cam = UploaderObjects.query.filter_by(
            uploader_id=cur_up.id, object_level=object_level,
            uploader_type=uploader_type).first()
        up_rel = UploaderRelations.query.filter_by(
            uploader_objects_id=up_cam.id).all()
        import uploader.upload.utils as utl
        os.chdir(adjust_path(cur_up.local_path))
        file_name = uploader_file_translation(
            'uploader_full_relation', object_level=object_level,
            uploader_type=uploader_type)
        df = pd.read_excel(file_name)
        for rel in up_rel:
            if rel.relation_constant:
                df = df.loc[df['impacted_column_name'] !=
                            rel.impacted_column_name]
                ndf = pd.DataFrame(
                    {'impacted_column_name': [rel.impacted_column_name],
                     'impacted_column_new_value': [rel.relation_constant],
                     'position': ['Constant']})
                df = df.append(ndf, ignore_index=True, sort=False)
        utl.write_df(df, file_name)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())


def get_uploader_create_dict(object_level='Campaign', create_type='Media Plan',
                             creator_column=None, file_filter=None,
                             duplication_type=None, uploader_type='Facebook'):
    import uploader.upload.creator as cre
    primary_column = get_primary_column(object_level, uploader_type)
    if uploader_type == 'Facebook':
        upload_create_path = 'fb'
    elif uploader_type == 'Adwords':
        upload_create_path = 'aw'
    elif uploader_type == 'DCM':
        upload_create_path = 'dcm'
    else:
        upload_create_path = 'fb'
    base_create_path = '{}/{}/'.format('create', upload_create_path)
    if object_level == 'Campaign':
        if uploader_type == 'Facebook':
            upload_file = 'fb/campaign_upload.xlsx'
        elif uploader_type == 'Adwords':
            upload_file = 'aw/aw_campaign_upload.xlsx'
        elif uploader_type == 'DCM':
            upload_file = 'dcm/campaign_upload.xlsx'
        else:
            upload_file = 'fb/campaign_upload.xlsx'
        if create_type == 'Media Plan':
            col_file_name = [
                'mediaplan.xlsx',
                '/{}{}'.format(base_create_path, 'campaign_name_creator.xlsx'),
                '/{}{}'.format(base_create_path, 'campaign_relation.xlsx')]
            col_new_file = [
                '{}{}'.format(base_create_path, 'campaign_name_creator.xlsx'),
                upload_file, upload_file]
            col_create_type = ['mediaplan', 'create', 'relation']
            col_column_name = [creator_column, primary_column, '']
            col_overwrite = [True, True, '']
            col_filter = [file_filter, '', '']
        else:
            col_file_name = [
                '/{}{}'.format(base_create_path, 'campaign_name_creator.xlsx'),
                '/{}{}'.format(base_create_path, 'campaign_relation.xlsx')]
            col_new_file = [upload_file, upload_file]
            col_create_type = ['create', 'relation']
            col_column_name = [primary_column, '']
            col_overwrite = [True, '']
            col_filter = ['', '']
    elif object_level == 'Adset':
        if uploader_type == 'Facebook':
            upload_file = 'fb/adset_upload.xlsx'
        elif uploader_type == 'Adwords':
            upload_file = 'aw/aw_adgroup_upload.xlsx'
        elif uploader_type == 'DCM':
            upload_file = 'dcm/placement_upload.xlsx'
        else:
            upload_file = 'fb/adset_upload.xlsx'
        if create_type == 'Media Plan':
            col_file_name = [
                'mediaplan.xlsx',
                '/{}{}'.format(base_create_path, 'adset_name_creator.xlsx'),
                '/{}{}'.format(base_create_path, 'adset_relation.xlsx')]
            col_new_file = [
                '{}{}'.format(base_create_path, 'adset_name_creator.xlsx'),
                upload_file, upload_file]
            col_create_type = ['mediaplan', 'create', 'relation']
            col_column_name = [creator_column, primary_column, '']
            col_overwrite = [True, True, '']
            col_filter = [file_filter, '', '']
        else:
            col_file_name = [
                '/{}{}'.format(base_create_path, 'adset_name_creator.xlsx'),
                '/{}{}'.format(base_create_path, 'adset_relation.xlsx')]
            col_new_file = [upload_file, upload_file]
            col_create_type = ['create', 'relation']
            col_column_name = [primary_column, '']
            col_overwrite = [True, '']
            col_filter = ['', '']
    elif object_level == 'Ad':
        if uploader_type == 'Facebook':
            upload_file = 'fb/ad_upload.xlsx'
            previous_upload_file = '/fb/adset_upload.xlsx'
        elif uploader_type == 'Adwords':
            upload_file = 'aw/aw_ad_upload.xlsx'
            previous_upload_file = '/aw/aw_adgroup_upload.xlsx'
        elif uploader_type == 'DCM':
            upload_file = 'dcm/ad_upload.xlsx'
            previous_upload_file = '/dcm/placement_upload.xlsx'
        else:
            upload_file = 'fb/ad_upload.xlsx'
            previous_upload_file = '/fb/adset_upload.xlsx'
        if create_type == 'Media Plan':
            col_file_name = [
                'mediaplan.xlsx',
                '/{}{}'.format(base_create_path, 'ad_name_creator.xlsx'),
                '/{}{}'.format(base_create_path, 'ad_relation.xlsx')]
            col_new_file = [
                '{}{}'.format(base_create_path, 'ad_name_creator.xlsx'),
                upload_file, upload_file]
            col_create_type = ['mediaplan', 'create', 'relation']
            col_column_name = [creator_column, primary_column, '']
            col_overwrite = [True, True, '']
            col_filter = [file_filter, '', '']
        else:
            campaign_primary_col = get_primary_column('Campaign', uploader_type)
            adset_primary_col = get_primary_column('Adset', uploader_type)
            if duplication_type == 'Custom':
                dup_col_name = '{}::{}|{}::/{}ad_upload_filter.xlsx'.format(
                    primary_column, campaign_primary_col, adset_primary_col,
                    base_create_path)
            else:
                dup_col_name = '{}::{}|{}'.format(
                    primary_column, campaign_primary_col, adset_primary_col)
            col_file_name = [previous_upload_file, previous_upload_file,
                             '/{}ad_relation.xlsx'.format(base_create_path)]
            col_new_file = [upload_file, upload_file, upload_file]
            col_create_type = ['create', 'duplicate', 'relation']
            col_column_name = [primary_column, dup_col_name, '']
            col_overwrite = [True, '', '']
            col_filter = ['', '', '']
            if create_type == 'Match Table':
                col_file_name.insert(
                    0, '/{}ad_match_table.xlsx'.format(base_create_path))
                col_new_file.insert(
                    0,
                    '/{}ad_name_creator.xlsx|'.format(base_create_path),
                    '/{}ad_upload_filter.xlsx|/{}ad_relation.xlsx'.format(
                        base_create_path, base_create_path))
                col_create_type.insert(0, 'match')
                col_column_name.insert(0, '')
                col_overwrite.insert(0, '')
                col_filter.insert(0, '')
    else:
        col_file_name = ['']
        col_new_file = ['']
        col_create_type = ['']
        col_column_name = ['']
        col_overwrite = ['']
        col_filter = ['']
    new_dict = {
        cre.CreatorConfig.col_file_name: col_file_name,
        cre.CreatorConfig.col_new_file: col_new_file,
        cre.CreatorConfig.col_create_type: col_create_type,
        cre.CreatorConfig.col_column_name: col_column_name,
        cre.CreatorConfig.col_overwrite: col_overwrite,
        cre.CreatorConfig.col_filter: col_filter}
    return new_dict


def uploader_create_objects(uploader_id, current_user_id,
                            object_level='Campaign', uploader_type='Facebook'):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        up_obj = UploaderObjects.query.filter_by(
            uploader_id=cur_up.id, object_level=object_level,
            uploader_type=uploader_type).first()
        _set_task_progress(0)
        import uploader.upload.creator as cre
        import uploader.upload.utils as utl
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        creator_col = UploaderObjects.string_to_list(up_obj.media_plan_columns)
        creator_column = '|'.join(creator_col)
        file_filter = 'Partner Name::{}'.format(up_obj.partner_filter)
        new_dict = get_uploader_create_dict(
            object_level=object_level, create_type=up_obj.name_create_type,
            creator_column=creator_column, file_filter=file_filter,
            duplication_type=up_obj.duplication_type,
            uploader_type=uploader_type)
        df = pd.DataFrame(new_dict)
        os.chdir(adjust_path(cur_up.local_path))
        file_name = uploader_file_translation('Creator')
        utl.write_df(df, file_name)
        os.chdir(cur_path)
        set_object_relation_file(uploader_id, current_user_id,
                                 object_level=object_level,
                                 uploader_type=uploader_type)
        os.chdir(cur_path)
        run_uploader(uploader_id, current_user_id, run_args='--create')
        msg_text = ('{} uploader {} creation file was updated.'
                    ''.format(cur_up.name, object_level))
        processor_post_message(cur_up, user_that_ran, msg_text,
                               object_name='Uploader')
        os.chdir(cur_path)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Uploader {} User {} Object Level {}'.format(
                uploader_id, current_user_id, object_level),
            exc_info=sys.exc_info())


def uploader_create_and_upload_objects(uploader_id, current_user_id,
                                       object_level='Campaign',
                                       uploader_type='Facebook'):
    try:
        uploader_create_objects(uploader_id, current_user_id,
                                object_level=object_level,
                                uploader_type=uploader_type)
        if uploader_type == 'Facebook':
            uploader_type_arg = 'fb'
        elif uploader_type == 'Adwords':
            uploader_type_arg = 'aw'
        elif uploader_type == 'DCM':
            uploader_type_arg = 'dcm'
        else:
            uploader_type_arg = 'fb'
        if object_level == 'Campaign':
            run_args = '--api {} --upload c'.format(uploader_type_arg)
        elif object_level == 'Adset':
            run_args = '--api {} --upload as'.format(uploader_type_arg)
        elif object_level == 'Ad':
            run_args = '--api {} --upload ad'.format(uploader_type_arg)
        else:
            run_args = ''
        run_uploader(uploader_id, current_user_id, run_args=run_args)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())


def uploader_save_creative(uploader_id, current_user_id, file, file_name):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        _set_task_progress(0)
        file_path = adjust_path(cur_up.local_path)
        os.chdir(file_path)
        file.seek(0)
        with open(os.path.join('creative', file_name), 'wb') as f:
            shutil.copyfileobj(file, f, length=131072)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())


def get_uploader_creative(uploader_id, current_user_id):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        _set_task_progress(0)
        file_path = adjust_path(cur_up.local_path)
        os.chdir(file_path)
        file_names = os.listdir(".")
        df = pd.DataFrame(file_names, columns=['creative_file_names'])
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())


def set_processor_values(processor_id, current_user_id, form_sources, table):
    cur_processor, user_that_ran = get_processor_and_user_from_id(
        processor_id=processor_id, current_user_id=current_user_id)
    old_items = table.query.filter_by(
        processor_id=cur_processor.id).all()
    _set_task_progress(0)
    if old_items:
        for item in old_items:
            db.session.delete(item)
        db.session.commit()
    for form_source in form_sources:
        t = table()
        t.set_from_form(form_source, cur_processor)
        db.session.add(t)
    db.session.commit()
    msg_text = "Processor {} {} set.".format(cur_processor.name,
                                             table.__name__)
    processor_post_message(cur_processor, user_that_ran, msg_text)


def set_processor_accounts(processor_id, current_user_id, form_sources):
    try:
        set_processor_values(processor_id=processor_id,
                             current_user_id=current_user_id,
                             form_sources=form_sources, table=Account)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def set_processor_conversions(processor_id, current_user_id, form_sources):
    try:
        set_processor_values(processor_id=processor_id,
                             current_user_id=current_user_id,
                             form_sources=form_sources, table=Conversion)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_processor_conversions(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        old_items = Conversion.query.filter_by(
            processor_id=cur_processor.id).all()
        _set_task_progress(0)
        conv_list = []
        if not old_items:
            conv_list.append({x: 'None'
                              for x in Rates.__table__.columns.keys()
                              if 'id' not in x})
        else:
            for row in old_items:
                conv_list.append(dict((col, getattr(row, col))
                                      for col in row.__table__.columns.keys()
                                      if 'id' not in col))
        df = pd.DataFrame(conv_list)
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def write_conversions(processor_id, current_user_id, new_data):
    try:
        form_sources = json.loads(new_data)
        set_processor_values(processor_id=processor_id,
                             current_user_id=current_user_id,
                             form_sources=form_sources, table=Conversion)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def set_conversions(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        for key in set(x.key for x in cur_processor.conversions):
            idx = matrix.vm_df[matrix.vm_df[vmc.vendorkey] == key].index
            if len(idx) == 0:
                continue
            else:
                idx = idx[0]
            for col in set(x.conversion_type
                           for x in cur_processor.conversions):
                conv = [x for x in cur_processor.conversions
                        if x.key == key and x.conversion_type == col]
                if conv:
                    if key == vmc.api_dc_key:
                        total_conv = '|'.join(
                            ['{} : {}: Total Conversions'.format(
                             x.dcm_category, x.conversion_name) for x in conv])
                        matrix.vm_change(idx, col, total_conv)
                        pc_conv = '|'.join(
                            ['{} : {}: Click-through Conversions'.format(
                             x.dcm_category, x.conversion_name) for x in conv])
                        matrix.vm_change(idx, col + vmc.postclick, pc_conv)
                        pi_conv = '|'.join(
                            ['{} : {}: View-through Conversions'.format(
                             x.dcm_category, x.conversion_name) for x in conv])
                        matrix.vm_change(idx, col + vmc.postimp, pi_conv)
                    elif key == vmc.api_szk_key:
                        total_conv = '|'.join(
                            ['{} Total Conversions'.format(
                             x.conversion_name) for x in conv])
                        matrix.vm_change(idx, col, total_conv)
                        pc_conv = '|'.join(
                            ['{} Post Click Conversions'.format(
                             x.conversion_name) for x in conv])
                        matrix.vm_change(idx, col + vmc.postclick, pc_conv)
                        pi_conv = '|'.join(
                            ['{} Post Impression Conversions'.format(
                             x.conversion_name) for x in conv])
                        matrix.vm_change(idx, col + vmc.postimp, pi_conv)
                    else:
                        total_conv = '|'.join(['{}'.format(
                            x.conversion_name) for x in conv])
                        matrix.vm_change(idx, col, total_conv)
        matrix.write()
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def set_processor_fees(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        rate_card = cur_processor.rate_card
        import processor.reporting.dictcolumns as dctc
        import processor.reporting.dictionary as dct
        os.chdir(adjust_path(cur_processor.local_path))
        rate_list = []
        for row in rate_card.rates:
            rate_list.append(dict((col, getattr(row, col))
                                  for col in row.__table__.columns.keys()
                                  if 'id' not in col))
        df = pd.DataFrame(rate_list)
        df = df.rename(columns={'adserving_fee': dctc.AR,
                                'reporting_fee': dctc.RFR,
                                'type_name': dctc.SRV})
        for col in [dctc.RFM, dctc.AM]:
            df[col] = 'CPM'
            df[col] = np.where(df[dctc.SRV].str.contains('Click'), 'CPC', 'CPM')
        df = df[[dctc.SRV, dctc.AM, dctc.AR, dctc.RFM, dctc.RFR]]
        rc = dct.RelationalConfig()
        rc.read(dctc.filename_rel_config)
        params = rc.get_relation_params('Serving')
        dr = dct.DictRelational(**params)
        dr.write(df)
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.vendormatrix as vm
        matrix = vm.VendorMatrix()
        index = matrix.vm_df[matrix.vm_df[vmc.vendorkey] == 'DCM'].index[0]
        matrix.vm_change(index, 'RULE_3_FACTOR', cur_processor.dcm_service_fees)
        matrix.write()
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def set_processor_plan_net(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        from uploader.upload.creator import MediaPlan
        import processor.reporting.vendormatrix as vm
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        if not os.path.exists('mediaplan.csv'):
            return False
        df = pd.read_csv('mediaplan.csv')
        if MediaPlan.placement_phase in df.columns:
            cam_name = MediaPlan.placement_phase
        else:
            cam_name = MediaPlan.campaign_phase
        df = df.groupby([cam_name, MediaPlan.partner_name])[
            dctc.PNC].sum().reset_index()
        df = df.rename(columns={cam_name: dctc.CAM,
                                MediaPlan.partner_name: dctc.VEN})
        df[dctc.FPN] = df[dctc.CAM] + '_' + df[dctc.VEN]
        matrix = vm.VendorMatrix()
        param = matrix.vendor_set('DCM')
        uncapped_partners = param['RULE_1_QUERY'].split('::')[1].split(',')
        df[dctc.UNC] = df[dctc.VEN].isin(uncapped_partners).replace(False, '')
        data_source = matrix.get_data_source(vm.plan_key)
        dic = dct.Dict(data_source.p[vmc.filenamedict])
        dic.write(df)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def send_processor_build_email(
        processor_id, current_user_id, progress,
        title='[Liquid App] New Processor Creation Request!',
        object_type=Processor, recipients=None):
    try:
        progress = ['{}.....{}'.format(k, v)
                    for k, v in progress.items()]
        cur_processor = object_type.query.get(processor_id)
        from urllib.parse import quote
        processor_name = quote(cur_processor.name)
        if not recipients:
            recipients = cur_processor.processor_followers
        for user in recipients:
            send_email(title,
                       sender=app.config['ADMINS'][0],
                       recipients=[user.email],
                       text_body=render_template(
                           'email/processor_request_build.txt', user=user,
                           processor_name=processor_name,
                           progress=progress),
                       html_body=render_template(
                           'email/processor_request_build.html', user=user,
                           processor_name=processor_name,
                           progress=progress),
                       sync=True)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def processor_assignment_email(
        processor_id, current_user_id,
        title='[Liquid App] New Processor Assignment!'):
    try:
        cur_processor = Processor.query.get(processor_id)
        cur_user = User.query.get(current_user_id)
        from urllib.parse import quote
        processor_name = quote(cur_processor.name)
        recipients = [cur_processor.user, cur_user]
        for user in recipients:
            send_email(title,
                       sender=app.config['ADMINS'][0],
                       recipients=[user.email],
                       text_body=render_template(
                           'email/processor_assignment.txt', user=user,
                           processor_name=processor_name,
                           cur_processor=cur_processor,
                           assigner=cur_user, assignee=cur_processor.user),
                       html_body=render_template(
                           'email/processor_assignment.html', user=user,
                           cur_processor=cur_processor,
                           processor_name=processor_name,
                           assigner=cur_user, assignee=cur_processor.user),
                       sync=True)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def send_processor_request_email(processor_id, current_user_id, progress):
    try:
        progress = ['Request #{}.....{}'.format(k, v)
                    for k, v in progress.items()]
        cur_processor = Processor.query.get(processor_id)
        from urllib.parse import quote
        processor_name = quote(cur_processor.name)
        for user in cur_processor.processor_followers:
            send_email('[Liquid App] New Processor Fix Request!',
                       sender=app.config['ADMINS'][0],
                       recipients=[user.email],
                       text_body=render_template(
                           'email/processor_request_fix.txt', user=user,
                           processor_name=processor_name,
                           progress=progress),
                       html_body=render_template(
                           'email/processor_request_fix.html', user=user,
                           processor_name=processor_name,
                           progress=progress),
                       sync=True)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def send_processor_analysis_email(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        if ((not cur_processor.end_date) or
                (cur_processor.end_date < datetime.today().date())):
            _set_task_progress(100)
            return True
        from urllib.parse import quote
        processor_name = quote(cur_processor.name)
        text_body = build_processor_analysis_email(
            processor_id, current_user_id)[0]
        for user in cur_processor.processor_followers:
            send_email('[Liquid App] {} | Analysis | {}'.format(
                cur_processor.name,
                datetime.today().date().strftime('%Y-%m-%d')),
                       sender=app.config['ADMINS'][0],
                       recipients=[user.email],
                       text_body=render_template(
                           'email/processor_analysis.txt', user=user,
                           processor_name=processor_name,
                           analysis=text_body),
                       html_body=render_template(
                           'email/processor_analysis.html', user=user,
                           processor_name=processor_name,
                           analysis=text_body),
                       sync=True)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def monthly_email_last_login(current_user_id, text_body, header, tab=0):
    try:
        cu = User.query.get(current_user_id)
        days_since_login = (cu.last_seen - datetime.today()).days
        if days_since_login == 0:
            days_ago_msg = "Today.  Woo hoo for you!"
        elif days_since_login == 1:
            days_ago_msg = "1 day ago.  Missed you today!"
        else:
            days_ago_msg = "{} days ago.  Where have you been?".format(
                days_since_login)
        msg = "Your last login was {}  ".format(days_ago_msg)
        if days_since_login <= 7:
            msg += ("We're happy you're using the app - please don't hesitate "
                    "to provide feedback!")
        elif days_since_login <= 30:
            msg += ("Logging in once a month is something.  If there is "
                    "anything that can be provided to help you find the app "
                    "more useful, please reach out!")
        elif days_since_login > 30:
            msg += ("You don't really care about data. :(  If you have "
                    "any suggestions on improving the app, "
                    "we would love to hear them!")
        text_body = add_text_body(text_body, msg, tab=tab)
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - User {}'.format(
                current_user_id), exc_info=sys.exc_info())
        return []


def monthly_email_app_updates(current_user_id, text_body, header, tab=0):
    try:
        import datetime as dt
        new_posts = Post.query.filter(Post.timestamp >= datetime.today() -
                                      dt.timedelta(days=1))
        new_posts = new_posts.filter_by(user_id=3)
        new_posts = new_posts.filter_by(processor_id=None)
        new_posts = new_posts.filter_by(uploader_id=None)
        text_body = add_text_body(text_body, header, tab=tab)
        msg = ("Here are some of the new features and updates for the app"
               " over the past month!  ")
        text_body = add_text_body(text_body, msg, tab)
        for p in new_posts:
            text_body = add_text_body(text_body, p.body, tab=tab + 1)
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - User {}'.format(
                current_user_id), exc_info=sys.exc_info())
        return []


def monthly_email_data_toplines(current_user_id, text_body, header, tab=0):
    try:
        import datetime as dt
        import processor.reporting.analyze as az
        cu = User.query.get(current_user_id)
        proc = cu.processor_followed.filter(
            Processor.end_date > datetime.today()).all()
        text_body = add_text_body(text_body, header, tab=tab)
        msg = ("You follow {} processors that are currently live!  ".format(
            len(proc)))
        if len(proc) == 0:
            msg += ("That's a bit sad isn't it!  To find some "
                    "processor instances to follow, go to the explore tab "
                    "(rocket ship in navbar) and press the blue 'Follow' "
                    "button!  Following a processor will give you automated "
                    "analysis emails daily and help populate your app "
                    "homepage.  Here's some topline data that you could be "
                    "getting: \n")
            ji_user = User.query.get(3)
            proc = ji_user.processor_followed.filter(
                Processor.end_date > datetime.today()).all()
        else:
            msg += "Some of that data is provided below."
        text_body = add_text_body(text_body, msg, tab=tab)
        proc = proc[:3]
        for p in proc:
            analysis = p.processor_analysis.all()
            text_body = analysis_email_basic(
                p.id, current_user_id, text_body=text_body,
                header='{} - TOPLINE TABLES'.format(p.name),
                full_analysis=analysis, analysis_keys=[az.Analyze.topline_col])
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - User {}'.format(
                current_user_id), exc_info=sys.exc_info())
        return []


def build_app_monthly_email(current_user_id):
    try:
        _set_task_progress(0)
        text_body = []
        arguments = [
            ('LAST LOGIN', monthly_email_last_login),
            ('APP UPDATES', monthly_email_app_updates),
            ('DATA TOPLINES', monthly_email_data_toplines)]
        for arg in arguments:
            text_body = arg[1](
                current_user_id, text_body=text_body, header=arg[0])
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - User {}'.format(
                current_user_id), exc_info=sys.exc_info())
        return []


def send_app_monthly_email(processor_id, current_user_id):
    try:
        # all_users = User.query.all()
        all_users = User.query.filter_by(username='James')
        for cur_user in all_users:
            text_body = build_app_monthly_email(cur_user.id)
            send_email('[Liquid App] | Monthly Update | {}'.format(
                datetime.today().date().strftime('%Y-%m-%d')),
                       sender=app.config['ADMINS'][0],
                       recipients=[cur_user.email],
                       text_body=render_template(
                           'email/app_monthly_updates.txt', user=cur_user,
                           analysis=text_body),
                       html_body=render_template(
                           'email/app_monthly_updates.html', user=cur_user,
                           analysis=text_body),
                       sync=True)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def set_processor_config_file(processor_id, current_user_id, config_type,
                              config_file_name):
    try:
        cur_processor = Processor.query.get(processor_id)
        client_name = cur_processor.campaign.product.client.name
        import processor.reporting.utils as utl
        os.chdir(adjust_path(cur_processor.local_path))
        file_path = '{}_api_cred'.format(config_type)
        file_name = '{}_dict.csv'.format(config_type)
        file_path = os.path.join(utl.config_path, file_path)
        df = pd.read_csv(os.path.join(file_path, file_name))
        file_name = df[df['client'] == client_name]['file'].values
        if file_name:
            file_name = file_name[0]
            copy_file(os.path.join(file_path, file_name),
                      os.path.join(utl.config_path, config_file_name))
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def set_processor_config_files(processor_id, current_user_id):
    try:
        for ct in [('twitter', 'twconfig.json'),
                   ('dc', 'dcapi.json'), ('dv', 'dvapi.json'),
                   ('s3', 's3config.json'),
                   ('exp', 'export_handler.csv')]:
            set_processor_config_file(
                processor_id=processor_id, current_user_id=current_user_id,
                config_type=ct[0], config_file_name=ct[1])
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def make_database_view(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.utils as utl
        import processor.reporting.export as exp
        os.chdir(adjust_path(cur_processor.local_path))
        product_name = cur_processor.campaign.product.name
        sb = exp.ScriptBuilder()
        script_text = sb.get_full_script(
            filter_col='productname',
            filter_val=product_name,
            filter_table='product')
        for x in [' ', ',', '.', '-', ':', '&', '+', '/']:
            product_name = product_name.replace(x, '')
        view_name = 'lqadb.lqapp_{}'.format(product_name)
        view_script = "CREATE OR REPLACE VIEW {} AS \n".format(view_name)
        view_script = view_script + script_text
        report_db = exp.DB('dbconfig.json')
        report_db.connect()
        report_db.cursor.execute(view_script)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def schedule_processor(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        msg_text = 'Scheduling processor: {}'.format(cur_processor.name)
        import datetime as dt
        sched = TaskScheduler.query.filter_by(
            processor_id=cur_processor.id).first()
        if not sched:
            cur_processor.schedule_job('.full_run_processor', msg_text,
                                       start_date=cur_processor.start_date,
                                       end_date=cur_processor.end_date,
                                       scheduled_time=dt.time(8, 0, 0),
                                       interval=24)
            db.session.commit()
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def build_processor_from_request(processor_id, current_user_id):
    progress = {
        'create': 'Failed',
        'set_config_files': 'Failed',
        'set_apis': 'Failed',
        'set_conversions': 'Failed',
        'set_fees': 'Failed',
        'set_planned_net': 'Failed',
        'run_processor': 'Failed',
        'schedule_processor': 'Failed'
    }
    try:
        _set_task_progress(0)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        cur_processor = Processor.query.get(processor_id)
        base_path = '/mnt/c/clients/{}/{}/{}/{}/processor'.format(
            cur_processor.campaign.product.client.name,
            cur_processor.campaign.product.name, cur_processor.campaign.name,
            cur_processor.name)
        cur_processor.local_path = base_path
        db.session.commit()
        _set_task_progress(12)
        result = create_processor(processor_id, current_user_id,
                                  app.config['BASE_PROCESSOR_PATH'])
        if result:
            progress['create'] = 'Success!'
        _set_task_progress(25)
        import_names = (cur_processor.campaign.name.
                        replace(' ', '').replace('_', '').replace('|', '').
                        replace(':', '').replace('.', '').replace("'", ''))
        proc_dict = [
            x.get_dict_for_processor(import_names, cur_processor.start_date)
            for x in cur_processor.accounts]
        os.chdir(cur_path)
        result = set_processor_config_files(processor_id, current_user_id)
        if result:
            progress['set_config_files'] = 'Success!'
        os.chdir(cur_path)
        result = set_processor_imports(processor_id, current_user_id, proc_dict)
        if result:
            progress['set_apis'] = 'Success!'
        _set_task_progress(37)
        os.chdir(cur_path)
        result = set_conversions(processor_id, current_user_id)
        if result:
            progress['set_conversions'] = 'Success!'
        _set_task_progress(50)
        os.chdir(cur_path)
        result = set_processor_fees(processor_id, current_user_id)
        if result:
            progress['set_fees'] = 'Success!'
        _set_task_progress(62)
        os.chdir(cur_path)
        result = set_processor_plan_net(processor_id, current_user_id)
        if result:
            progress['set_planned_net'] = 'Success!'
        _set_task_progress(75)
        os.chdir(cur_path)
        result = run_processor(processor_id, current_user_id,
                               '--api all --ftp all --dbi all --exp all --tab')
        if result:
            progress['run_processor'] = 'Success!'
        _set_task_progress(88)
        os.chdir(cur_path)
        result = schedule_processor(processor_id, current_user_id)
        if result:
            progress['schedule_processor'] = 'Success!'
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
    finally:
        send_processor_build_email(processor_id, current_user_id, progress)


def save_media_plan(processor_id, current_user_id, media_plan,
                    object_type=Processor):
    try:
        cur_obj = object_type.query.get(processor_id)
        cur_user = User.query.get(current_user_id)
        if not cur_obj.local_path:
            base_path = '/mnt/c/clients/{}/{}/{}/{}/processor'.format(
                cur_obj.campaign.product.client.name,
                cur_obj.campaign.product.name,
                cur_obj.campaign.name,
                cur_obj.name)
        else:
            base_path = cur_obj.local_path
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        if object_type == Processor:
            object_name = 'Processor'
            media_plan.to_csv(os.path.join(
                base_path, 'mediaplan.csv'
            ))
        else:
            object_name = 'Uploader'
            import uploader.upload.utils as utl
            utl.write_df(df=media_plan,
                         file_name=os.path.join(base_path, 'mediaplan.xlsx'),
                         sheet_name='Media Plan')
        msg_text = ('{} media plan was updated.'
                    ''.format(cur_obj.name))
        processor_post_message(cur_obj, cur_user, msg_text,
                               object_name=object_name)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def save_spend_cap_file(processor_id, current_user_id, new_data):
    try:
        cur_obj = Processor.query.get(processor_id)
        cur_user = User.query.get(current_user_id)
        new_data.seek(0)
        file_name = '/dictionaries/plannet_placement.csv'
        with open(cur_obj.local_path + file_name, 'wb') as f:
            shutil.copyfileobj(new_data, f, length=131072)
        msg_text = 'Spend cap file was saved.'
        processor_post_message(cur_obj, cur_user, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def set_spend_cap_config_file(processor_id, current_user_id, dict_col):
    try:
        cur_obj = Processor.query.get(processor_id)
        cur_user = User.query.get(current_user_id)
        cap_config_dict = {
            'file_name': ['dictionaries/plannet_placement.csv'],
            'file_dim': [dict_col],
            'file_metric': ['Net Cost (Capped)'],
            'processor_dim': [dict_col],
            'processor_metric': ['Planned Net Cost']}
        df = pd.DataFrame(cap_config_dict)
        os.chdir(adjust_path(cur_obj.local_path))
        df.to_csv('config/cap_config.csv', index=False)
        msg_text = ('{} spend cap config was updated.'
                    ''.format(cur_obj.name))
        processor_post_message(cur_obj, cur_user, msg_text)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def processor_fix_request(processor_id, current_user_id, fix):
    try:
        cur_processor = Processor.query.get(processor_id)
        ali_user = User.query.get(4)
        fixed = False
        if fix.fix_type == 'Update Plan':
            fixed = set_processor_plan_net(processor_id, ali_user.id)
        elif fix.fix_type == 'Spend Cap':
            fixed = set_spend_cap_config_file(processor_id, ali_user.id,
                                              fix.column_name)
        elif fix.fix_type == 'Change Dimension':
            pass
        elif fix.fix_type == 'Change Metric':
            pass
        if fixed:
            fix.mark_resolved()
            msg_text = ('{} processor request #{} was auto completed by ALI, '
                        'and marked as resolved!'
                        ''.format(cur_processor.name, fix.id))
            processor_post_message(cur_processor, ali_user, msg_text,
                                   request_id=fix.id)
            db.session.commit()
        return fixed
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def processor_fix_requests(processor_id, current_user_id):
    fix_result_dict = {}
    try:
        cur_processor = Processor.query.get(processor_id)
        cur_user = User.query.get(current_user_id)
        fixes = (Requests.query.filter_by(processor_id=cur_processor.id,
                                          complete=False).
                 order_by(Requests.created_at.desc()).all())
        for fix in fixes:
            result = processor_fix_request(processor_id, current_user_id, fix)
            if result:
                result = 'Successfully fixed!'
            else:
                result = 'Was not fixed.'
            fix_result_dict[fix.id] = result
        msg_text = ('{} processor requests were updated.'
                    ''.format(cur_processor.name))
        processor_post_message(cur_processor, cur_user, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
    finally:
        send_processor_request_email(processor_id, current_user_id,
                                     fix_result_dict)


def duplicate_processor_in_db(processor_id, current_user_id, form_data):
    try:
        cur_processor = Processor.query.get(processor_id)
        proc_dict = cur_processor.to_dict()
        if 'new_proc' in form_data:
            new_processor = Processor.query.get(form_data['new_proc'])
        else:
            new_processor = Processor()
        for k, v in proc_dict.items():
            new_processor.__setattr__(k, v)
        new_path = '/mnt/c/clients/{}/{}/{}/{}/processor'.format(
            cur_processor.campaign.product.client.name,
            cur_processor.campaign.product.name, cur_processor.campaign.name,
            form_data['new_name'])
        new_processor.local_path = new_path
        new_processor.name = form_data['new_name']
        new_processor.start_date = form_data['new_start_date']
        new_processor.end_date = form_data['new_end_date']
        if 'new_proc' not in form_data:
            db.session.add(new_processor)
        db.session.commit()
        return new_processor.id
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def set_vendormatrix_dates(processor_id, current_user_id, start_date=None,
                           end_date=None):
    try:
        cur_processor, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        import processor.reporting.utils as utl
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.vendormatrix as vm
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        if start_date:
            matrix.vm_df[vmc.startdate] = np.where(
                matrix.vm_df[vmc.startdate].isnull(),
                start_date, matrix.vm_df[vmc.startdate])
            start_date = np.datetime64(start_date)
            matrix.vm_df = utl.data_to_type(df=matrix.vm_df,
                                            date_col=[vmc.startdate])
            matrix.vm_df[vmc.startdate] = np.where(
                matrix.vm_df[vmc.startdate] < pd.Timestamp(start_date),
                start_date, matrix.vm_df[vmc.startdate])
        if end_date:
            matrix.vm_df[vmc.enddate] = np.where(
                matrix.vm_df[vmc.enddate].isnull(),
                end_date, matrix.vm_df[vmc.enddate])
            matrix.vm_df = utl.data_to_type(df=matrix.vm_df,
                                            date_col=[vmc.enddate])
            end_date = np.datetime64(end_date)
            matrix.vm_df[vmc.enddate] = np.where(
                matrix.vm_df[vmc.enddate] > end_date,
                end_date, matrix.vm_df[vmc.enddate])
        matrix.write()
        msg_text = ('{} processor vendormatrix dates were updated.'
                    ''.format(cur_processor.name))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        os.chdir(cur_path)
        get_processor_sources(processor_id, current_user_id)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def remove_upload_id_file(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.utils as utl
        import processor.reporting.expcolumns as exp
        os.chdir(adjust_path(cur_processor.local_path))
        os.remove(os.path.join(utl.config_path, exp.upload_id_file))
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def duplicate_processor(processor_id, current_user_id, form_data):
    progress = {
        'duplicate_in_db': 'Failed',
        'duplicate_in_server': 'Failed',
        'old_processor_set_dates': 'Failed',
        'new_processor_set_dates': 'Failed',
        'new_processor_remove_upload_id': 'Failed',
        'new_processor_run': 'Failed',
        'old_processor_run': 'Failed',
        'schedule_processor': 'Failed'
    }
    try:
        import datetime as dt
        _set_task_progress(0)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        cur_processor = Processor.query.get(processor_id)
        new_processor_id = duplicate_processor_in_db(
            processor_id, current_user_id, form_data)
        if new_processor_id:
            progress['duplicate_in_db'] = 'Success!'
        _set_task_progress(12)
        result = create_processor(new_processor_id, current_user_id,
                                  cur_processor.local_path)
        if result:
            progress['duplicate_in_server'] = 'Success!'
        _set_task_progress(25)
        os.chdir(cur_path)
        result = set_vendormatrix_dates(cur_processor.id, current_user_id,
                                        end_date=(form_data['new_start_date'] -
                                                  dt.timedelta(days=1)))
        if result:
            progress['old_processor_set_dates'] = 'Success!'
        _set_task_progress(37)
        os.chdir(cur_path)
        result = set_vendormatrix_dates(new_processor_id, current_user_id,
                                        start_date=form_data['new_start_date'])
        if result:
            progress['new_processor_set_dates'] = 'Success!'
        _set_task_progress(50)
        os.chdir(cur_path)
        result = remove_upload_id_file(new_processor_id, current_user_id)
        if result:
            progress['new_processor_remove_upload_id'] = 'Success!'
        _set_task_progress(62)
        os.chdir(cur_path)
        result = run_processor(new_processor_id, current_user_id,
                               '--api all --ftp all --dbi all --exp all --tab')
        if result:
            progress['new_processor_run'] = 'Success!'
        _set_task_progress(75)
        os.chdir(cur_path)
        if form_data['old_processor_run']:
            result = run_processor(
                processor_id, current_user_id,
                '--api all --ftp all --dbi all --exp all --tab')
        else:
            result = True
        if result:
            progress['old_processor_run'] = 'Success!'
        _set_task_progress(88)
        os.chdir(cur_path)
        result = schedule_processor(new_processor_id, current_user_id)
        if result:
            progress['schedule_processor'] = 'Success!'
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
    finally:
        send_processor_build_email(
            processor_id, current_user_id, progress,
            title='[Liquid App] New Processor Duplication Request!')


def duplicate_uploader_in_db(uploader_id, current_user_id, form_data):
    try:
        cur_uploader = Uploader.query.get(uploader_id)
        up_dict = cur_uploader.to_dict()
        new_uploader = Uploader()
        for k, v in up_dict.items():
            new_uploader.__setattr__(k, v)
        new_path = '/mnt/c/clients/{}/{}/{}/{}/uploader'.format(
            cur_uploader.campaign.product.client.name,
            cur_uploader.campaign.product.name, cur_uploader.campaign.name,
            form_data['new_name'])
        new_uploader.local_path = new_path
        new_uploader.name = form_data['new_name']
        db.session.add(new_uploader)
        db.session.commit()
        return new_uploader.id
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        return False


def duplicate_uploader_objects(uploader_id, current_user_id, old_uploader_id):
    try:
        from app.main.routes import create_base_uploader_objects
        create_base_uploader_objects(uploader_id)
        for object_level in ['Campaign', 'Adset', 'Ad']:
            upo = UploaderObjects.query.filter_by(
                uploader_id=uploader_id,  object_level=object_level).first()
            old_upo = UploaderObjects.query.filter_by(
                uploader_id=old_uploader_id,  object_level=object_level).first()
            old_up_dict = old_upo.to_dict()
            for k, v in old_up_dict.items():
                upo.__setattr__(k, v)
            for relation in upo.uploader_relations:
                old_rel = UploaderRelations.query.filter_by(
                    impacted_column_name=relation.impacted_column_name,
                    uploader_objects_id=old_upo.id).first()
                old_rel_dict = old_rel.to_dict()
                for k, v in old_rel_dict.items():
                    relation.__setattr__(k, v)
        db.session.commit()
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        return False


def duplicate_uploader(uploader_id, current_user_id, form_data):
    progress = {
        'duplicate_in_db': 'Failed',
        'duplicate_in_server': 'Failed',
        'create_new_uploader_objects_in_db': 'Failed'
    }
    try:
        import datetime as dt
        _set_task_progress(0)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        cur_uploader = Uploader.query.get(uploader_id)
        new_uploader_id = duplicate_uploader_in_db(
            uploader_id, current_user_id, form_data)
        if new_uploader_id:
            progress['duplicate_in_db'] = 'Success!'
        _set_task_progress(33)
        result = create_uploader(new_uploader_id, current_user_id,
                                 cur_uploader.local_path)
        if result:
            progress['duplicate_in_server'] = 'Success!'
        _set_task_progress(66)
        os.chdir(cur_path)
        result = duplicate_uploader_objects(new_uploader_id, current_user_id,
                                            uploader_id)
        if result:
            progress['create_new_uploader_objects_in_db'] = 'Success!'
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
    finally:
        cur_user = User.query.get(current_user_id)
        send_processor_build_email(
            uploader_id, current_user_id, progress,
            title='[Liquid App] New Uploader Duplication Request!',
            object_type=Uploader, recipients=[cur_user])


def clean_total_metric_df(df, col_name):
    if df.empty:
        df[col_name] = '0'
    else:
        df = df.rename(columns={df.columns[0]: col_name})
    return df


def get_processor_total_metrics_file(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        import reporting.analyze as az
        import reporting.vendormatrix as vm
        import reporting.dictcolumns as dctc
        import reporting.vmcolumns as vmc
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        aly = az.Analyze(file_name='Raw Data Output.csv', matrix=matrix)
        df, tdf, twdf = aly.generate_topline_and_weekly_metrics(group=dctc.PRN)
        df = clean_total_metric_df(df, 'current_value')
        tdf = clean_total_metric_df(tdf, 'new_value')
        twdf = clean_total_metric_df(twdf, 'old_value')
        df = df.join(tdf)
        df = df.join(twdf)
        df['change'] = (
            (df['new_value'].str.replace(',', '').str.replace(
                '$', '').astype(float) -
             df['old_value'].str.replace(',', '').str.replace(
                 '$', '').astype(float)) /
            df['old_value'].str.replace(',', '').str.replace(
                 '$', '').astype(float))
        df['change'] = df['change'].round(4)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna(0)
        df = df.rename_axis('name').reset_index()
        df = df[df['name'].isin(['Net Cost Final', vmc.impressions,
                                 vmc.clicks, 'CPC'])]
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id),
            exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def clean_topline_df_from_db(db_item, new_col_name):
    import processor.reporting.utils as utl
    import processor.reporting.analyze as az
    df = pd.DataFrame(db_item.data)
    if not df.empty:
        df = utl.data_to_type(df, float_col=list(df.columns))
        df = pd.DataFrame(df.fillna(0).T.sum()).T
        calculated_metrics = az.ValueCalc().metric_names
        metric_names = [x for x in df.columns if x in calculated_metrics]
        df = az.ValueCalc().calculate_all_metrics(
            metric_names=metric_names, df=df)
    df = clean_total_metric_df(df.T, new_col_name)
    return df


def get_processor_total_metrics(processor_id, current_user_id,
                                filter_dict=None):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.analyze as az
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.utils as utl
        topline_analysis = cur_processor.processor_analysis.filter_by(
            key=az.Analyze.topline_col).all()
        if not topline_analysis:
            _set_task_progress(100)
            return [pd.DataFrame()]
        df = clean_topline_df_from_db(
            [x for x in topline_analysis
             if x.parameter == az.Analyze.topline_col][0], 'current_value')
        tdf = clean_topline_df_from_db(
            [x for x in topline_analysis
             if x.parameter == az.Analyze.lw_topline_col][0], 'new_value')
        twdf = clean_topline_df_from_db(
            [x for x in topline_analysis
             if x.parameter == az.Analyze.tw_topline_col][0], 'old_value')
        df = df.join(tdf)
        df = df.join(twdf)
        if filter_dict:
            tdf = get_data_tables_from_db(
                processor_id, current_user_id, dimensions=['productname'],
                metrics=['impressions', 'clicks', 'netcost'],
                filter_dict=filter_dict)
            tdf = tdf[0][['impressions', 'clicks', 'netcost']]
            tdf = tdf.rename(
                columns={'impressions': 'Impressions', 'clicks': 'Clicks',
                         'netcost': 'Net Cost Final'})
            tdf['CPC'] = tdf['Net Cost Final'] / tdf['Clicks']
            df = df.join(tdf.T)
            df['change'] = (df[0].astype(float) /
                            df['current_value'].astype(float))
            df = df.drop(columns='current_value').rename(
                columns={0: 'current_value'})
            df = df[['current_value'] +
                    [x for x in df.columns if x != 'current_value']]
        else:
            df['change'] = ((df['new_value'].astype(float) -
                             df['old_value'].astype(float)) /
                            df['old_value'].astype(float))
        df['change'] = df['change'].round(4)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna(0)
        tdf = df.T.iloc[:-1]
        tdf = utl.give_df_default_format(tdf)
        df = tdf.T.join(df['change'])
        df = df.rename_axis('name').reset_index()
        df = df[df['name'].isin(['Net Cost Final', vmc.impressions,
                                 vmc.clicks, 'CPC'])]
        if filter_dict:
            df['msg'] = 'Of Total'
        else:
            df['msg'] = 'Since Last Week'
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id),
            exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_data_tables_from_db(processor_id, current_user_id, parameter=None,
                            dimensions=None, metrics=None, filter_dict=None):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.utils as utl
        import processor.reporting.export as export
        import processor.reporting.analyze as az
        if ((not cur_processor.local_path) or
                (not os.path.exists(adjust_path(cur_processor.local_path)))):
            _set_task_progress(100)
            return [pd.DataFrame({x: [] for x in dimensions + metrics})]
        _set_task_progress(15)
        if metrics == ['kpi']:
            kpis, kpi_cols = get_kpis_for_processor(
                processor_id, current_user_id)
            metrics = [x for x in ['impressions', 'clicks', 'netcost']
                       if x not in kpi_cols] + kpi_cols
        else:
            kpis = None
        os.chdir(adjust_path(cur_processor.local_path))
        if not metrics:
            metrics = ['impressions', 'clicks', 'netcost']
        if not os.path.exists('config/upload_id_file.csv'):
            _set_task_progress(100)
            return [pd.DataFrame({x: [] for x in dimensions + metrics})]
        dimensions = ['event.{}'.format(x) if x == 'eventdate'
                      else x for x in dimensions]
        dimensions = ','.join(dimensions)
        metric_sql = ','.join(['SUM({0}) AS {0}'.format(x) for x in metrics])
        up_id = pd.read_csv('config/upload_id_file.csv')
        up_id = up_id['uploadid'][0]
        where_sql = "WHERE fullplacement.uploadid = '{}'".format(up_id)
        where_args = []
        if filter_dict:
            for f in filter_dict:
                for k, v in f.items():
                    if v:
                        if k == 'eventdate':
                            sd = datetime.strptime(v[0],
                                '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d')
                            ed = datetime.strptime(v[1],
                                '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d')
                            w = (" AND (event.{0} BETWEEN '{1}' AND '{2}' "
                                 "OR event.{0} IS NULL)".format(k, sd, ed))
                        else:
                            w = " AND {} IN ({})".format(
                                k, ', '.join(['%s'] * len(v)))
                            where_args.extend(v)
                        where_sql += w
        _set_task_progress(30)
        command = """SELECT {0},{1}
            FROM lqadb.event
            FULL JOIN lqadb.fullplacement ON event.fullplacementid = fullplacement.fullplacementid
            FULL JOIN lqadb.plan ON plan.fullplacementid = fullplacement.fullplacementid
            LEFT JOIN lqadb.vendor ON fullplacement.vendorid = vendor.vendorid
            LEFT JOIN lqadb.campaign ON fullplacement.campaignid = campaign.campaignid
            LEFT JOIN lqadb.country ON fullplacement.countryid = country.countryid
            LEFT JOIN lqadb.product ON campaign.productid = product.productid
            LEFT JOIN lqadb.environment ON fullplacement.environmentid = environment.environmentid
            LEFT JOIN lqadb.kpi ON fullplacement.kpiid = kpi.kpiid
            {2}
            GROUP BY {0}
        """.format(dimensions, metric_sql, where_sql)
        db_class = export.DB()
        db_class.input_config('dbconfig.json')
        db_class.connect()
        _set_task_progress(50)
        db_class.cursor.execute(command, where_args)
        data = db_class.cursor.fetchall()
        _set_task_progress(70)
        columns = [i[0] for i in db_class.cursor.description]
        df = pd.DataFrame(data=data, columns=columns)
        _set_task_progress(90)
        df = utl.data_to_type(df, float_col=metrics)
        if 'eventdate' in df.columns:
            df = utl.data_to_type(df, str_col=['eventdate'])
            df = df[df['eventdate'] != 'None']
        df = df.fillna(0)
        if kpis:
            calculated_metrics = az.ValueCalc().metric_names
            metric_names = [x for x in kpis if x in calculated_metrics]
            df = az.ValueCalc().calculate_all_metrics(
                metric_names=metric_names, df=df, db_translate=True)
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.fillna(0)
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} Parameter {}'.format(
                processor_id, current_user_id, parameter),
            exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_raw_file_data_table(processor_id, current_user_id, parameter=None,
                            dimensions=None, metrics=None, filter_dict=None):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.vendormatrix as vm
        import processor.reporting.utils as utl
        import processor.reporting.export as export
        import processor.reporting.analyze as az
        if ((not cur_processor.local_path) or
                (not os.path.exists(adjust_path(cur_processor.local_path)))):
            _set_task_progress(100)
            return [pd.DataFrame({x: [] for x in dimensions + metrics})]
        _set_task_progress(15)
        if metrics == ['kpi']:
            kpis, kpi_cols = get_kpis_for_processor(
                processor_id, current_user_id)
            metrics = [x for x in ['Impressions', 'Clicks', 'Net Cost']
                       if x not in kpi_cols] + kpi_cols
        else:
            kpis = None
        if not metrics:
            metrics = ['Impressions', 'Clicks', 'Net Cost']
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        _set_task_progress(60)
        df = matrix.vendor_get(parameter)
        _set_task_progress(90)
        df = utl.data_to_type(df, float_col=metrics)
        metrics = [x for x in metrics if x in df.columns]
        df = df.groupby(dimensions)[metrics].sum()
        df = df.reset_index()
        if 'Date' in df.columns:
            df = utl.data_to_type(df, str_col=['Date'])
            df = df[df['Date'] != 'None']
        df = df.fillna(0)
        if kpis:
            df['Net Cost Final'] = df['Net Cost']
            calculated_metrics = az.ValueCalc().metric_names
            metric_names = [x for x in kpis if x in calculated_metrics]
            df = az.ValueCalc().calculate_all_metrics(
                metric_names=metric_names, df=df, db_translate=False)
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.fillna(0)
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} Parameter {}'.format(
                processor_id, current_user_id, parameter),
            exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def create_processor_request(processor_id, current_user_id, fix_type,
                             fix_description):
    try:
        cur_proc = Processor.query.get(processor_id)
        ali_user = User.query.get(4)
        new_processor_request = Requests(
            processor_id=cur_proc.id, fix_type=fix_type,
            fix_description=fix_description
        )
        db.session.add(new_processor_request)
        db.session.commit()
        creation_text = "Processor {} fix request {} was created".format(
            cur_proc.name, new_processor_request.id)
        post = Post(body=creation_text, author=ali_user,
                    processor_id=cur_proc.id,
                    request_id=new_processor_request.id)
        db.session.add(post)
        db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def update_single_auto_request(processor_id, current_user_id, fix_type,
                               fix_description, undefined):
    try:
        ali_user = User.query.get(4)
        cur_processor = Processor.query.get(processor_id)
        cur_request = [x for x in cur_processor.get_all_requests()
                       if x.fix_type == fix_type]
        if len(undefined) > 0 and len(cur_request) == 0:
            create_processor_request(processor_id, current_user_id, fix_type,
                                     fix_description)
        elif len(undefined) > 0 and len(cur_request) > 0:
            cur_request = cur_request[0]
            cur_request.fix_description = fix_description
            cur_request.complete = False
            db.session.add(cur_request)
            db.session.commit()
        elif len(undefined) == 0 and len(cur_request) > 0:
            cur_request = cur_request[0]
            if not cur_request.complete:
                msg_txt = 'The fix #{} has been marked as resolved!'.format(
                    cur_request.id)
                cur_request.mark_resolved()
                post = Post(body=msg_txt, author=ali_user,
                            processor_id=cur_request.processor.id,
                            request_id=cur_request.id)
                db.session.add(post)
                db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def update_automatic_requests(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.analyze as az
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=az.Analyze.unknown_col).first()
        fix_type = az.Analyze.unknown_col
        if analysis.data:
            tdf = pd.DataFrame(analysis.data)
            for col in tdf.columns:
                tdf[col] = tdf[col].str.strip("'")
            cols = [x for x in tdf.columns if x != 'Vendor Key']
            col = 'Undefined Plan Net'
            tdf[col] = tdf[cols].values.tolist()
            tdf[col] = tdf[col].str.join('_')
            undefined = tdf[col].to_list()
            fix_description = '{} {}'.format(analysis.message, undefined)
        else:
            undefined = []
            fix_description = '{}'.format(analysis.message)
        update_single_auto_request(processor_id, current_user_id,
                                   fix_type=fix_type,
                                   fix_description=fix_description,
                                   undefined=undefined)
        fix_type = az.Analyze.raw_file_update_col
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis.data:
            df = pd.DataFrame(analysis.data)
            tdf = df[df['source'].str[:3] == 'API']
            tdf = tdf[tdf['source'].str[:len('API_Rawfile')] == 'API_Rawfile']
            tdf = tdf[tdf['update_tier'] == 'Greater Than One Week']
            undefined = tdf['source'].tolist()
            msg = ''
            if len(tdf) > 0:
                msg += ('The following raw files have not been updated for '
                        'over a week: {}\n\n'.format(','.join(undefined)))
            tdf = df[df['source'].str[:3] == 'API']
            tdf = tdf[tdf['source'].str[:len('API_Rawfile')] != 'API_Rawfile']
            tdf = tdf[tdf['update_tier'] != 'Today']
            if len(tdf) > 0:
                api_undefined = tdf['source'].tolist()
                msg += ('The following API files did not update today: '
                        ' {}\n'.format(','.join(api_undefined)))
                undefined.extend(api_undefined)
            update_single_auto_request(processor_id, current_user_id,
                                       fix_type=fix_type,
                                       fix_description=msg,
                                       undefined=undefined)
        fix_type = az.Analyze.missing_metrics
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis.data:
            df = pd.DataFrame(analysis.data)
            undefined = (df['mpVendor'] + ' - ' + df['missing_metrics']).to_list()
            msg = ('{} {}\n\n'.format(analysis.message, ','.join(undefined)))
            update_single_auto_request(processor_id, current_user_id,
                                       fix_type=fix_type,
                                       fix_description=msg,
                                       undefined=undefined)
        else:
            undefined = []
            msg = '{}'.format(analysis.message)
            update_single_auto_request(processor_id, current_user_id,
                                       fix_type=fix_type,
                                       fix_description=msg,
                                       undefined=undefined)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def update_analysis_in_db(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.analyze as az
        os.chdir(adjust_path(cur_processor.local_path))
        with open(az.Analyze.analysis_dict_file_name, 'r') as f:
            analysis_dict = json.load(f)
        all_analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id).all()
        for analysis in all_analysis:
            analysis_dict_val = [
                x for x in analysis_dict if
                x[az.Analyze.analysis_dict_key_col] == analysis.key and
                x[az.Analyze.analysis_dict_filter_col
                  ] == analysis.filter_col and
                x[az.Analyze.analysis_dict_filter_val
                  ] == analysis.filter_val and
                x[az.Analyze.analysis_dict_split_col] == analysis.split_col and
                x[az.Analyze.analysis_dict_param_col] == analysis.parameter and
                x[az.Analyze.analysis_dict_param_2_col] == analysis.parameter_2]
            if not analysis_dict_val:
                db.session.delete(analysis)
                db.session.commit()
        for analysis in analysis_dict:
            old_analysis = [
                x for x in all_analysis if
                analysis[az.Analyze.analysis_dict_key_col] == x.key and
                analysis[
                    az.Analyze.analysis_dict_filter_col] == x.filter_col and
                analysis[
                    az.Analyze.analysis_dict_filter_val] == x.filter_val and
                analysis[
                    az.Analyze.analysis_dict_split_col] == x.split_col and
                analysis[az.Analyze.analysis_dict_param_col] == x.parameter and
                analysis[az.Analyze.analysis_dict_param_2_col] == x.parameter_2]
            if old_analysis:
                old_analysis = old_analysis[0]
                old_analysis.data = analysis[az.Analyze.analysis_dict_data_col]
                old_analysis.message = analysis[
                    az.Analyze.analysis_dict_msg_col]
                old_analysis.date = datetime.today().date()
                db.session.commit()
            else:
                new_analysis = ProcessorAnalysis(
                    key=analysis[az.Analyze.analysis_dict_key_col],
                    parameter=analysis[az.Analyze.analysis_dict_param_col],
                    parameter_2=analysis[az.Analyze.analysis_dict_param_2_col],
                    filter_col=analysis[az.Analyze.analysis_dict_filter_col],
                    filter_val=analysis[az.Analyze.analysis_dict_filter_val],
                    split_col=analysis[az.Analyze.analysis_dict_split_col],
                    data=analysis[az.Analyze.analysis_dict_data_col],
                    message=analysis[az.Analyze.analysis_dict_msg_col],
                    processor_id=cur_processor.id,
                    date=datetime.today().date())
                db.session.add(new_analysis)
                db.session.commit()
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def add_text_body(text_body, msg, tab, data=pd.DataFrame()):
    text_dict = {'message': '{}\n'.format(msg), 'tab': tab}
    if not data.empty:
        text_dict['data'] = data
    text_body.append(text_dict)
    return text_body


def analysis_email_basic(processor_id, current_user_id, text_body, header,
                         full_analysis, analysis_keys, tab=0, param='key'):
    try:
        analysis = [
            x for x in full_analysis if x.__dict__[param] in analysis_keys]
        text_body = add_text_body(text_body, header, tab=tab)
        for a in analysis:
            if not a.data:
                text_body = add_text_body(text_body, a.message, tab=tab + 1)
            else:
                df = pd.DataFrame(a.data)
                if not df.empty:
                    text_body = add_text_body(text_body, a.message,
                                              tab=tab+1, data=df)
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return []


def analysis_email_kpi(processor_id, current_user_id, text_body, header,
                       full_analysis, analysis_keys):
    try:
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.dictcolumns as dctc
        analysis = [x for x in full_analysis if x.key in analysis_keys]
        text_body = add_text_body(text_body, header, tab=0)
        kpis = set(x.parameter for x in analysis
                   if x.parameter not in ['0', 'nan'])
        for kpi in kpis:
            text_body = add_text_body(text_body, kpi, tab=1)
            cur_analysis = [x for x in analysis if x.parameter == kpi]
            text_body = add_text_body(text_body, 'Partner', tab=2)
            par_analysis = [x for x in cur_analysis if x.split_col == dctc.VEN]
            for a in par_analysis:
                df = pd.DataFrame(a.data)
                text_body = add_text_body(text_body, a.message, 3, df)
                partners = pd.DataFrame(a.data)[dctc.VEN].to_list()
                for p in partners:
                    text_body = add_text_body(text_body, p, 3)
                    ind_par_anlaysis = [x for x in cur_analysis
                                        if x.filter_val == p
                                        and x.parameter_2 == a.parameter_2]
                    for ind_par in ind_par_anlaysis:
                        text_body = add_text_body(text_body, ind_par.message, 4)
            analysis_email_basic(
                processor_id, current_user_id, text_body, vmc.date,
                cur_analysis, [vmc.date], tab=2, param='split_col')
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return []


def build_processor_analysis_email(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        import processor.reporting.analyze as az
        cur_processor = Processor.query.get(processor_id)
        analysis = cur_processor.processor_analysis.all()
        text_body = []
        arguments = [
            ('TOPLINE', [az.Analyze.topline_col], analysis_email_basic),
            ('DELIVERY',
             [az.Analyze.delivery_col, az.Analyze.delivery_comp_col],
             analysis_email_basic),
            ('KPI ANALYSIS', [az.Analyze.kpi_col], analysis_email_kpi),
            ('REPORTING QA',
             [az.Analyze.unknown_col, az.Analyze.raw_file_update_col],
             analysis_email_basic)
        ]
        for arg in arguments:
            text_body = arg[2](
                processor_id, current_user_id, text_body=text_body,
                header=arg[0], full_analysis=analysis,
                analysis_keys=arg[1])
        _set_task_progress(100)
        return [text_body]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return []


def get_kpis_for_processor(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        import processor.reporting.analyze as az
        import processor.reporting.expcolumns as exc
        cur_processor = Processor.query.get(processor_id)
        vc = az.ValueCalc()
        analysis = cur_processor.processor_analysis.all()
        analysis = [x for x in analysis if x.key in [az.Analyze.kpi_col]]
        kpis = set(x.parameter for x in analysis
                   if x.parameter not in ['0', 'nan', 'CPA'])
        kpi_formula = [vc.calculations[x] for x in vc.calculations
                       if vc.calculations[x][vc.metric_name] in kpis]
        kpi_cols = [x[vc.formula][::2] for x in kpi_formula]
        kpi_cols = set([x for x in kpi_cols for x in x if x])
        df = pd.read_csv(os.path.join(adjust_path(cur_processor.local_path),
                                      'config', 'db_df_translation.csv'))
        translation = dict(zip(df[exc.translation_df], df[exc.translation_db]))
        kpi_cols = [translation[x] for x in kpi_cols if x in translation]
        _set_task_progress(100)
        return kpis, kpi_cols
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return [], []


def parse_date_from_project_number(cur_string, date_opened):
    try:
        sd = cur_string.strip().split('/')
        if len(sd) > 2:
            cur_year = int(sd[2])
        else:
            cur_year = date_opened.year
        sd = datetime(cur_year, int(sd[0]), int(sd[1]))
        return sd
    except:
        return None


def get_project_numbers(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        import processor.reporting.gsapi as gsapi
        import processor.reporting.utils as utl
        os.chdir('processor')
        api = gsapi.GsApi()
        api.input_config('gsapi.json')
        df = api.get_data()
        df = utl.first_last_adj(df, 3, 0)
        df = df.rename_axis(None, axis=1).rename_axis(
            'index', axis=0).reset_index(drop=True)
        df = df.sort_index(ascending=False)
        pn_max = ProjectNumberMax.query.get(1)
        ndf = df[(df.index >= pn_max.max_number) & (~df['Client'].isna())]
        for pn in ndf.to_dict(orient='records'):
            c_project = Project.query.filter_by(project_number=pn['#']).first()
            if not c_project:
                cur_client = Client.query.filter_by(name=pn['Client']).first()
                if not cur_client:
                    cur_client = Client(name=pn['Client'])
                    db.session.add(cur_client)
                    db.session.commit()
                if pn['Date Opened']:
                    try:
                        date_opened = datetime.strptime(
                            pn['Date Opened'], '%m/%d/%y')
                    except:
                        date_opened = None
                else:
                    date_opened = None
                if pn['FLIGHT DATES']:
                    flight = pn['FLIGHT DATES'].split(
                        '-' if '-' in pn['FLIGHT DATES'] else 'to')
                    sd = parse_date_from_project_number(flight[0], date_opened)
                    if len(flight) > 1:
                        ed = parse_date_from_project_number(flight[1],
                                                            date_opened)
                    else:
                        ed = sd
                else:
                    sd = ed = None
                new_project = Project(
                    project_number=pn['#'],
                    initial_project_number=pn['initial PN'],
                    client_id=cur_client.id, project_name=pn['Project'],
                    media=True if pn['Media'] else False,
                    creative=True if pn['Creative'] else False,
                    date_opened=date_opened, flight_start_date=sd,
                    flight_end_date=ed, exhibit=pn['Exhibit #'],
                    sow_received=pn["SOW rec'd"],
                    billing_dates=pn['Billings + date(s)'], notes=pn['NOTES'])
                db.session.add(new_project)
                db.session.commit()
                form_product = Product(
                    name='None',
                    client_id=cur_client.id).check_and_add()
                form_campaign = Campaign(
                    name=pn['Project'],
                    product_id=form_product.id).check_and_add()
                description = ('Automatically generated from '
                               'project number: {}').format(pn['#'])
                name = (pn['Project'].
                        replace('_', ' ').replace('|', ' ').
                        replace(':', ' ').replace('.', ' ').replace("'", ' ').
                        replace('&', ' '))
                new_processor = Processor(
                    name=name, description=description,
                    user_id=4, created_at=datetime.utcnow(),
                    start_date=sd, end_date=ed, campaign_id=form_campaign.id)
                db.session.add(new_processor)
                db.session.commit()
                new_processor.projects.append(new_project)
                db.session.commit()
        pn_max.max_number = max(ndf.index)
        db.session.commit()
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())
        return pd.DataFrame()


def get_all_processors(user_id, running_user):
    try:
        _set_task_progress(0)
        p = Processor.query.all()
        df = pd.DataFrame([
            {'name': x.name, 'id': x.id, 'campaign': x.campaign.name,
             'product': x.campaign.product.name,
             'client': x.campaign.product.client.name,
             'project_numbers': ','.join(
                 [y.project_number for y in x.projects.all()]),
             'url': 'lqadata.com/processor/{}'.format(x.name)} for x in p])
        tables = [df]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - User {}'.format(user_id),
            exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def update_tutorial(user_id, running_user, tutorial_name, new_data):
    try:
        _set_task_progress(0)
        cur_tutorial = Tutorial.query.filter_by(name=tutorial_name).first()
        if not cur_tutorial:
            cur_tutorial = Tutorial(name=tutorial_name)
            db.session.add(cur_tutorial)
            db.session.commit()
        new_data.seek(0)
        df = pd.read_excel(new_data)
        df = df.fillna('')
        tut_dict = df.to_dict(orient='index')
        for tut_stage_id in tut_dict:
            tut_stage = tut_dict[tut_stage_id]
            tut_level = int(tut_stage['tutorial_level'])
            db_stage = TutorialStage.query.filter_by(
                tutorial_id=cur_tutorial.id, tutorial_level=tut_level).first()
            if not db_stage:
                new_stage = TutorialStage(
                    tutorial_id=cur_tutorial.id,
                    tutorial_level=tut_level,
                    header=tut_stage['header'],
                    sub_header=tut_stage['sub_header'],
                    message=tut_stage['message'], alert=tut_stage['alert'],
                    alert_level=tut_stage['alert_level'],
                    image=tut_stage['image'], question=tut_stage['question'],
                    question_answers=tut_stage['question_answers'],
                    correct_answer=tut_stage['correct_answer'])
                db.session.add(new_stage)
                db.session.commit()
            else:
                db_stage.header = tut_stage['header']
                db_stage.sub_header = tut_stage['sub_header']
                db_stage.message = tut_stage['message']
                db_stage.alert = tut_stage['alert']
                db_stage.alert_level = tut_stage['alert_level']
                db_stage.image = tut_stage['image']
                db_stage.question = tut_stage['question']
                db_stage.question_answers = tut_stage['question_answers']
                db_stage.correct_answer = tut_stage['correct_answer']
                db.session.commit()
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - User {}'.format(user_id),
            exc_info=sys.exc_info())
        return False


def update_walkthrough(user_id, running_user, new_data):
    try:
        _set_task_progress(0)
        new_data.seek(0)
        df = pd.read_excel(new_data)
        df = df.fillna('')
        walk_dict = df.to_dict(orient='index')
        for k, walk in walk_dict.items():
            db_id = k + 1
            cur_walk = Walkthrough.query.get(db_id)
            if cur_walk:
                cur_walk.edit_name = walk['edit_name']
                cur_walk.title = walk['title']
                db.session.commit()
            else:
                cur_walk = Walkthrough(
                    edit_name=walk['edit_name'], title=walk['title'])
                db.session.add(cur_walk)
                db.session.commit()
            max_slides = max([x.split('-')[0].split('slide')[1]
                              for x in walk.keys() if 'slide' in x])
            for slide_num in range(int(max_slides) + 1):
                slide_text_name = 'slide{}-text'.format(slide_num)
                slide_show_name = 'slide{}-show_me'.format(slide_num)
                slide_data_name = 'slide{}-data'.format(slide_num)
                walk_slide = WalkthroughSlide.query.filter_by(
                    walkthrough_id=cur_walk.id, slide_number=slide_num).first()
                if walk_slide and not walk[slide_text_name]:
                    db.session.delete(walk_slide)
                    db.session.commit()
                elif walk_slide:
                    walk_slide.slide_text = walk[slide_text_name]
                    walk_slide.show_me_element = walk[slide_show_name]
                    if slide_data_name in walk:
                        walk_slide.data = walk[slide_data_name]
                else:
                    new_slide = WalkthroughSlide(
                        walkthrough_id=cur_walk.id,
                        slide_number=slide_num,
                        slide_text=walk[slide_text_name],
                        show_me_element=walk[slide_show_name])
                    if slide_data_name in walk:
                        new_slide.data = walk[slide_data_name]
                    db.session.add(new_slide)
                    db.session.commit()
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - User {}'.format(user_id),
            exc_info=sys.exc_info())
        return False


def get_raw_file_comparison(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        import reporting.analyze as az
        import reporting.vendormatrix as vm
        import processor.reporting.utils as utl
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        aly = az.Analyze(file_name='Raw Data Output.csv', matrix=matrix)
        aly.compare_raw_files(vk)
        file_name = "{}.json".format(vk)
        with open(file_name, 'r') as f:
            config_file = json.load(f)
        # df = pd.DataFrame(config_file)
        tables = [config_file]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]
