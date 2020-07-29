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
    ProcessorAnalysis

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
        msg = Message(author=usr, recipient=usr, body=text)
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


def run_processor(processor_id, current_user_id, processor_args):
    try:
        processor_to_run, user_that_ran = get_processor_and_user_from_id(
            processor_id=processor_id, current_user_id=current_user_id)
        post_body = ('Running {} for processor: {}...'.format(
            processor_args, processor_to_run.name))
        processor_post_message(processor_to_run, user_that_ran, post_body)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        _set_task_progress(0)
        old_file_path = adjust_path(processor_to_run.local_path)
        file_path = copy_processor_local(old_file_path)
        from processor.main import main
        os.chdir(file_path)
        if processor_args:
            main(processor_args)
        else:
            main()
        copy_processor_local(old_file_path, copy_back=True)
        if 'analyze' in processor_args:
            os.chdir(cur_path)
            update_analysis_in_db(processor_id, current_user_id)
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
        ic = ImportConfig(default_param_ic=default_param_ic)
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
            sources[idx]['original_vendor_key'] = [
                x for x in form_sources
                if x['vendor_key'] == source[vmc.vendorkey]
            ][0]['original_vendor_key']
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        matrix.set_data_sources(sources)
        msg_text = "Processor {} datasources set.".format(cur_processor.name)
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
                   new_name=False):
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
        utl.dir_check(utl.raw_path)
        if mem_file:
            new_data.seek(0)
            file_name = data_source.p[vmc.filename]
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
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.dictionary as dct
        import processor.reporting.vendormatrix as vm
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        dic = dct.Dict(data_source.p[vmc.filenamedict])
        df = pd.read_json(new_data)
        if 'index' in df.columns:
            df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        if vk == vm.plan_key:
            df = df[dctc.PCOLS]
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


def full_run_processor(processor_id, current_user_id, processor_args=None):
    try:
        _set_task_progress(0)
        if not processor_args or processor_args == 'full':
            processor_args = (
                '--api all --ftp all --dbi all --exp all --tab --analyze')
        run_processor(processor_id, current_user_id, processor_args)
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
        set_uploader_config_file(uploader_id, current_user_id)
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
                    object_level='Campaign').first()
            elif key == 'fb/adset_upload.xlsx':
                upo = UploaderObjects.query.filter_by(
                    uploader_id=uploader_to_run.id,
                    object_level='Adset').first()
            elif key == 'fb/ad_upload.xlsx':
                upo = UploaderObjects.query.filter_by(
                    uploader_id=uploader_to_run.id,
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


def run_uploader(uploader_id, current_user_id, uploader_args):
    try:
        uploader_to_run, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        post_body = ('Running {} for uploader: {}...'.format(
            uploader_args, uploader_to_run.name))
        processor_post_message(uploader_to_run, user_that_ran, post_body,
                               object_name='Uploader')
        _set_task_progress(0)
        file_path = adjust_path(uploader_to_run.local_path)
        from uploader.main import main
        os.chdir(file_path)
        error_dict = main(uploader_args)
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


def uploader_file_translation(uploader_file_name, object_level='Campaign'):
    base_config_path = 'config'
    base_create_path = os.path.join(base_config_path, 'create')
    base_fb_path = os.path.join(base_config_path, 'fb')
    file_translation = {
        'Creator': os.path.join(base_create_path, 'creator_config.xlsx'),
        'uploader_creative_files': ''}
    for name in ['Campaign', 'Adset', 'Ad', 'uploader_current_name']:
        if name == 'uploader_current_name':
            file_name = object_level.lower()
        else:
            file_name = name.lower()
        file_name = '{}_upload.xlsx'.format(file_name)
        file_translation[name] = os.path.join(base_fb_path, file_name)
    for name in ['edit_relation', 'uploader_full_relation']:
        file_name = '{}_relation.xlsx'.format(object_level.lower())
        file_translation[name] = os.path.join(base_create_path, file_name)
    for name in ['name_creator', 'upload_filter', 'match_table']:
        file_name = '{}_{}.xlsx'.format(object_level.lower(), name)
        file_translation[name] = os.path.join(base_create_path, file_name)
    return file_translation[uploader_file_name]


def get_primary_column(object_level):
    if object_level == 'Campaign':
        col = 'campaign_name'
    elif object_level == 'Adset':
        col = 'adset_name'
    elif object_level == 'Ad':
        col = 'ad_name'
    else:
        col = ''
    return col


def get_current_uploader_obj_names(uploader_id, current_user_id, cur_path,
                                   file_path, file_name, object_level):
    col = get_primary_column(object_level=object_level)
    os.chdir(cur_path)
    uploader_create_objects(uploader_id, current_user_id,
                            object_level=object_level)
    os.chdir(file_path)
    ndf = pd.read_excel(file_name)
    df = ndf[col].str.split('_', expand=True)
    df[col] = ndf[col]
    return df


def get_uploader_relation_values_from_position(rel_pos, df, vk, object_level):
    rel_pos = [int(x) for x in rel_pos]
    col = get_primary_column(object_level)
    df = df.loc[df['impacted_column_name'] == vk]
    cdf = pd.read_excel(uploader_file_translation(object_level))
    cdf = cdf[col].str.split('_', expand=True)
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
                      object_level='Campaign'):
    try:
        uploader_to_run, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        _set_task_progress(0)
        upo = UploaderObjects.query.filter_by(
            uploader_id=uploader_to_run.id,
            object_level=object_level).first()
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        file_path = adjust_path(uploader_to_run.local_path)
        os.chdir(file_path)
        file_name = uploader_file_translation(
            uploader_file_name=parameter, object_level=object_level)
        if parameter in ['uploader_current_name']:
            df = get_current_uploader_obj_names(
                uploader_id, current_user_id, cur_path, file_path, file_name,
                object_level=object_level)
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
                    rel_pos=rel_pos, df=df, vk=vk, object_level=object_level)
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


def set_uploader_config_file(uploader_id, current_user_id):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        _set_task_progress(0)
        import uploader.upload.fbapi as fbapi
        file_path = adjust_path(cur_up.local_path)
        os.chdir(file_path)
        with open(os.path.join(fbapi.config_path, 'fbconfig.json'), 'r') as f:
            config_file = json.load(f)
        config_file['act_id'] = 'act_' + cur_up.fb_account_id
        with open(os.path.join(fbapi.config_path, 'fbconfig.json'), 'w') as f:
            json.dump(config_file, f)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        return False


def write_uploader_file(uploader_id, current_user_id, new_data, parameter=None,
                        vk=None, mem_file=False, object_level='Campaign'):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        _set_task_progress(0)
        import uploader.upload.utils as utl
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        os.chdir(adjust_path(cur_up.local_path))
        file_name = uploader_file_translation(
            uploader_file_name=parameter, object_level=object_level)
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
                             object_level='Campaign'):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        up_cam = UploaderObjects.query.filter_by(
            uploader_id=cur_up.id, object_level=object_level).first()
        up_rel = UploaderRelations.query.filter_by(
            uploader_objects_id=up_cam.id).all()
        import uploader.upload.utils as utl
        os.chdir(adjust_path(cur_up.local_path))
        file_name = uploader_file_translation(
            'uploader_full_relation', object_level=object_level)
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
                             duplication_type=None):
    import uploader.upload.creator as cre
    if object_level == 'Campaign':
        if create_type == 'Media Plan':
            col_file_name = [
                'mediaplan.xlsx', '/create/campaign_name_creator.xlsx',
                '/create/campaign_relation.xlsx']
            col_new_file = [
                'create/campaign_name_creator.xlsx',
                'fb/campaign_upload.xlsx', 'fb/campaign_upload.xlsx']
            col_create_type = ['mediaplan', 'create', 'relation']
            col_column_name = [creator_column, 'campaign_name', '']
            col_overwrite = [True, True, '']
            col_filter = [file_filter, '', '']
        else:
            col_file_name = [
                '/create/campaign_name_creator.xlsx',
                '/create/campaign_relation.xlsx']
            col_new_file = [
                'fb/campaign_upload.xlsx', 'fb/campaign_upload.xlsx']
            col_create_type = ['create', 'relation']
            col_column_name = ['campaign_name', '']
            col_overwrite = [True, '']
            col_filter = ['', '']
    elif object_level == 'Adset':
        if create_type == 'Media Plan':
            col_file_name = [
                'mediaplan.xlsx', '/create/adset_name_creator.xlsx',
                '/create/adset_relation.xlsx']
            col_new_file = [
                'create/adset_name_creator.xlsx',
                'fb/adset_upload.xlsx', 'fb/adset_upload.xlsx']
            col_create_type = ['mediaplan', 'create', 'relation']
            col_column_name = [creator_column, 'adset_name', '']
            col_overwrite = [True, True, '']
            col_filter = [file_filter, '', '']
        else:
            col_file_name = ['/create/adset_name_creator.xlsx',
                             '/create/adset_relation.xlsx']
            col_new_file = ['fb/adset_upload.xlsx', 'fb/adset_upload.xlsx']
            col_create_type = ['create', 'relation']
            col_column_name = ['adset_name', '']
            col_overwrite = [True, '']
            col_filter = ['', '']
    elif object_level == 'Ad':
        if create_type == 'Media Plan':
            col_file_name = [
                'mediaplan.xlsx', '/create/ad_name_creator.xlsx',
                '/create/ad_relation.xlsx']
            col_new_file = [
                'create/ad_name_creator.xlsx',
                'fb/ad_upload.xlsx', 'fb/ad_upload.xlsx']
            col_create_type = ['mediaplan', 'create', 'relation']
            col_column_name = [creator_column, 'ad_name', '']
            col_overwrite = [True, True, '']
            col_filter = [file_filter, '', '']
        else:
            if duplication_type == 'Custom':
                dup_col_name = ('ad_name::campaign_name|adset_name::'
                                '/create/ad_upload_filter.xlsx')
            else:
                dup_col_name = 'ad_name::campaign_name|adset_name'
            col_file_name = [
                '/create/ad_name_creator.xlsx', '/fb/adset_upload.xlsx',
                '/create/ad_relation.xlsx']
            col_new_file = [
                'fb/ad_upload.xlsx', 'fb/ad_upload.xlsx',
                'fb/ad_upload.xlsx']
            col_create_type = ['create', 'duplicate', 'relation']
            col_column_name = ['ad_name', dup_col_name, '']
            col_overwrite = [True, '', '']
            col_filter = ['', '', '']
            if create_type == 'Match Table':
                col_file_name.insert(0, '/create/ad_match_table.xlsx')
                col_new_file.insert(
                    0, '/create/ad_name_creator.xlsx|'
                       '/create/ad_upload_filter.xlsx|/create/ad_relation.xlsx')
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
                            object_level='Campaign'):
    try:
        cur_up, user_that_ran = get_uploader_and_user_from_id(
            uploader_id=uploader_id, current_user_id=current_user_id)
        up_obj = UploaderObjects.query.filter_by(
            uploader_id=cur_up.id, object_level=object_level).first()
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
            duplication_type=up_obj.duplication_type)
        df = pd.DataFrame(new_dict)
        os.chdir(adjust_path(cur_up.local_path))
        file_name = uploader_file_translation('Creator')
        utl.write_df(df, file_name)
        os.chdir(cur_path)
        set_object_relation_file(uploader_id, current_user_id,
                                 object_level=object_level)
        os.chdir(cur_path)
        run_uploader(uploader_id, current_user_id, uploader_args='--create')
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
                                       object_level='Campaign'):
    try:
        uploader_create_objects(uploader_id, current_user_id,
                                object_level=object_level)
        if object_level == 'Campaign':
            uploader_args = '--api fb --upload c'
        elif object_level == 'Adset':
            uploader_args = '--api fb --upload as'
        elif object_level == 'Ad':
            uploader_args = '--api fb --upload ad'
        else:
            uploader_args = ''
        run_uploader(uploader_id, current_user_id, uploader_args=uploader_args)
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
            processor_id, current_user_id)
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
        for config_type in [('twitter', 'twconfig.json'),
                            ('dc', 'dcapi.json'), ('dv', 'dvapi.json')]:
            set_processor_config_file(
                processor_id=processor_id, current_user_id=current_user_id,
                config_type=config_type[0], config_file_name=config_type[1])
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
                        replace(' ', '').replace('_', '').replace('|', ''))
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
        result = run_processor(processor_id, current_user_id,
                               '--api all --ftp all --dbi all --exp all --tab')
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


def get_processor_total_metrics(processor_id, current_user_id):
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
                            dimensions=None, metrics=None):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.utils as utl
        import processor.reporting.export as export
        if not cur_processor.local_path:
            _set_task_progress(100)
            return [pd.DataFrame({x: [] for x in dimensions + metrics})]
        os.chdir(adjust_path(cur_processor.local_path))
        _set_task_progress(15)
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
        _set_task_progress(30)
        command = """SELECT {0},{1}
            FROM lqadb.event
            FULL JOIN lqadb.fullplacement ON event.fullplacementid = fullplacement.fullplacementid
            FULL JOIN lqadb.plan ON plan.fullplacementid = fullplacement.fullplacementid
            LEFT JOIN lqadb.vendor ON fullplacement.vendorid = vendor.vendorid
            LEFT JOIN lqadb.campaign ON fullplacement.campaignid = campaign.campaignid
            LEFT JOIN lqadb.country ON fullplacement.countryid = country.countryid
            LEFT JOIN lqadb.product ON campaign.productid = product.productid
            LEFT JOIN lqadb.targeting ON fullplacement.targetingid = targeting.targetingid
            LEFT JOIN lqadb.creative ON fullplacement.creativeid = creative.creativeid
            WHERE fullplacement.uploadid = '{2}'
            GROUP BY {0}
        """.format(dimensions, metric_sql, up_id)
        db_class = export.DB()
        db_class.input_config('dbconfig.json')
        db_class.connect()
        _set_task_progress(50)
        db_class.cursor.execute(command)
        data = db_class.cursor.fetchall()
        _set_task_progress(70)
        columns = [i[0] for i in db_class.cursor.description]
        df = pd.DataFrame(data=data, columns=columns)
        _set_task_progress(90)
        df = utl.data_to_type(df, float_col=metrics)
        if 'eventdate' in df.columns:
            df = utl.data_to_type(df, str_col=['eventdate'])
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


def build_processor_analysis_email(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        import processor.reporting.analyze as az
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.dictcolumns as dctc
        cur_processor = Processor.query.get(processor_id)
        analysis = cur_processor.processor_analysis.all()
        text_body = []
        topline_analysis = [x for x in analysis
                            if x.key == az.Analyze.topline_col]
        text_body.append({'message': 'TOPLINE\n\n', 'tab': 0})
        for topline in topline_analysis:
            df = pd.DataFrame(topline.data)
            if not df.empty:
                text_body.append({'message': '-  {}\n'.format(topline.message),
                                  'data': df,
                                  'tab': 1})
        delivery_analysis = [x for x in analysis if x.key in
                             [az.Analyze.delivery_col,
                              az.Analyze.delivery_comp_col]]
        text_body.append({'message': 'DELIVERY\n\n', 'tab': 0})
        for delivery in delivery_analysis:
            df = pd.DataFrame(delivery.data)
            if not df.empty:
                text_body.append({'message': '-  {}\n'.format(delivery.message),
                                  'data': pd.DataFrame(delivery.data),
                                  'tab': 1})
        kpi_analysis = [x for x in analysis
                        if x.key == az.Analyze.kpi_col]
        kpis = set(x.parameter for x in kpi_analysis
                   if x.parameter not in ['0', 'nan'])
        text_body.append({'message': 'KPI ANALYSIS\n\n', 'tab': 0})
        for kpi in kpis:
            text_body.append({'message': '-  {}\n\n'.format(kpi), 'tab': 1})
            cur_analysis = [x for x in kpi_analysis if x.parameter == kpi]
            text_body.append({'message': '-  {}\n\n'.format('Partner'),
                              'tab': 2})
            par_analysis = [x for x in cur_analysis if x.split_col == dctc.VEN]
            for a in par_analysis:
                text_body.append({'message': '-  {}\n'.format(a.message),
                                  'data': pd.DataFrame(a.data), 'tab': 3})
                partners = pd.DataFrame(a.data)[dctc.VEN].to_list()
                for p in partners:
                    text_body.append({'message': '-  {}\n\n'.format(p),
                                      'tab': 3})
                    ind_par_anlaysis = [x for x in cur_analysis
                                        if x.filter_val == p
                                        and x.parameter_2 == a.parameter_2]
                    for ind_par in ind_par_anlaysis:
                        text_body.append(
                            {'message': '-  {}\n'.format(ind_par.message),
                             'tab': 4})
            date_analysis = [x for x in cur_analysis if x.split_col == vmc.date]
            text_body.append({'message': '-  {}\n\n'.format('Date'), 'tab': 2})
            for a in date_analysis:
                text_body.append({'message': '-  {}\n'.format(a.message),
                                  'data': pd.DataFrame(a.data), 'tab': 3})
        qa_analysis = [
            x for x in analysis if x.key in
            [az.Analyze.unknown_col, az.Analyze.raw_file_update_col]]
        text_body.append({'message': 'REPORTING QA\n\n', 'tab': 0})
        for qa in qa_analysis:
            df = pd.DataFrame(qa.data)
            if not df.empty:
                text_body.append({'message': '-  {}\n'.format(qa.message),
                                  'data': pd.DataFrame(qa.data), 'tab': 1})
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return []
