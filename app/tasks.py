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
    ProcessorDatasources, Uploader, Account, RateCard, Rates, Conversion

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
            time.sleep(5)
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


def processor_post_message(proc, usr, text):
    try:
        msg = Message(author=usr, recipient=usr, body=text)
        db.session.add(msg)
        usr.add_notification('unread_message_count', usr.new_messages())
        post = Post(body=text, author=usr, processor_id=proc.id)
        db.session.add(post)
        db.session.commit()
        usr.add_notification(
            'task_complete', {'text': text,
                              'timestamp': post.timestamp.isoformat(),
                              'post_id': post.id})
        db.session.commit()
    except:
        db.session.rollback()
        processor_post_message(proc, usr, text)


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
        processor_to_run = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        post_body = ('Running {} for processor: {}...'.format(
            processor_args, processor_to_run.name))
        processor_post_message(processor_to_run, user_that_ran, post_body)
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
        msg_text = ("{} finished running.".format(processor_to_run.name))
        processor_post_message(processor_to_run, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


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
        cur_processor = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
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
                (dctc.PRN, cur_processor.campaign.product.name)]:
        idx = dcc.df[dcc.df[dctc.DICT_COL_NAME] == col[0]].index[0]
        if dcc.df.loc[idx, dctc.DICT_COL_VALUE] == 'None':
            dcc.df.loc[idx, dctc.DICT_COL_VALUE] = col[1]
    dcc.write(dcc.df, dctc.filename_con_config)


def create_processor(processor_id, current_user_id, base_path):
    try:
        new_processor = Processor.query.get(processor_id)
        user_create = User.query.get(current_user_id)
        old_path = adjust_path(base_path)
        new_path = adjust_path(new_processor.local_path)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        copy_tree_no_overwrite(old_path, new_path)
        set_initial_constant_file(new_processor)
        msg_text = "Processor {} was created.".format(new_processor.name)
        processor_post_message(new_processor, user_create, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def add_data_sources_from_processor(cur_processor, data_sources, attempt=1):
    for source in data_sources:
        proc_import = ProcessorDatasources()
        proc_import.set_from_processor(source, cur_processor)
        db.session.add(proc_import)
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
        add_data_sources_from_processor(cur_processor, data_sources)
        msg_text = "Processor {} imports refreshed.".format(cur_processor.name)
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
        db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


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
        msg_text = "Processor {} imports set.".format(cur_processor.name)
        processor_post_message(cur_processor, user_that_ran, msg_text)
        get_processor_sources(processor_id, current_user_id)
        _set_task_progress(100)
        db.session.commit()
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


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


def get_data_tables(processor_id, current_user_id, parameter):
    try:
        cur_processor = Processor.query.get(processor_id)
        file_name = os.path.join(adjust_path(cur_processor.local_path),
                                 'Raw Data Output.csv')
        tables = pd.read_csv(file_name)
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
        parameter = param_translate[parameter]
        if parameter:
            tables = [tables.groupby(parameter)[metrics].sum()]
        else:
            tables = [tables]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


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
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


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
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_raw_data(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        tables = [data_source.get_raw_df()]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


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
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def write_dictionary(processor_id, current_user_id, new_data, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.dictionary as dct
        import processor.reporting.vendormatrix as vm
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        dic = dct.Dict(data_source.p[vmc.filenamedict])
        df = pd.read_json(new_data)
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
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


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


def write_vendormatrix(processor_id, current_user_id, new_data):
    try:
        cur_processor = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        import processor.reporting.vmcolumns as vmc
        import processor.reporting.vendormatrix as vm
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        df = pd.read_json(new_data)
        df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        rule_cols = [x for x in df.columns if x not in vmc.vmkeys]
        df = df[[vmc.vendorkey] + vmc.vmkeys + rule_cols]
        matrix.vm_df = df
        matrix.write()
        msg_text = ('{} processor vendormatrix was updated.'
                    ''.format(cur_processor.name))
        processor_post_message(cur_processor, user_that_ran, msg_text)
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


def write_constant_dict(processor_id, current_user_id, new_data):
    try:
        cur_processor = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        dcc = dct.DictConstantConfig(None)
        df = pd.read_json(new_data)
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


def get_relational_config(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        rc = dct.RelationalConfig()
        rc.read(dctc.filename_rel_config)
        _set_task_progress(100)
        return [rc.df]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def write_relational_config(processor_id, current_user_id, new_data):
    try:
        cur_processor = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        import processor.reporting.dictionary as dct
        import processor.reporting.dictcolumns as dctc
        os.chdir(adjust_path(cur_processor.local_path))
        rc = dct.RelationalConfig()
        df = pd.read_json(new_data)
        df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        df = df[[dctc.RK, dctc.FN, dctc.KEY, dctc.DEP, dctc.AUTO]]
        rc.write(df, dctc.filename_rel_config)
        msg_text = ('{} processor constant dict was updated.'
                    ''.format(cur_processor.name))
        processor_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def full_run_processor(processor_id, current_user_id, processor_args):
    try:
        _set_task_progress(0)
        run_processor(processor_id, current_user_id,
                      '--api all --ftp all --dbi all --exp all --tab')
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
        cur_processor = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
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
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


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


def uploader_post_message(uploader, usr, text):
    try:
        msg = Message(author=usr, recipient=usr, body=text)
        db.session.add(msg)
        usr.add_notification('unread_message_count', usr.new_messages())
        post = Post(body=text, author=usr, uploader_id=uploader.id)
        db.session.add(post)
        db.session.commit()
        usr.add_notification(
            'task_complete', {'text': text,
                              'timestamp': post.timestamp.isoformat(),
                              'post_id': post.id})
        db.session.commit()
    except:
        db.session.rollback()
        uploader_post_message(uploader, usr, text)


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
        uploader_post_message(new_uploader, user_create, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())


def set_processor_values(processor_id, current_user_id, form_sources, table):
    cur_processor = Processor.query.get(processor_id)
    old_items = table.query.filter_by(
        processor_id=cur_processor.id).all()
    user_that_ran = User.query.get(current_user_id)
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
