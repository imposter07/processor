import os
import re
import sys
import json
import yaml
import time
import copy
import random
import shutil
import itertools
import pandas as pd
import numpy as np
import datetime as dt
from functools import wraps
import app.utils as app_utl
from datetime import datetime
from flask import render_template, current_app
from rq import get_current_job
from app import create_app, db
from app.email import send_email
from sqlalchemy import or_, and_
from app.models import User, Post, Task, Processor, \
    ProcessorDatasources, Uploader, Account, RateCard, Rates, Conversion, \
    TaskScheduler, Requests, UploaderObjects, UploaderRelations, \
    ProcessorAnalysis, Project, ProjectNumberMax, Client, Product, Campaign, \
    Tutorial, TutorialStage, Walkthrough, WalkthroughSlide, Plan, Sow, Notes, \
    ProcessorReports, Partner, PlanRule, Brandtracker, \
    PartnerPlacements, Rfp, RfpFile, Specs, Contacts, PlanPhase, \
    PlanEffectiveness, InsertionOrder
import app.plan.routes as plan_routes
import app.main.routes as main_routes
import processor.reporting.calc as cal
import processor.reporting.utils as utl
import processor.reporting.export as exp
import processor.reporting.analyze as az
import processor.reporting.vmcolumns as vmc
import processor.reporting.expcolumns as exc
import processor.reporting.dictionary as dct
import processor.reporting.vendormatrix as vm
import processor.reporting.dictcolumns as dctc
import processor.reporting.importhandler as ih
import processor.reporting.gsapi as gsapi
import processor.reporting.awss3 as awss3
from processor.reporting.vendormatrix import full_placement_creation
import processor.reporting.models as prc_model
import uploader.upload.utils as u_utl
import uploader.upload.creator as cre

app = current_app
if not app:
    app = create_app()
    app.app_context().push()


def _set_task_progress(progress, total_time=0.0, route_function=None):
    job = get_current_job()
    if job:
        job_id = job.get_id()
        job.meta['progress'] = progress
        job.save_meta()
        task = db.session.get(Task, job_id)
        if task:
            cu = task.user
            if cu:
                msg = {'task_id': job_id, 'progress': progress}
                cu.add_notification('task_progress', msg)
                msg['task'] = task.name
                msg['route'] = str(route_function)
                app.logger.info(msg)
            str_func = str(route_function)
            is_original_route = route_function and task.name[1:] in str_func
            is_original_route = is_original_route or not route_function
            if progress >= 100 and is_original_route:
                app.logger.info('{} Completed'.format(task.name))
                task.complete = True
                if total_time:
                    task.total_time = total_time
            db.session.commit()


def serialize_arg(arg):
    try:
        return json.dumps(arg)
    except TypeError as e:
        return json.dumps({'Non-serializable argument': 'TypeError'})


def uploader_run_error(args):
    uploader_id = args[0]
    current_user_id = args[1]
    cur_up = db.session.get(Uploader, uploader_id)
    cur_user = db.session.get(User, current_user_id)
    obj = Uploader.__name__
    msg_text = ("{} run failed.".format(cur_up.name))
    app_utl.object_post_message(cur_up, cur_user, msg_text, object_name=obj)


def error_handler(route_function):
    @wraps(route_function)
    def decorated_function(*args, **kwargs):
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        try:
            start = time.time()
            _set_task_progress(50)
            json_args = json.dumps([serialize_arg(arg) for arg in args])
            log_msg = 'Task: {}. Args {}.'.format(route_function, json_args)
            app.logger.info(log_msg)
            result = route_function(*args, **kwargs)
            end = time.time()
            total_time = end - start
            _set_task_progress(100, total_time=total_time,
                               route_function=route_function)
            os.chdir(cur_path)
            return result
        except:
            args = json.dumps([serialize_arg(arg) for arg in args])
            msg = 'Unhandled exception {}'.format(json.dumps(args))
            _set_task_progress(100, route_function=route_function)
            app.logger.error(msg, exc_info=sys.exc_info())
            os.chdir(cur_path)
            if 'run_uploader' in str(route_function):
                uploader_run_error(args)
            return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]

    return decorated_function


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
    if os.name == 'nt':
        for x in [['/mnt/c', 'C:'], ['/mnt/c', 'c:']]:
            if path[:len(x[0])] == x[0]:
                path = x[1] + path[len(x[0]):]
    return path


def copy_file(old_file, new_file, attempt=1, max_attempts=100):
    try:
        shutil.copy(old_file, new_file)
    except PermissionError as e:
        app.logger.warning('Could not copy {}: '
                           '{}'.format(old_file, e))
    except OSError as e:
        attempt += 1
        if attempt > max_attempts:
            app.logger.warning(
                'Exceeded after {} attempts not copying {} '
                '{}'.format(max_attempts, old_file, e))
        else:
            app.logger.warning('Attempt {}: could not copy {} due to OSError '
                               'retrying in 60s: {}'.format(attempt, old_file,
                                                            e))
            time.sleep(60)
            copy_file(old_file, new_file, attempt=attempt,
                      max_attempts=max_attempts)


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
        user_that_ran = db.session.get(User, current_user_id)
        cur_processor = db.session.get(Processor, processor_id)
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


@error_handler
def update_base_plan(vendor_name, df, current_user_id):
    plan_name = 'Base Plan'
    cur_plan = Plan.query.filter_by(name=plan_name).first()
    if not cur_plan:
        cur_plan = Plan(name=plan_name)
        db.session.add(cur_plan)
        db.session.commit()
    cur_phase = PlanPhase.query.filter_by(
        name=plan_name, plan_id=cur_plan.id).first()
    if not cur_phase:
        cur_phase = PlanPhase(name=plan_name, plan_id=cur_plan.id)
        db.session.add(cur_phase)
        db.session.commit()
    p = Partner.query.filter_by(plan_phase_id=cur_phase.id,
                                name=vendor_name).first()
    if not p:
        p = Partner(name=vendor_name, plan_phase_id=cur_phase.id)
        db.session.add(p)
        db.session.commit()
    exclude_values = [0, 'None', None, '0', '0.0', 0.0]
    max_sums = {}
    df = PartnerPlacements.translate_plan_names(df)
    cols = [x for x in df.columns if x not in [vmc.impressions]]
    for col in cols:
        mask = ~df[col].isin(exclude_values)
        tdf = df[mask]
        tdf = tdf.groupby(col)[vmc.impressions].sum().reset_index()
        if not tdf.empty:
            max_row = tdf.loc[tdf[vmc.impressions].idxmax()]
            max_sums[col] = max_row[col]
    app_utl.set_db_values(
        p.id, current_user_id, form_sources=[max_sums],
        table=PartnerPlacements, parent_model=Partner)


@error_handler
def update_total_db_analysis(processor_id, current_user_id, df, ven_col,
                             cur_path, col, batch_size=100):
    ven_list = df[0].groupby(ven_col)[vmc.impressions].sum()
    ven_list = ven_list[ven_list > 100000].index.to_list()
    for idx, vendor in enumerate(ven_list):
        os.chdir(cur_path)
        tdf = df[0][df[0][ven_col] == vendor]
        t_filter_dict = [{ven_col: [vendor]}]
        update_analysis_in_db_reporting_cache(
            processor_id, current_user_id, df=tdf,
            dimensions=col, metrics=['kpi'],
            filter_dict=t_filter_dict, commit=False)
        update_base_plan(vendor, tdf, current_user_id)
        if idx % batch_size == 0 or idx == len(ven_list):
            app.logger.info('Committing: #{}'.format(idx))
            db.session.commit()


@error_handler
def update_cached_data_in_processor_run(processor_id, current_user_id):
    ven_col = prc_model.Vendor.vendorname.name
    dim_list = [
        [ven_col], ['countryname'], ['kpiname'], ['environmentname'],
        ['productname'], ['eventdate'], ['campaignname'], ['clientname'],
        ['vendorname', 'vendortypename']]
    filter_dicts = [[]]
    cols = []
    if processor_id == 23:
        today = dt.datetime.today()
        thirty = today - dt.timedelta(days=30)
        today = today.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        thirty = thirty.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        filter_dict = [{'eventdate': [thirty, today]}]
        filter_dicts.append(filter_dict)
        dims = [[x] for x in
                ['placementdescriptionname', 'packagedescriptionname',
                 'mediachannelname', 'targetingbucketname',
                 'creativelineitemname', 'copyname']]
        dim_list.extend(dims)
        cols = PartnerPlacements.get_cols_for_db()
        dim_list.append(cols)
    cur_path = adjust_path(os.path.abspath(os.getcwd()))
    for col in dim_list:
        app.logger.info('Getting db col: {}'.format(col))
        os.chdir(cur_path)
        for filter_dict in filter_dicts:
            df = get_data_tables_from_db(
                processor_id, current_user_id, dimensions=col,
                metrics=['kpi'], filter_dict=filter_dict, use_cache=False)
            if col == cols and not filter_dict:
                update_total_db_analysis(processor_id, current_user_id,
                                         df, ven_col, cur_path, col)


def run_processor(processor_id, current_user_id, run_args):
    try:
        processor_to_run, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
        post_body = ('Running {} for processor: {}...'.format(
            run_args, processor_to_run.name))
        app_utl.object_post_message(processor_to_run, user_that_ran, post_body)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        _set_task_progress(0)
        old_file_path = adjust_path(processor_to_run.local_path)
        file_path = copy_processor_local(old_file_path)
        from processor.main import main
        if not file_path:
            return False
        os.chdir(file_path)
        if run_args:
            main(run_args)
        else:
            main()
        copy_processor_local(old_file_path, copy_back=True)
        if 'analyze' in run_args:
            task_functions = [get_processor_sources, update_analysis_in_db,
                              update_report_in_db, update_automatic_requests]
            for task_function in task_functions:
                os.chdir(cur_path)
                task_function(processor_id, current_user_id)
        if 'exp' in run_args:
            os.chdir(cur_path)
            update_cached_data_in_processor_run(processor_id, current_user_id)
            update_all_notes_table(processor_id, current_user_id)
            if processor_id == 23:
                task_functions = [
                    get_project_numbers, get_post_mortems, get_contact_numbers,
                    get_rate_cards, get_all_tutorials_from_google_doc]
                for task_function in task_functions:
                    os.chdir(cur_path)
                    task_function(processor_id, current_user_id)
        msg_text = ("{} finished running.".format(processor_to_run.name))
        app_utl.object_post_message(proc=processor_to_run, usr=user_that_ran,
                                    text=msg_text, run_complete=True)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        processor_to_run = db.session.get(Processor, processor_id)
        user_that_ran = db.session.get(User, current_user_id)
        msg_text = ("{} run failed.".format(processor_to_run.name))
        app_utl.object_post_message(processor_to_run, user_that_ran, msg_text)
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
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
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
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def set_initial_constant_file(cur_processor):
    proc_path = cur_processor.local_path
    if not proc_path:
        return False
    os.chdir(adjust_path(proc_path))
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
    return True


@error_handler
def create_processor(processor_id, current_user_id, base_path):
    new_processor, user_create = app_utl.get_obj_user(
        object_id=processor_id, current_user_id=current_user_id)
    old_path = adjust_path(base_path)
    new_path = adjust_path(new_processor.local_path)
    if not new_path:
        return False
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    copy_tree_no_overwrite(old_path, new_path)
    set_initial_constant_file(new_processor)
    msg_text = "Processor {} was created.".format(new_processor.name)
    app_utl.object_post_message(new_processor, user_create, msg_text)
    return True


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


@error_handler
def get_processor_sources(processor_id, current_user_id):
    cur_processor, user_that_ran = app_utl.get_obj_user(
        object_id=processor_id, current_user_id=current_user_id)
    os.chdir('processor')
    default_param_ic = vm.ImportConfig(matrix=True)
    processor_path = adjust_path(cur_processor.local_path)
    os.chdir(processor_path)
    matrix = vm.VendorMatrix()
    data_sources = matrix.get_all_data_sources(
        default_param=default_param_ic)
    add_data_sources_from_processor(cur_processor, data_sources)
    msg_text = "Processor {} sources refreshed.".format(cur_processor.name)
    app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
    df = pd.DataFrame([{'Result': msg_text}])
    return [df]


def set_processor_imports(processor_id, current_user_id, form_imports,
                          set_in_db=True):
    try:
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
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
        if not processor_path:
            return False
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
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
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
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
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
        old_sources = ProcessorDatasources.query.filter_by(
            processor_id=cur_processor.id).all()
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
        proc_path = cur_processor.local_path
        if not proc_path:
            return False
        os.chdir(adjust_path(proc_path))
        matrix = vm.VendorMatrix()
        matrix.set_data_sources(sources)
        msg_text = "Processor {} datasources set.".format(cur_processor.name)
        if len(form_sources) == 1:
            name = form_sources[0]['vendor_key']
            msg_text += "  {} was saved.".format(name)
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_file_in_memory(tables, file_name='raw.csv'):
    import io
    import zipfile
    file_type = os.path.splitext(file_name)[1]
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode='w') as f:
        data = zipfile.ZipInfo(file_name)
        data.date_time = time.localtime(time.time())[:6]
        data.compress_type = zipfile.ZIP_DEFLATED
        if file_type == '.pdf' or file_type == '.xls' or file_type == '.xlsx':
            f.write(tables, arcname=file_name)
        else:
            f.writestr(data, data=tables.to_csv())
    mem.seek(0)
    return mem


def get_data_tables(processor_id, current_user_id, parameter=None,
                    dimensions=None, metrics=None):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        if not cur_processor.local_path:
            _set_task_progress(100)
            return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]
        file_name = os.path.join(adjust_path(cur_processor.local_path),
                                 vmc.output_file)
        _set_task_progress(15)
        tables = utl.import_read_csv(file_name)
        tables = tables.fillna('None')
        _set_task_progress(30)
        if not metrics:
            metrics = [vmc.impressions, vmc.clicks, vmc.cost, dctc.PNC, cal.NCF,
                       vmc.AD_COST, cal.TOTAL_COST]
        base_param = [dctc.CAM, dctc.VEN]
        param_translate = {}
        params = [vmc.output_file, vmc.vendorkey, dctc.VEN, dctc.TAR, dctc.CRE,
                  dctc.COP, dctc.BM, dctc.SRV]
        for param in params:
            key = param.replace('mp', '').replace(' ', '').replace('.csv', '')
            cols = base_param
            if param not in base_param:
                cols = cols + [param]
            if param == dctc.BM:
                cols = cols + [dctc.BR, dctc.PD]
            if param == dctc.SRV:
                cols = cols + [dctc.AM, dctc.AR]
            if param == vmc.output_file:
                cols = []
            param_translate[key] = cols
        if parameter:
            parameter = param_translate[parameter]
        elif dimensions:
            parameter = dimensions
        else:
            parameter = []
        if parameter and not tables.empty:
            tables = tables.groupby(parameter)[metrics].sum()
            tables = tables.reset_index()
            tables = app_utl.LiquidTable(df=tables,
                                         table_name='modal-body-table')
            tables = [tables.table_dict]
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


def get_change_dict_order(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        if not os.path.exists(data_source.p[vmc.filename_true]):
            if vmc.api_raw_key in data_source.key:
                msg = 'NO DATA - ADD THE RAW FILE.'
            else:
                msg = 'NO DATA - CHECK THE API CONNECTION AND RUN PROCESSOR.'
            _set_task_progress(100)
            return [pd.DataFrame([{'Result': msg}]), [], []]
        tdf = data_source.get_dict_order_df(include_index=False,
                                            include_full_name=True)
        rc = dct.RelationalConfig()
        rc.read(dctc.filename_rel_config)
        relational_auto = rc.get_auto_cols()
        relational_sep = rc.get_auto_delims()
        relational_dict = [relational_auto, relational_sep]
        dependants = [col for sublist in rc.rc[dctc.DEP].values() for col in
                      sublist]
        autos = rc.get_auto_cols_list()
        dict_cols = [col for col in dctc.COLS if
                     col not in [dctc.FPN, dctc.PN, dctc.MN, dctc.MT]
                     + dependants or col in autos]
        sample_size = 5
        if len(tdf.index) > sample_size:
            tdf = tdf.sample(sample_size)
        tdf = tdf.T
        tables = [tdf, dict_cols, relational_dict]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}]), []]


def delete_dict(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
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


@error_handler
def get_raw_data(processor_id, current_user_id, vk=None, parameter=None):
    cur_processor = db.session.get(Processor, processor_id)
    _set_task_progress(20)
    os.chdir(adjust_path(cur_processor.local_path))
    _set_task_progress(40)
    if vk:
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
        tables = data_source.get_raw_df()
    else:
        file_name = os.path.join(adjust_path(cur_processor.local_path),
                                 vmc.output_file)
        tables = utl.import_read_csv(file_name)
        tables = tables.fillna('None')
    _set_task_progress(60)
    if parameter:
        tables = get_file_in_memory(tables)
    tables = [tables]
    return tables


@error_handler
def write_raw_data(processor_id, current_user_id, new_data, vk, mem_file=False,
                   new_name=False, file_type='.csv'):
    cur_processor, user_that_ran = app_utl.get_obj_user(
        object_id=processor_id, current_user_id=current_user_id)
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
        new_value = data_source.p[vmc.filename].replace(
            current_file_type, file_type)
        matrix.vm_change(index=idx, col=vmc.filename, new_value=new_value)
        matrix.write()
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source(vk)
    utl.dir_check(utl.raw_path)
    if mem_file:
        tmp_suffix = ''
        if not new_name:
            tmp_suffix = 'TMP'
        new_data.seek(0)
        file_name = data_source.p[vmc.filename_true].replace(
            file_type, '{}{}'.format(tmp_suffix, file_type))
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(new_data, f, length=131072)
    else:
        df = pd.read_json(new_data)
        df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        data_source.write(df)
    msg_text = ('{} processor raw_data: {} was updated.'
                ''.format(cur_processor.name, vk))
    app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
    os.chdir(cur_path)
    get_processor_sources(processor_id, current_user_id)


def get_dictionary(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        if not cur_processor.local_path:
            _set_task_progress(100)
            return [pd.DataFrame([{'Result': 'FINISH PROCESSOR CREATION'}])]
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


def write_dictionary(processor_id, current_user_id, new_data, vk,
                     object_level=None):
    try:
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
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
            app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
            _set_task_progress(100)
            return True
        if 'index' in df.columns:
            df = df.drop('index', axis=1)
        df = df.replace('NaN', '')
        if vk == vm.plan_key:
            if object_level == 'Page':
                plan_dimensions = data_source.p[vmc.fullplacename]
                df[dctc.FPN] = df[plan_dimensions].agg('_'.join, axis=1)
            add_cols = [x for x in df.columns if x not in dctc.PCOLS]
            df = df[dctc.PCOLS + add_cols]
        else:
            df = df[dctc.COLS]
        dic.write(df)
        msg_text = ('{} processor dictionary: {} was updated.'
                    ''.format(cur_processor.name, vk))
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())


def write_dictionary_order(processor_id, current_user_id, new_data, vk):
    try:
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        _set_task_progress(0)
        df = pd.read_json(new_data)
        if df.empty:
            dict_order = ''
        else:
            dict_order = df['Auto Dictionary Order'].drop([0, 1]).to_list()
            dict_order = list(utl.rename_duplicates(dict_order))
            dict_order = '|'.join(dict_order)
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        matrix.vm_change_on_key(vk, vmc.autodicord, dict_order)
        matrix.write()
        msg_text = ('{} processor auto dictionary order: {} was updated.'
                    ''.format(cur_processor.name, vk))
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
        os.chdir(cur_path)
        get_processor_sources(processor_id, current_user_id)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())


def get_translation_dict(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
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
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
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
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
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
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_relational_config(processor_id, current_user_id, parameter=None):
    try:
        cur_processor = Processor.query.get(processor_id)
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
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())


def get_import_config_file(processor_id, current_user_id, vk):
    try:
        cur_processor = Processor.query.get(processor_id)
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        ic = vm.ImportConfig()
        data_source = matrix.get_data_source(vk=vk)
        f_lib = ic.set_config_file_lib(data_source.params[ic.config_file])
        config_file = ic.load_file(data_source.params[ic.config_file], f_lib)
        api_type = vk.split('_')[1]
        if api_type == vmc.api_aw_key:
            df = pd.DataFrame(config_file)
        elif api_type == vmc.api_raw_key:
            df = pd.DataFrame({'values': ['RAW FILES DO NOT HAVE CONFIG']})
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
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
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
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
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
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
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
            app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
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


@error_handler
def get_logfile(processor_id, current_user_id, object_type=Processor):
    cur_obj = db.session.get(object_type, processor_id)
    file_path = os.path.join(adjust_path(cur_obj.local_path), 'logfile.log')
    with open(file_path, 'r') as f:
        log_file = f.read()
    log_file = json.dumps(log_file.split('\n'))
    return [log_file]


@error_handler
def get_rate_card(processor_id, current_user_id, vk):
    if vk == '__None' or vk == '':
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
    return [df]


@error_handler
def write_rate_card(processor_id, current_user_id, new_data, vk,
                    object_type=Processor.__name__):
    object_type = Plan if object_type == Plan.__name__ else Processor
    cur_proc, cur_user = app_utl.get_obj_user(
        object_id=processor_id, current_user_id=current_user_id,
        db_model=object_type)
    rate_card_name = '{}|{}'.format(cur_proc.name, cur_user.username)
    rate_card = RateCard.query.filter_by(name=rate_card_name).first()
    if not rate_card:
        rate_card = RateCard(name=rate_card_name, owner_id=current_user_id)
        db.session.add(rate_card)
        db.session.commit()
        rate_card = RateCard.query.filter_by(name=rate_card_name).first()
    data = json.loads(new_data)
    app_utl.set_db_values(rate_card.id, current_user_id, data,
                          table=Rates, parent_model=RateCard,
                          post_model=cur_proc)
    if cur_proc.rate_card_id != rate_card.id:
        cur_proc.rate_card_id = rate_card.id
        db.session.commit()


@error_handler
def create_uploader(uploader_id, current_user_id, base_path):
    new_uploader = db.session.get(Uploader, uploader_id)
    user_create = db.session.get(User, current_user_id)
    cur_path = adjust_path(os.path.abspath(os.getcwd()))
    old_path = adjust_path(base_path)
    new_path = adjust_path(new_uploader.local_path)
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    copy_tree_no_overwrite(old_path, new_path)
    msg_text = "Uploader was created."
    app_utl.object_post_message(new_uploader, user_create, msg_text,
                                object_name='Uploader')
    set_uploader_config_files(uploader_id, current_user_id)
    os.chdir(cur_path)
    save_task = '.{}'.format(save_media_plan.__name__)
    for x in range(10):
        if new_uploader.get_task_in_progress(save_task):
            time.sleep(1)
        else:
            uploader_add_plan_costs(uploader_id, current_user_id)
            break
    return True


def get_uploader_and_user_from_id(uploader_id, current_user_id):
    uploader_to_run = db.session.get(Uploader, uploader_id)
    user_that_ran = db.session.get(User, current_user_id)
    return uploader_to_run, user_that_ran


@error_handler
def parse_uploader_error_dict(uploader_id, current_user_id, error_dict):
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


@error_handler
def run_uploader(uploader_id, current_user_id, run_args):
    uploader_to_run, user_that_ran = get_uploader_and_user_from_id(
        uploader_id=uploader_id, current_user_id=current_user_id)
    post_body = ('Running {} for uploader: {}...'.format(
        run_args, uploader_to_run.name))
    app_utl.object_post_message(uploader_to_run, user_that_ran, post_body,
                                object_name='Uploader')
    file_path = adjust_path(uploader_to_run.local_path)
    from uploader.main import main
    os.chdir(file_path)
    error_dict = main(run_args)
    parse_uploader_error_dict(uploader_id, current_user_id, error_dict)
    msg_text = ("{} finished running.".format(uploader_to_run.name))
    app_utl.object_post_message(proc=uploader_to_run, usr=user_that_ran,
                                text=msg_text, run_complete=True,
                                object_name='Uploader')
    return True


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
        'uploader_creative_files': '', 'getLog': ''}
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


def get_spend_column(object_level, uploader_type='Facebook'):
    if uploader_type == 'Facebook':
        if object_level == 'Campaign':
            col = 'campaign_spend_cap'
        elif object_level == 'Adset':
            col = 'adset_budget_value'
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
    int_list = [x for x in cdf.columns if isinstance(x, int)]
    if not int_list:
        app.logger.warning('Could not split name.')
        return df
    max_split = max(int_list)
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
    cdf['column_name'] = '|'.join([col for _ in rel_pos])
    cdf['impacted_column_name'] = vk
    if vk in ['campaign_name', 'adset_name', 'ad_name']:
        impacted_new_value = cdf['column_value'].str.replace('|', '_')
    else:
        impacted_new_value = ''
    cdf['impacted_column_new_value'] = impacted_new_value
    df = pd.concat([df, cdf], ignore_index=True, sort=False)
    df['position'] = '|'.join([str(x) for x in rel_pos])
    return df


@error_handler
def get_uploader_file(uploader_id, current_user_id, parameter=None, vk=None,
                      object_level='Campaign', uploader_type='Facebook'):
    uploader_to_run, user_that_ran = get_uploader_and_user_from_id(
        uploader_id=uploader_id, current_user_id=current_user_id)
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
    elif parameter in ['getLog']:
        df = get_logfile(uploader_id, current_user_id, Uploader)
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
    return [df]


@error_handler
def set_uploader_config_files(uploader_id, current_user_id):
    import uploader.upload.fbapi as fbapi
    import uploader.upload.awapi as awapi
    import uploader.upload.dcapi as dcapi
    new_uploader = db.session.get(Uploader, uploader_id)
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
    return True


@error_handler
def set_uploader_config_file(uploader_id, current_user_id, id_val=None,
                             config_file_path=None, file_name=None,
                             file_type=None, file_key=None,
                             id_prefix=None, nested_key=None):
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
    return True


@error_handler
def write_uploader_file(uploader_id, current_user_id, new_data, parameter=None,
                        vk=None, mem_file=False, object_level='Campaign',
                        uploader_type='Facebook'):
    cur_up, user_that_ran = get_uploader_and_user_from_id(
        uploader_id=uploader_id, current_user_id=current_user_id)
    _set_task_progress(0)
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
            df = pd.concat([df, odf], ignore_index=True, sort=False)
        u_utl.write_df(df, file_name)
    msg_text = ('{} uploader {} was updated.'
                ''.format(file_name, cur_up.name))
    app_utl.object_post_message(cur_up, user_that_ran, msg_text,
                                object_name='Uploader')
    os.chdir(cur_path)
    uploader_create_objects(
        uploader_id, current_user_id, object_level, uploader_type)


@error_handler
def set_object_relation_file(uploader_id, current_user_id,
                             object_level='Campaign', uploader_type='Facebook'):
    cur_up, user_that_ran = get_uploader_and_user_from_id(
        uploader_id=uploader_id, current_user_id=current_user_id)
    up_cam = UploaderObjects.query.filter_by(
        uploader_id=cur_up.id, object_level=object_level,
        uploader_type=uploader_type).first()
    up_rel = UploaderRelations.query.filter_by(
        uploader_objects_id=up_cam.id).all()
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
            df = pd.concat([df, ndf], ignore_index=True, sort=False)
        else:
            ndf = df[df['impacted_column_name'] == rel.impacted_column_name]
            ndf = ndf.reset_index(drop=True)
            pos_list = rel.convert_string_to_list(rel.position)
            if not pos_list:
                pos = ''
            else:
                pos = '|'.join(pos_list)
            if (len(ndf['position']) > 0 and pos != ndf['position'][0] and
                    pos):
                ndf['position'] = pos
                col_name = ndf['column_name'][0].split('|')[0]
                cols = '|'.join([col_name for _ in pos_list])
                ndf['column_name'] = cols
                df = df.loc[df['impacted_column_name'] !=
                            rel.impacted_column_name]
                df = pd.concat([df, ndf], ignore_index=True, sort=False)
    u_utl.write_df(df, file_name)


def get_uploader_create_dict(object_level='Campaign', create_type='Media Plan',
                             creator_column=None, file_filter=None,
                             duplication_type=None, uploader_type='Facebook'):
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


@error_handler
def uploader_create_objects(uploader_id, current_user_id,
                            object_level='Campaign', uploader_type='Facebook'):
    cur_up, user_that_ran = get_uploader_and_user_from_id(
        uploader_id=uploader_id, current_user_id=current_user_id)
    up_obj = UploaderObjects.query.filter_by(
        uploader_id=cur_up.id, object_level=object_level,
        uploader_type=uploader_type).first()
    _set_task_progress(0)
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
    u_utl.write_df(df, file_name)
    os.chdir(cur_path)
    set_object_relation_file(uploader_id, current_user_id,
                             object_level=object_level,
                             uploader_type=uploader_type)
    os.chdir(cur_path)
    run_uploader(uploader_id, current_user_id, run_args='--create')
    msg_text = ('{} uploader {} creation file was updated.'
                ''.format(cur_up.name, object_level))
    app_utl.object_post_message(cur_up, user_that_ran, msg_text,
                                object_name='Uploader')
    os.chdir(cur_path)


@error_handler
def uploader_create_and_upload_objects(uploader_id, current_user_id,
                                       object_level='Campaign',
                                       uploader_type='Facebook'):
    cur_path = adjust_path(os.path.abspath(os.getcwd()))
    set_uploader_config_files(uploader_id, current_user_id)
    os.chdir(cur_path)
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


@error_handler
def uploader_save_creative(uploader_id, current_user_id, file, file_name):
    cur_up, user_that_ran = get_uploader_and_user_from_id(
        uploader_id=uploader_id, current_user_id=current_user_id)
    file_path = adjust_path(cur_up.local_path)
    os.chdir(file_path)
    file.seek(0)
    utl.dir_check('creative')
    with open(os.path.join('creative', file_name), 'wb') as f:
        shutil.copyfileobj(file, f, length=131072)
    return True


@error_handler
def get_uploader_creative(uploader_id, current_user_id):
    cur_up, user_that_ran = get_uploader_and_user_from_id(
        uploader_id=uploader_id, current_user_id=current_user_id)
    _set_task_progress(0)
    file_path = adjust_path(cur_up.local_path)
    os.chdir(file_path)
    file_names = os.listdir(".")
    df = pd.DataFrame(file_names, columns=['creative_file_names'])
    return [df]


@error_handler
def set_processor_accounts(processor_id, current_user_id, form_sources):
    app_utl.set_db_values(object_id=processor_id,
                          current_user_id=current_user_id,
                          form_sources=form_sources, table=Account)


@error_handler
def set_processor_conversions(processor_id, current_user_id, form_sources):
    app_utl.set_db_values(object_id=processor_id,
                          current_user_id=current_user_id,
                          form_sources=form_sources, table=Conversion)


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
    form_sources = json.loads(new_data)
    app_utl.set_db_values(object_id=processor_id,
                          current_user_id=current_user_id,
                          form_sources=form_sources, table=Conversion)


def set_conversions(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        proc_path = cur_processor.local_path
        if not proc_path:
            return False
        os.chdir(adjust_path(proc_path))
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
                                x.dcm_category, x.conversion_name) for x in
                                conv])
                        matrix.vm_change(idx, col, total_conv)
                        pc_conv = '|'.join(
                            ['{} : {}: Click-through Conversions'.format(
                                x.dcm_category, x.conversion_name) for x in
                                conv])
                        matrix.vm_change(idx, col + vmc.postclick, pc_conv)
                        pi_conv = '|'.join(
                            ['{} : {}: View-through Conversions'.format(
                                x.dcm_category, x.conversion_name) for x in
                                conv])
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


def convert_rate_card_to_relation(df):
    df = df.rename(columns={'adserving_fee': dctc.AR,
                            'reporting_fee': dctc.RFR,
                            'type_name': dctc.SRV})
    for col in [dctc.RFM, dctc.AM]:
        df[col] = 'CPM'
        df[col] = np.where(df[dctc.SRV].str.contains('Click'), 'CPC', 'CPM')
    df = df[[dctc.SRV, dctc.AM, dctc.AR, dctc.RFM, dctc.RFR]]
    return df


def set_processor_fees(processor_id, current_user_id):
    try:
        cur_processor = Processor.query.get(processor_id)
        rate_card = cur_processor.rate_card
        proc_path = cur_processor.local_path
        if not proc_path:
            return False
        os.chdir(adjust_path(proc_path))
        rate_list = []
        for row in rate_card.rates:
            rate_list.append(dict((col, getattr(row, col))
                                  for col in row.__table__.columns.keys()
                                  if 'id' not in col))
        df = pd.DataFrame(rate_list)
        df = convert_rate_card_to_relation(df)
        rc = dct.RelationalConfig()
        rc.read(dctc.filename_rel_config)
        params = rc.get_relation_params('Serving')
        dr = dct.DictRelational(**params)
        dr.write(df)
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


def set_processor_plan_net(processor_id, current_user_id, default_vm=None):
    try:
        cur_processor = Processor.query.get(processor_id)
        from uploader.upload.creator import MediaPlan
        base_path = app_utl.create_local_path(cur_processor)
        os.chdir(adjust_path(base_path))
        if not os.path.exists('mediaplan.csv'):
            return False, 'Plan does not exist.'
        df = pd.read_csv('mediaplan.csv')
        if MediaPlan.placement_phase in df.columns:
            cam_name = MediaPlan.placement_phase
        else:
            cam_name = MediaPlan.campaign_phase
        plan_cols = [cam_name, MediaPlan.partner_name]
        miss_cols = [x for x in plan_cols + [dctc.PNC] if x not in df.columns]
        if miss_cols:
            return False, '{} not a column name in plan.'.format(miss_cols)
        df = df.groupby(plan_cols)[dctc.PNC].sum().reset_index()
        df = df.rename(columns={cam_name: dctc.CAM,
                                MediaPlan.partner_name: dctc.VEN})
        df[dctc.FPN] = df[dctc.CAM] + '_' + df[dctc.VEN]
        if default_vm:
            matrix = default_vm
        else:
            matrix = vm.VendorMatrix()
        param = matrix.vendor_set('DCM')
        uncapped_partners = param['RULE_1_QUERY'].split('::')[1].split(',')
        df[dctc.UNC] = df[dctc.VEN].isin(uncapped_partners).replace(False, '')
        data_source = matrix.get_data_source(vm.plan_key)
        dic = dct.Dict(data_source.p[vmc.filenamedict])
        dic.write(df)
        return True, ''
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False, 'Unknown.'


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
        proc_path = cur_processor.local_path
        if not proc_path:
            return False
        os.chdir(adjust_path(proc_path))
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
        for ct in [('twitter', 'twconfig.json'), ('rs', 'rsapi.json'),
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
        base_path = app_utl.create_local_path(cur_processor)
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
                        replace(':', '').replace('.', '').replace("'", '').
                        replace('&', ''))
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
        result, msg = set_processor_plan_net(processor_id, current_user_id)
        if result:
            progress['set_planned_net'] = 'Success!'
        _set_task_progress(75)
        os.chdir(cur_path)
        result = run_processor(
            processor_id, current_user_id,
            run_args='--api all --ftp all --dbi all --exp all --tab --analyze')
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


def get_account_types(processor_id, current_user_id, vk):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        if cur_proc.local_path:
            cur_act_model = ProcessorDatasources
        else:
            cur_act_model = Account
        acts = cur_act_model.query.filter_by(processor_id=processor_id).all()
        acts = [x.key for x in acts if x.key]
        df = pd.DataFrame({'Current Accounts': acts})
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return False


def get_package_capping(processor_id, current_user_id, vk):
    try:
        _set_task_progress(0)
        cur_obj = Processor.query.get(processor_id)
        file_name = '/dictionaries/plannet_placement.csv'
        full_file = cur_obj.local_path + file_name
        if os.path.exists(full_file):
            df = pd.read_csv(full_file)
        else:
            df = pd.DataFrame({'RESULT': ['SPEND CAP FILE DOES NOT EXIST']})
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return False


def get_media_plan(processor_id, current_user_id, vk):
    try:
        _set_task_progress(0)
        cur_obj = Processor.query.get(processor_id)
        base_path = app_utl.create_local_path(cur_obj)
        mp_path = os.path.join(base_path, 'mediaplan.csv')
        if os.path.exists(mp_path):
            df = pd.read_csv(mp_path)
            mp_cols = [x for x in df.columns if 'Unnamed' not in x]
            df = df[mp_cols]
        else:
            df = pd.DataFrame({'RESULT': ['MEDIA PLAN DOES NOT EXIST']})
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return False


def get_serving_fees(processor_id, current_user_id, vk):
    try:
        _set_task_progress(0)
        df = get_relational_config(processor_id, current_user_id,
                                   parameter='Serving')[0]
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return False


def get_plan_property(processor_id, current_user_id, vk):
    try:
        _set_task_progress(0)
        func_dict = {
            'Add Account Types': get_account_types,
            'Plan Net': get_dictionary,
            'Package Capping': get_package_capping,
            'Plan As Datasource': get_media_plan,
            'Add Fees': get_serving_fees}
        if vk in func_dict:
            cur_func = func_dict[vk]
            if cur_func:
                df = cur_func(processor_id, current_user_id, vk)
            else:
                df = [pd.DataFrame({'Result': ['FUNCTION NOT KNOWN']})]
        else:
            df = [pd.DataFrame({'Result': ['FUNCTION NOT KNOWN']})]
        _set_task_progress(100)
        return df
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def check_processor_plan(processor_id, current_user_id, object_type=Processor):
    try:
        cur_obj = object_type.query.get(processor_id)
        base_path = app_utl.create_local_path(cur_obj)
        mp_path = os.path.join(base_path, 'mediaplan.csv')
        if os.path.exists(mp_path):
            t = os.path.getmtime(mp_path)
            last_update = dt.datetime.fromtimestamp(t)
            update_time = last_update.strftime('%Y-%m-%d')
            msg = 'Media Plan found, was last updated {}'.format(update_time)
            msg_level = 'success'
        else:
            msg = 'Media Plan not found.'
            msg_level = 'danger'
        resp = {'msg': msg, 'level': msg_level}
        _set_task_progress(100)
        return [resp]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def set_plan_as_datasource(processor_id, current_user_id, base_matrix):
    try:
        _set_task_progress(0)
        cur_obj = Processor.query.get(processor_id)
        base_path = app_utl.create_local_path(cur_obj)
        mp_path = os.path.join(base_path, 'mediaplan.csv')
        if os.path.exists(mp_path):
            raw_path = os.path.join(base_path, 'raw_data')
            utl.dir_check(raw_path)
            copy_file(mp_path, os.path.join(raw_path, 'mediaplan.csv'))
            vm_path = os.path.join(base_path, 'config', 'Vendormatrix.csv')
            if os.path.exists(vm_path):
                os.chdir(adjust_path(base_path))
                matrix = vm.VendorMatrix()
                vm_df = matrix.vm_df
                if vmc.api_mp_key not in vm_df[vmc.vendorkey].values:
                    mp_df = base_matrix.vm_df[
                        base_matrix.vm_df[vmc.vendorkey] == vmc.api_mp_key]
                    mp_df = mp_df.reset_index(drop=True)
                    mp_df[vmc.firstrow] = 0
                    vm_df = pd.concat([vm_df, mp_df]).reset_index(drop=True)
                    matrix.vm_df = vm_df
                    matrix.write()
            else:
                utl.dir_check(os.path.join(base_path, 'config'))
                os.chdir(adjust_path(base_path))
                base_matrix.write()
        _set_task_progress(100)
        return True, ''
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False, 'Unknown error'


def add_account_types(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        from uploader.upload.creator import MediaPlan
        if cur_proc.local_path:
            cur_act_model = ProcessorDatasources
        else:
            cur_act_model = Account
        acts = cur_act_model.query.filter_by(processor_id=processor_id).all()
        acts = [x.key for x in acts if x.key]
        base_path = adjust_path(app_utl.create_local_path(cur_proc))
        mp_path = os.path.join(base_path, 'mediaplan.csv')
        if not os.path.exists(mp_path):
            return False, 'Plan does not exist.'
        df = pd.read_csv(mp_path)
        if MediaPlan.partner_name not in df.columns:
            msg = '{} not a column name in plan.'.format(MediaPlan.partner_name)
            return False, msg
        partner_list = df[MediaPlan.partner_name].unique()
        api_dict = {}
        for key, value in vmc.api_partner_name_translation.items():
            for v in value:
                api_dict[v] = key
        for partner in partner_list:
            if partner in api_dict.keys():
                api_key = api_dict[partner]
                if api_key not in acts:
                    acts.append(api_key)
                    new_act = cur_act_model()
                    new_act.key = api_key
                    new_act.processor_id = processor_id
                    if cur_proc.local_path:
                        new_act.name = 'API_{}_FromPlan'.format(api_key)
                    db.session.add(new_act)
                    db.session.commit()
        _set_task_progress(100)
        return True, ''
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False, 'Unknown Error'


def add_plan_fees_to_processor(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        cur_user = User.query.get(current_user_id)
        base_path = adjust_path(app_utl.create_local_path(cur_proc))
        mp_path = os.path.join(base_path, 'mediaplan.csv')
        if not os.path.exists(mp_path):
            return False, 'Media plan does not exist.'
        df = pd.read_csv(mp_path)
        serving_cols = ['Ad Serving Type', 'Ad Serving Rate', 'Reporting Fee']
        for col in serving_cols:
            if col not in df.columns:
                df[col] = 0
        sdf = df.groupby(serving_cols).size().reset_index()[serving_cols]
        sdf = sdf.rename(
            columns={'Ad Serving Type': Rates.type_name.name,
                     'Ad Serving Rate': Rates.adserving_fee.name,
                     'Reporting Fee': Rates.reporting_fee.name})
        afee_cols = ['Agency Fee Rate']
        adf = df.groupby(afee_cols).size().reset_index()[afee_cols]
        if cur_proc.local_path:
            df = get_constant_dict(processor_id, current_user_id)[0]
            df = df[df[dctc.DICT_COL_NAME] != dctc.AGF]
            adf = pd.DataFrame(
                {dctc.DICT_COL_NAME: [dctc.AGF],
                 dctc.DICT_COL_VALUE: [adf[afee_cols].values[0][0]],
                 dctc.DICT_COL_DICTNAME: [None]})
            df = pd.concat([df, adf], ignore_index=True).reset_index(drop=True)
            write_constant_dict(processor_id, current_user_id, df.to_json())
            df = get_relational_config(processor_id, current_user_id,
                                       parameter='Serving')[0]
            sdf = convert_rate_card_to_relation(sdf)
            df = df[~df[dctc.SRV].isin(sdf[dctc.SRV].to_list())]
            df = pd.concat([df, sdf], ignore_index=True).reset_index(drop=True)
            write_relational_config(processor_id, current_user_id, df.to_json(),
                                    parameter='Serving')
        else:
            cur_proc.digital_agency_fees = adf[afee_cols].values[0][0]
            write_rate_card(processor_id, current_user_id,
                            sdf.to_json(orient='records'), 'None')
            rate_card_name = '{}|{}'.format(cur_proc.name,
                                            cur_user.username)
            rate_card = RateCard.query.filter_by(name=rate_card_name).first()
            cur_proc.rate_card_id = rate_card.id
            db.session.commit()
        _set_task_progress(100)
        return True, ''
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False, 'Unknown error'


def write_plan_property(processor_id, current_user_id, vk, new_data):
    try:
        _set_task_progress(0)
        if vk == 'Plan Net':
            write_dictionary(processor_id, current_user_id, new_data, vk)
        elif vk == 'Package Capping':
            save_spend_cap_file(processor_id, current_user_id, new_data,
                                as_json=True)
        elif vk == 'Plan As Datasource':
            save_media_plan(processor_id, current_user_id,
                            pd.read_json(new_data))
        elif vk == 'Add Fees':
            write_relational_config(processor_id, current_user_id, new_data,
                                    dctc.SRV)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def single_apply_processor_plan(processor_id, current_user_id, progress,
                                progress_type, cur_path, matrix):
    os.chdir(cur_path)
    r = None
    if progress_type == 'Plan Net':
        r = set_processor_plan_net(processor_id, current_user_id, matrix)
    elif progress_type == 'Package Capping':
        r = set_spend_cap_config_file(processor_id, current_user_id, dctc.PKD)
        if r:
            os.chdir(cur_path)
            r = save_spend_cap_file(processor_id, current_user_id, None,
                                    from_plan=True)
    elif progress_type == 'Plan As Datasource':
        r = set_plan_as_datasource(processor_id, current_user_id, matrix)
    elif progress_type == 'Add Account Types':
        r = add_account_types(processor_id, current_user_id)
    elif progress_type == 'Add Fees':
        r = add_plan_fees_to_processor(processor_id, current_user_id)
    if r[0]:
        progress[progress_type] = ['Success!']
    else:
        progress[progress_type] = ['FAILED: {}'.format(r[1])]
    return progress


def apply_processor_plan(processor_id, current_user_id, vk):
    progress_types = Processor.get_plan_properties()
    progress = {}
    for k in progress_types:
        progress[k] = ['Failed']
    try:
        current_progress = 0
        _set_task_progress(current_progress)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        os.chdir('processor')
        matrix = vm.VendorMatrix()
        os.chdir(cur_path)
        vk = json.loads(vk)
        progress = {k: v if k in vk else ['Skipped']
                    for k, v in progress.items()}
        for progress_type in vk:
            progress = single_apply_processor_plan(
                processor_id, current_user_id, progress, progress_type,
                cur_path, matrix)
            current_progress += (100 / len(progress_types))
            _set_task_progress(current_progress)
        df = pd.DataFrame(progress).T.reset_index()
        df = df.rename(columns={0: 'Result', 'index': 'Plan Task'})
        _set_task_progress(100)
        return [df]
    except:
        df = pd.DataFrame(progress)
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return [df]


def uploader_full_placement_creation(upo, mp_df, budget_col):
    name_list = upo.string_to_list(upo.media_plan_columns)
    name_list = [x.strip() for x in name_list]
    miss_cols = [x for x in name_list if x not in mp_df.columns]
    env_list = ['device', 'environment']
    tar_list = ['targeting', 'targeting_bucket']
    for miss_col in miss_cols:
        if_need_str = ' (If Needed)'
        name_str = ' Name'
        poss_strs = [if_need_str, name_str]
        new_val = [miss_col.replace(x, '') for x in poss_strs]
        new_val += ['{}{}'.format(miss_col, x) for x in poss_strs]
        new_val += [miss_col.capitalize(), miss_col.lower()]
        for col_list in [env_list, tar_list]:
            if miss_col.lower() in col_list:
                new_val += col_list
                new_val += [x.capitalize() for x in col_list]
        new_val = [x for x in new_val if x in mp_df.columns]
        if new_val:
            name_list = [new_val[0] if x == miss_col else x for x in name_list]
            upo.media_plan_columns = name_list
            db.session.commit()
    ndf = full_placement_creation(mp_df, '', vmc.fullplacename,
                                  name_list)
    ndf = ndf.groupby(vmc.fullplacename)[budget_col].sum()
    ndf = ndf.reset_index()
    return ndf


@error_handler
def uploader_add_plan_costs(uploader_id, current_user_id):
    u = db.session.get(Uploader, uploader_id)
    cur_path = adjust_path(os.path.abspath(os.getcwd()))
    os.chdir(adjust_path(u.local_path))
    uploader_type = 'Facebook'
    object_levels = ['Campaign', 'Adset', 'Ad']
    file_name = 'mediaplan.xlsx'
    if not os.path.exists(file_name):
        return False
    mp_df = utl.import_read_csv(file_name)
    budget_cols = [PartnerPlacements.total_budget.name, 'Planned Net Cost',
                   'Net Cost']
    budget_col = [x for x in budget_cols if x in mp_df.columns]
    if not budget_col:
        return True
    else:
        budget_col = budget_col[0]
    for idx, object_level in enumerate(object_levels):
        os.chdir(adjust_path(u.local_path))
        upo = UploaderObjects.query.filter_by(
            uploader_id=u.id, object_level=object_level,
            uploader_type=uploader_type).first()
        file_name = uploader_file_translation(
            'uploader_full_relation', object_level=object_level,
            uploader_type=uploader_type)
        df = utl.import_read_csv(file_name)
        spend_col = get_spend_column(object_level, uploader_type)
        if spend_col:
            rel = upo.uploader_relations.filter_by(
                impacted_column_name=spend_col).first()
            ndf = uploader_full_placement_creation(upo, mp_df, budget_col)
            p_col = get_primary_column(object_level, uploader_type)
            ndf['column_name'] = p_col
            ndf['position'] = ''
            ndf['impacted_column_name'] = rel.impacted_column_name
            new_cols = {
                vmc.fullplacename: 'column_value',
                budget_col: 'impacted_column_new_value'}
            ndf = ndf.rename(columns=new_cols)
            rel.relation_constant = ''
            db.session.commit()
            df = df.loc[df['impacted_column_name'] != rel.impacted_column_name]
            df = pd.concat([df, ndf], ignore_index=True, sort=False)
            u_utl.write_df(df, file_name)
        prev_levels = object_levels[:idx]
        for prev_level in prev_levels:
            prev_primary = get_primary_column(prev_level)
            ndf = get_uploader_file(
                uploader_id, current_user_id, object_level=object_level,
                parameter='edit_relation', uploader_type=uploader_type,
                vk=prev_primary)
            if not ndf or 'Result' in ndf[0].columns:
                continue
            df = df.loc[df['impacted_column_name'] != prev_primary]
            df = pd.concat([df, ndf[0]], ignore_index=True, sort=False)
            u_utl.write_df(df, file_name)
        os.chdir(cur_path)
        uploader_create_objects(
            uploader_id, current_user_id, object_level, uploader_type)
    return True


@error_handler
def save_media_plan(processor_id, current_user_id, media_plan,
                    object_type=Processor):
    cur_obj = db.session.get(object_type, processor_id)
    cur_user = db.session.get(User, current_user_id)
    base_path = app_utl.create_local_path(cur_obj)
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    object_name = object_type.__name__
    if object_type == Processor:
        file_name = os.path.join(base_path, 'mediaplan.csv')
        media_plan.to_csv(file_name)
    else:
        file_name = os.path.join(base_path, 'mediaplan.xlsx')
        u_utl.write_df(df=media_plan, file_name=file_name,
                       sheet_name='Media Plan')
        create_task = '.{}'.format(create_uploader.__name__)
        if not cur_obj.get_task_in_progress(create_task):
            uploader_add_plan_costs(processor_id, current_user_id)
    msg_text = ('{} media plan was updated'.format(cur_obj.name))
    app_utl.object_post_message(cur_obj, cur_user, msg_text,
                                object_name=object_name)
    return True


def save_spend_cap_file(processor_id, current_user_id, new_data,
                        from_plan=False, as_json=False):
    try:
        cur_obj = Processor.query.get(processor_id)
        cur_user = User.query.get(current_user_id)
        file_name = '/dictionaries/plannet_placement.csv'
        if from_plan:
            base_path = app_utl.create_local_path(cur_obj)
            mp_file = os.path.join(base_path, 'mediaplan.csv')
            df = pd.read_csv(mp_file)
            pack_col = dctc.PKD.replace('mp', '')
            if pack_col not in df.columns:
                return False, '{} not in file'.format(pack_col)
            df = df.groupby([pack_col])[dctc.PNC].sum().reset_index()
            df = df[~df[pack_col].isin(['0', 0, 'None'])]
            df = df.rename(columns={dctc.PNC: 'Net Cost (Capped)'})
            full_file_path = base_path + file_name
            df.to_csv(full_file_path, index=False)
        elif as_json:
            base_path = app_utl.create_local_path(cur_obj)
            cap_file = os.path.join(base_path, 'dictionaries',
                                    'plannet_placement.csv')
            df = pd.read_json(new_data)
            if 'index' in df.columns:
                df = df.drop('index', axis=1)
            df = df.replace('NaN', '')
            df.to_csv(cap_file, index=False)
        else:
            new_data.seek(0)
            with open(cur_obj.local_path + file_name, 'wb') as f:
                shutil.copyfileobj(new_data, f, length=131072)
        msg_text = 'Spend cap file was saved.'
        app_utl.object_post_message(cur_obj, cur_user, msg_text)
        _set_task_progress(100)
        return True, ''
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False, 'Unknown Error'


def set_spend_cap_config_file(processor_id, current_user_id, dict_col):
    try:
        cur_obj = Processor.query.get(processor_id)
        cur_user = User.query.get(current_user_id)
        cap_config_dict = {
            'file_name': ['dictionaries/plannet_placement.csv'],
            'file_dim': [dict_col.replace('mp', '')],
            'file_metric': ['Net Cost (Capped)'],
            'processor_dim': [dict_col],
            'processor_metric': ['Planned Net Cost']}
        df = pd.DataFrame(cap_config_dict)
        base_path = app_utl.create_local_path(cur_obj)
        os.chdir(adjust_path(base_path))
        df.to_csv('config/cap_config.csv', index=False)
        msg_text = ('{} spend cap config was updated.'
                    ''.format(cur_obj.name))
        app_utl.object_post_message(cur_obj, cur_user, msg_text)
        _set_task_progress(100)
        return True, ''
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False, 'Unknown error occurred'


def processor_fix_request(processor_id, current_user_id, fix):
    try:
        cur_processor = Processor.query.get(processor_id)
        ali_user = User.query.get(4)
        fixed = False
        if fix.fix_type == 'Update Plan':
            fixed, msg = set_processor_plan_net(processor_id, ali_user.id)
        elif fix.fix_type == 'Spend Cap':
            fixed, msg = set_spend_cap_config_file(processor_id, ali_user.id,
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
            app_utl.object_post_message(cur_processor, ali_user, msg_text,
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
        app_utl.object_post_message(cur_processor, cur_user, msg_text)
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
        cur_processor = db.session.get(Processor, processor_id)
        proc_dict = cur_processor.to_dict()
        if 'new_proc' in form_data:
            new_processor = Processor.query.get(form_data['new_proc'])
        else:
            new_processor = Processor()
        for k, v in proc_dict.items():
            new_processor.__setattr__(k, v)
        new_processor.name = form_data['new_name']
        new_processor.start_date = form_data['new_start_date']
        new_processor.end_date = form_data['new_end_date']
        if 'new_proc' not in form_data:
            db.session.add(new_processor)
        new_processor.local_path = ''
        db.session.commit()
        new_path = app_utl.create_local_path(new_processor)
        new_processor.local_path = new_path
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
        cur_processor, user_that_ran = app_utl.get_obj_user(
            object_id=processor_id, current_user_id=current_user_id)
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
        app_utl.object_post_message(cur_processor, user_that_ran, msg_text)
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
        os.chdir(adjust_path(cur_processor.local_path))
        os.remove(os.path.join(utl.config_path, exc.upload_id_file))
        shutil.rmtree('backup')
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
        new_uploader.name = form_data['new_name']
        db.session.add(new_uploader)
        db.session.commit()
        new_path = app_utl.create_local_path(new_uploader)
        new_uploader.local_path = new_path
        db.session.commit()
        return new_uploader.id
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Uploader {} User {}'.format(
            uploader_id, current_user_id), exc_info=sys.exc_info())
        return False


def duplicate_uploader_objects(uploader_id, current_user_id, old_uploader_id):
    try:
        u = db.session.get(Uploader, uploader_id)
        u.create_base_uploader_objects(uploader_id)
        for object_level in ['Campaign', 'Adset', 'Ad']:
            upo = UploaderObjects.query.filter_by(
                uploader_id=uploader_id, object_level=object_level).first()
            old_upo = UploaderObjects.query.filter_by(
                uploader_id=old_uploader_id, object_level=object_level).first()
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


@error_handler
def get_processor_total_metrics_file(processor_id, current_user_id):
    cur_processor = db.session.get(Processor, processor_id)
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
    return [df]


def clean_topline_df_from_db(db_item, new_col_name):
    df = pd.DataFrame(db_item.data)
    idx_col = 'Topline Metrics'
    if not df.empty and idx_col in df.columns:
        df = df.set_index(idx_col)
        df = utl.data_to_type(df, float_col=list(df.columns))
        df = pd.DataFrame(df.fillna(0).T.sum())
        calculated_metrics = az.ValueCalc().metric_names
        metric_names = [x for x in df.columns if x in calculated_metrics]
        df = az.ValueCalc().calculate_all_metrics(
            metric_names=metric_names, df=df)
    df = clean_total_metric_df(df, new_col_name)
    return df


@error_handler
def get_processor_total_metrics(processor_id, current_user_id, dimensions=None,
                                metrics=None, filter_dict=None,
                                return_func=None):
    cur_processor = db.session.get(Processor, processor_id)
    topline_analysis = cur_processor.processor_analysis.filter_by(
        key=az.Analyze.topline_col).all()
    kpis, kpi_cols = get_kpis_for_processor(processor_id, current_user_id)
    cv_col = 'current_value'
    if processor_id == 23:
        df = pd.DataFrame(columns=['impressions', 'clicks', 'netcost'])
    elif not topline_analysis:
        return [pd.DataFrame()]
    else:
        df = clean_topline_df_from_db(
            [x for x in topline_analysis
             if x.parameter == az.Analyze.topline_col][0], cv_col)
        tdf = clean_topline_df_from_db(
            [x for x in topline_analysis
             if x.parameter == az.Analyze.lw_topline_col][0], 'new_value')
        twdf = clean_topline_df_from_db(
            [x for x in topline_analysis
             if x.parameter == az.Analyze.tw_topline_col][0], 'old_value')
        try:
            df = df.join(tdf)
        except ValueError:
            return [
                pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]
        df = df.join(twdf)
    cv_col = 'current_value'
    if filter_dict:
        metrics = list(set(list(
            kpi_cols) + ['impressions', 'clicks', 'netcost']))
        tdf = get_data_tables_from_db(
            processor_id, current_user_id, dimensions=[],
            metrics=metrics, filter_dict=filter_dict)
        tdf = tdf[0]
        tdf = az.ValueCalc().calculate_all_metrics(kpis, tdf)
        if df.empty:
            if tdf.empty:
                return [tdf]
            df = tdf.T
            df[cv_col] = df[0]
        else:
            df = df.join(tdf.T)
        df['change'] = (df[0].astype(float) / df[cv_col].astype(float))
        df = df.drop(columns=cv_col).rename(columns={0: cv_col})
        df = df[[cv_col] + [x for x in df.columns if x != cv_col]]
    else:
        cols = ['new_value', 'old_value']
        df = utl.data_to_type(df, float_col=['new_value', 'old_value'])
        for col in cols:
            if col not in df:
                df[col] = 0
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
    df = df[df['name'].isin([cal.NCF, vmc.impressions,
                             vmc.clicks] + list(kpis))]
    if filter_dict:
        df['msg'] = 'Of Total'
    else:
        df['msg'] = 'Since Last Week'
    df = df.iloc[::-1]
    return [df]


@error_handler
def get_processor_daily_notes(processor_id, current_user_id, dimensions=None,
                              metrics=None, filter_dict=None, return_func=None):
    cur_processor = db.session.get(Processor, processor_id)
    kpi_analysis = cur_processor.processor_analysis.filter_by(
        key=az.Analyze.kpi_col).all()
    _set_task_progress(80)
    kpis = set(x.parameter for x in kpi_analysis
               if x.parameter not in ['0', 'nan'])
    param_2s = ['Trend', 'Smallest', 'Largest']
    data = {}
    for kpi in kpis:
        cur_analysis = {
            x.parameter_2: x.message for x in kpi_analysis
            if (x.parameter == kpi and
                ((x.parameter_2 in param_2s and x.split_col == dctc.VEN) or
                 (x.parameter_2 == param_2s[0] and x.split_col == vmc.date))
                )}
        data[kpi] = cur_analysis
    if not data or filter_dict:
        return [pd.DataFrame()]
    df = pd.DataFrame(data=data)
    df = df.fillna('')
    df = df.iloc[::-1]
    return [df]


@error_handler
def get_processor_topline_metrics(processor_id, current_user_id, vk=None):
    cur_processor = db.session.get(Processor, processor_id)
    os.chdir(adjust_path(cur_processor.local_path))
    topline_analysis = cur_processor.processor_analysis.filter_by(
        key=az.Analyze.topline_col).all()
    filter_dict = vk
    if not topline_analysis:
        df = pd.DataFrame([{'Result': 'No topline metrics yet'}])
        lt = app_utl.LiquidTable(df=df, table_name='toplineMetrics')
        return [lt.table_dict]
    else:
        df = pd.DataFrame([x for x in topline_analysis if
                           x.parameter == az.Analyze.topline_col][0].data)
    if filter_dict:
        kpis, kpi_cols = get_kpis_for_processor(
            processor_id, current_user_id)
        metrics = az.Analyze.topline_metrics
        filter_dict = json.loads(filter_dict)
        base_metrics = [x[0] for x in metrics]
        base_metrics = list(utl.db_df_translation(
            base_metrics, adjust_path(cur_processor.local_path)).values())
        base_metrics = list(set(list(kpi_cols) + base_metrics))
        tdf = get_data_tables_from_db(
            processor_id, current_user_id, dimensions=['campaignname'],
            metrics=base_metrics, filter_dict=filter_dict)[0]
        cols = utl.db_df_translation(
            tdf.columns.to_list(), adjust_path(cur_processor.local_path),
            reverse=True)
        tdf = tdf.rename(columns=cols)
        analyze_topline = az.Analyze(df=tdf)
        df = analyze_topline.generate_topline_metrics()
    lt = app_utl.LiquidTable(df=df, table_name='toplineMetrics',
                             col_filter=False, chart_btn=False,
                             search_bar=False)
    return [lt.table_dict]


@error_handler
def get_data_tables_from_db(processor_id, current_user_id, parameter=None,
                            dimensions=None, metrics=None, filter_dict=None,
                            use_cache=True, table_name=None, return_func=None):
    cur_processor = db.session.get(Processor, processor_id)
    if ((not cur_processor.local_path) or
            (not os.path.exists(adjust_path(cur_processor.local_path)))):
        return [pd.DataFrame({x: [] for x in dimensions + metrics})]
    _set_task_progress(15)
    if metrics == ['kpi']:
        kpis, kpi_cols = get_kpis_for_processor(processor_id, current_user_id)
        metrics = [x for x in ['impressions', 'clicks', 'netcost']
                   if x not in kpi_cols] + kpi_cols
    else:
        kpis = None
    os.chdir(adjust_path(cur_processor.local_path))
    if not metrics:
        metrics = ['impressions', 'clicks', 'netcost']
    metrics = sorted(metrics)
    old_analysis = update_analysis_in_db_reporting_cache(
        processor_id, current_user_id, pd.DataFrame(),
        dimensions, metrics, filter_dict, check=True)
    if old_analysis and use_cache:
        if old_analysis.date == datetime.today().date():
            df = pd.read_json(old_analysis.data).sort_index()
            df = df[~((df == 'None') | (df == 0)).all(axis=1)]
            return [df]
    dimensions_sql = ['event.{}'.format(x) if x == 'eventdate'
                      else x for x in dimensions]
    dimensions_sql = ','.join(dimensions_sql)
    metric_sql = ','.join(['SUM({0}) AS {0}'.format(x) for x in metrics])
    if dimensions_sql:
        select_sql = '{0},{1}'.format(dimensions_sql, metric_sql)
    else:
        select_sql = metric_sql
    if processor_id == 23:
        where_sql = ""
    else:
        if not os.path.exists('config/upload_id_file.csv'):
            return [pd.DataFrame({x: [] for x in dimensions + metrics})]
        else:
            up_id = pd.read_csv('config/upload_id_file.csv')
            up_id = up_id['uploadid'][0]
            where_sql = "WHERE fullplacement.uploadid = '{}'".format(up_id)
    where_args = []
    if filter_dict:
        for f in filter_dict:
            for k, v in f.items():
                if v:
                    if k == 'eventdate':
                        date_format_str = '%Y-%m-%dT%H:%M:%S.%fZ'
                        sd = datetime.strptime(
                            v[0], date_format_str).strftime('%Y-%m-%d')
                        ed = datetime.strptime(
                            v[1], date_format_str).strftime('%Y-%m-%d')
                        w = (" AND (event.{0} BETWEEN '{1}' AND '{2}' "
                             "OR event.{0} IS NULL)".format(k, sd, ed))
                    else:
                        w = " AND {} IN ({})".format(
                            k, ', '.join(['%s'] * len(v)))
                        where_args.extend(v)
                    if where_sql == "":
                        w = "{}{}".format("WHERE", w[4:])
                    where_sql += w
    _set_task_progress(30)
    sb = exp.ScriptBuilder()
    base_table = [x for x in sb.tables if x.name == 'event'][0]
    append_tables = sb.get_active_event_tables(metrics)
    from_script = sb.get_from_script_with_opts(base_table,
                                               event_tables=append_tables)
    command = """SELECT {0}
        {1}
        {2}
    """.format(select_sql, from_script, where_sql)
    if dimensions_sql:
        command += 'GROUP BY {}'.format(dimensions_sql)
    db_class = exp.DB()
    db_class.input_config(app.config['EXP_DB'])
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
    cols = utl.db_df_translation(
        metrics, adjust_path(cur_processor.local_path), reverse=True)
    df = df.rename(columns=cols)
    df = df[~((df == 'None') | (df == 0)).all(axis=1)]
    update_analysis_in_db_reporting_cache(
        processor_id, current_user_id, df, dimensions, metrics, filter_dict)
    return [df]


@error_handler
def get_liquid_table_from_db(processor_id, current_user_id, parameter=None,
                             dimensions=None, metrics=None, filter_dict=None,
                             use_cache=True, table_name=None, return_func=None,
                             chart_show=True):
    include_args = ['parameter', 'dimensions', 'metrics', 'filter_dict',
                    'use_cache']
    args_dict = {key: value for key, value in locals().items() if
                 key in include_args}
    df = get_data_tables_from_db(processor_id, current_user_id, parameter,
                                 dimensions, metrics, filter_dict,
                                 use_cache, table_name, return_func)[0]
    _set_task_progress(90)
    lt = app_utl.LiquidTable(df=df, table_name=table_name,
                             chart_func=return_func, chart_show=chart_show,
                             metadata=args_dict, download_table=True)
    return [lt.table_dict]


@error_handler
def get_raw_file_delta_table(processor_id, current_user_id, vk=None,
                             dimensions=None, metrics=None, filter_dict=None,
                             return_func=None):
    odf_data = get_raw_file_data_table(
        processor_id, current_user_id, vk, dimensions, metrics,
        filter_dict, temp=False)[0]['data']
    odf = pd.DataFrame(data=odf_data)
    ndf_data = get_raw_file_data_table(
        processor_id, current_user_id, vk,
        dimensions, metrics, filter_dict, temp=True)[0]['data']
    ndf = pd.DataFrame(data=ndf_data)
    if ([x for x in dimensions
         if x not in ndf.columns or x not in odf.columns]):
        df = pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])
    else:
        df = ndf.set_index(dimensions).subtract(odf.set_index(dimensions),
                                                fill_value=0).reset_index()
    lt = app_utl.LiquidTable(df=df, chart_func=return_func, table_name=None,
                             chart_show=True)
    return [lt.table_dict]


@error_handler
def get_raw_file_data_table(processor_id, current_user_id, vk=None,
                            dimensions=None, metrics=None, filter_dict=None,
                            temp=None, return_func=None):
    cur_processor = db.session.get(Processor, processor_id)
    if ((not cur_processor.local_path) or
            (not os.path.exists(adjust_path(cur_processor.local_path)))):
        lt = app_utl.LiquidTable(
            df=pd.DataFrame({x: [] for x in dimensions + metrics}),
            table_name=None)
        return [lt.table_dict]
    _set_task_progress(15)
    def_metrics = [vmc.impressions, vmc.clicks, vmc.cost]
    if metrics == ['kpi']:
        kpis, kpi_cols = get_kpis_for_processor(
            processor_id, current_user_id)
        metrics = [x for x in def_metrics if x not in kpi_cols] + kpi_cols
    else:
        kpis = None
    if not metrics:
        metrics = def_metrics
    os.chdir(adjust_path(cur_processor.local_path))
    matrix = vm.VendorMatrix()
    if temp:
        for col in [vmc.filename, vmc.filename_true]:
            if vk not in matrix.vm[col]:
                continue
            new_name = matrix.vm[col][vk]
            file_type = os.path.splitext(new_name)[1]
            new_name = new_name.replace(
                file_type, 'TMP{}'.format(file_type))
            matrix.vm[col][vk] = new_name
    _set_task_progress(60)
    try:
        df = matrix.vendor_get(vk)
    except:
        lt = app_utl.LiquidTable(
            df=pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}]),
            table_name=None)
        return [lt.table_dict]
    _set_task_progress(90)
    df = utl.data_to_type(df, float_col=metrics)
    metrics = [x for x in metrics if x in df.columns]
    if [x for x in dimensions if x not in df.columns]:
        lt = app_utl.LiquidTable(
            df=pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}]),
            table_name=None)
        return [lt.table_dict]
    df = df.groupby(dimensions)[metrics].sum()
    df = df.reset_index()
    if vmc.date in df.columns:
        df = utl.data_to_type(df, str_col=[vmc.date])
        df = df[df[vmc.date] != 'None']
    df = df.fillna(0)
    if kpis and vmc.cost in df.columns:
        df['Net Cost Final'] = df[vmc.cost]
        calculated_metrics = az.ValueCalc().metric_names
        metric_names = [x for x in kpis if x in calculated_metrics]
        df = az.ValueCalc().calculate_all_metrics(
            metric_names=metric_names, df=df, db_translate=False)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna(0)
    lt = app_utl.LiquidTable(df=df, chart_func=return_func, table_name=None,
                             chart_show=True)
    return [lt.table_dict]


def get_processor_pacing_metrics(processor_id, current_user_id, parameter=None,
                                 dimensions=None, metrics=None,
                                 filter_dict=None):
    try:
        _set_task_progress(0)
        if dimensions is None:
            dimensions = []
        cur_proc = Processor.query.filter_by(id=processor_id).first_or_404()
        os.chdir(cur_proc.local_path)
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source('Plan Net')
        dic = dct.Dict(data_source.p[vmc.filenamedict])
        pdf = pd.DataFrame(dic.data_dict)
        plan_cols = data_source.p[vmc.fullplacename]
        df_cols = [x for x in plan_cols if x not in dimensions]
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.delivery_comp_col).first()
        final_columns = df_cols + [dctc.SD, dctc.ED, vmc.cost,
                                   dctc.PNC, dctc.UNC, 'Delivery',
                                   'Projected Full Delivery',
                                   '% Through Campaign', vmc.AD_COST]
        adf = pd.DataFrame(columns=final_columns)
        if analysis and analysis.data:
            adf = pd.DataFrame(analysis.data)
            adf_cols = adf.columns.to_list()
            if not pdf.empty:
                adf_cols.remove(dctc.PNC)
                pdf_cols = plan_cols + [dctc.PNC, dctc.UNC]
                adf = pdf[pdf_cols].merge(adf[adf_cols], how='outer',
                                          on=plan_cols)
            else:
                adf[dctc.UNC] = ""
        adf = adf[final_columns]
        adf[dctc.PNC] = adf[dctc.PNC].replace(
            [np.inf, -np.inf], np.nan).fillna(0.0)
        adf[dctc.PNC] = utl.data_to_type(
            pd.DataFrame(adf[dctc.PNC]), float_col=[dctc.PNC])[dctc.PNC]
        adf[dctc.PNC] = adf[dctc.PNC].round(2)
        adf[dctc.PNC] = '$' + adf[dctc.PNC].astype(str)
        adf = adf.fillna("")
        if parameter:
            adf = get_file_in_memory(adf)
        lt = app_utl.LiquidTable(
            df=adf, table_name='pacingMetrics', specify_form_cols=False,
            prog_cols=['Delivery'])
        _set_task_progress(100)
        return [lt.table_dict]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} Parameter {} Metrics {}'
            ' Filter Dict {}'.format(
                processor_id, current_user_id, parameter, metrics, filter_dict),
            exc_info=sys.exc_info())
        return [pd.DataFrame([{
            'Result': 'DATA WAS UNABLE TO BE LOADED. Pacing Table only '
                      'available when planned spends are based on Vendor,'
                      ' Campaign, Country/Region, and or Environment'}]), []]


def get_pacing_alert_count(processor_id, current_user_id, dimensions=None,
                           metrics=None, filter_dict=None, return_func=None):
    try:
        _set_task_progress(0)
        count = 0
        cur_proc = Processor.query.filter_by(id=processor_id).first_or_404()
        os.chdir(cur_proc.local_path)
        over_delivery = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.delivery_col,
            parameter=az.Analyze.over_delivery_col).first()
        if over_delivery:
            df = pd.DataFrame(over_delivery.data)
            count += len(df.index)
        daily_over_pacing = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.daily_pacing_alert,
            parameter=az.Analyze.over_daily_pace).first()
        if daily_over_pacing:
            df = pd.DataFrame(daily_over_pacing.data)
            count += len(df.index)
        daily_under_pacing = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.daily_pacing_alert,
            parameter=az.Analyze.under_daily_pace).first()
        if daily_under_pacing:
            df = pd.DataFrame(daily_under_pacing.data)
            count += len(df.index)
        adserving_alerts = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.adserving_alert).first()
        if adserving_alerts:
            df = pd.DataFrame(adserving_alerts.data)
            count += len(df.index)
        _set_task_progress(100)
        df = pd.DataFrame(data={'count': [count]})
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} Dimensions {} Metrics '
            '{}  Filter Dict {}'.format(
                processor_id, current_user_id, dimensions, metrics,
                filter_dict), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}]), []]


def get_pacing_alerts(processor_id, current_user_id, dimensions=None,
                      metrics=None, filter_dict=None, return_func=None):
    try:
        _set_task_progress(0)
        rdf = pd.DataFrame(columns=['msg'])
        cur_proc = Processor.query.filter_by(id=processor_id).first_or_404()
        os.chdir(cur_proc.local_path)
        matrix = vm.VendorMatrix()
        data_source = matrix.get_data_source('Plan Net')
        plan_cols = data_source.p[vmc.fullplacename]
        over_delivery = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.delivery_col,
            parameter=az.Analyze.over_delivery_col).first()
        if over_delivery:
            df = pd.DataFrame(over_delivery.data)
            for index, row in df.iterrows():
                breakouts = []
                val = row["Delivery"]
                for col in plan_cols:
                    breakouts.append(row[col])
                breakout = ' '.join(breakouts)
                msg = ('Over delivered on {0} by: {1}. TURN OFF SPEND.'
                       ).format(breakout, val)
                rdf.loc[len(rdf.index)] = [msg]
        daily_over_pacing = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.daily_pacing_alert,
            parameter=az.Analyze.over_daily_pace).first()
        if daily_over_pacing:
            df = pd.DataFrame(daily_over_pacing.data)
            for index, row in df.iterrows():
                breakouts = []
                val = row['Day Pacing']
                for col in plan_cols:
                    breakouts.append(row[col])
                breakout = ' '.join(breakouts)
                msg = ('Yesterday\'s spend for {0} was OVER daily pacing goal '
                       'by: {1}.').format(breakout, val)
                rdf.loc[len(rdf.index)] = [msg]
        daily_under_pacing = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.daily_pacing_alert,
            parameter=az.Analyze.under_daily_pace).first()
        if daily_under_pacing:
            df = pd.DataFrame(daily_under_pacing.data)
            for index, row in df.iterrows():
                breakouts = []
                val = row['Day Pacing']
                for col in plan_cols:
                    breakouts.append(row[col])
                breakout = ' '.join(breakouts)
                msg = ('Yesterday\'s spend for {0} was UNDER daily pacing goal '
                       'by: {1}.').format(breakout, val)
                rdf.loc[len(rdf.index)] = [msg]
        adserving_alerts = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.adserving_alert).first()
        if adserving_alerts:
            df = pd.DataFrame(adserving_alerts.data)
            for index, row in df.iterrows():
                breakouts = []
                val = row[vmc.AD_COST]
                for col in plan_cols:
                    breakouts.append(row[col])
                breakout = ' '.join(breakouts)
                msg = ('Adserving cost significantly OVER for {0}: {1} \n '
                       'Double check Serving/Ad Rate in processor. If correct, '
                       'PAUSE CAMPAIGN. CHECK TRACKERS. Else, adjust '
                       'model/rates in processor.'
                       ).format(breakout, val)
                rdf.loc[len(rdf.index)] = [msg]
        _set_task_progress(100)
        return [rdf]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} Dimensions {} Metrics '
            '{}  Filter Dict {}'.format(
                processor_id, current_user_id, dimensions, metrics,
                filter_dict), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}]), []]


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


def get_vendor_keys_of_update_files(df):
    key_col = vmc.vendorkey if vmc.vendorkey in df.columns else 'source'
    tdf = df[df[key_col].str[:3] == 'API']
    tdf = tdf[tdf[key_col].str[:len('API_Rawfile')] == 'API_Rawfile']
    tdf = tdf[tdf['update_tier'] == 'Greater Than One Week']
    undefined = tdf[key_col].tolist()
    msg = ''
    if len(tdf) > 0:
        msg += ('The following raw files have not been updated for '
                'over a week: {}\n\n'.format(','.join(undefined)))
    tdf = df[df[key_col].str[:3] == 'API']
    tdf = tdf[tdf[key_col].str[:len('API_Rawfile')] != 'API_Rawfile']
    tdf = tdf[tdf['update_tier'] != 'Today']
    if len(tdf) > 0:
        api_undefined = tdf[key_col].tolist()
        msg += ('The following API files did not update today: '
                ' {}\n'.format(','.join(api_undefined)))
        undefined.extend(api_undefined)
    return undefined, msg


def update_automatic_requests(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        fix_type = az.Analyze.unknown_col
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis.data:
            tdf = pd.DataFrame(analysis.data)
            for col in tdf.columns:
                tdf[col] = tdf[col].str.strip("'")
            cols = [x for x in tdf.columns if x != vmc.vendorkey]
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
            undefined, msg = get_vendor_keys_of_update_files(df)
            update_single_auto_request(processor_id, current_user_id,
                                       fix_type=fix_type,
                                       fix_description=msg,
                                       undefined=undefined)
        fix_type = az.Analyze.missing_metrics
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis.data:
            df = pd.DataFrame(analysis.data)
            un = df[az.Analyze.missing_metrics].str.join(',')
            un = (df[dctc.VEN] + ' - ' + un).to_list()
            msg = ('{} {}\n\n'.format(analysis.message, ','.join(un)))
            update_single_auto_request(processor_id, current_user_id,
                                       fix_type=fix_type,
                                       fix_description=msg,
                                       undefined=un)
        else:
            undefined = []
            msg = '{}'.format(analysis.message)
            update_single_auto_request(processor_id, current_user_id,
                                       fix_type=fix_type,
                                       fix_description=msg,
                                       undefined=undefined)
        fix_type = az.Analyze.max_api_length
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis.data:
            df = pd.DataFrame(analysis.data)
            undefined = (df[vmc.vendorkey]).to_list()
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
        fix_type = az.Analyze.double_counting_all
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis.data:
            df = pd.DataFrame(analysis.data)
            cols = [dctc.VEN, vmc.vendorkey, az.CheckDoubleCounting.metric_col]
            undefined = app_utl.column_contents_to_list(df, cols)
            msg = (
                '{} {}\n\n'.format(analysis.message, ','.join(undefined)))
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
        fix_type = az.Analyze.double_counting_partial
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis:
            if analysis.data:
                df = pd.DataFrame(analysis.data)
                undefined = (df[dctc.VEN] + ' - ' + df[vmc.vendorkey] + ' - ' +
                             df['Metric'] +
                             ': Proportion of duplicate placements - ' +
                             df['Num Duplicates'] + '/' +
                             df['Total Num Placements']).to_list()
                msg = '{} {}\n\n'.format(analysis.message, ','.join(undefined))
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
        fix_type = az.Analyze.placement_col
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis.data:
            df = pd.DataFrame(analysis.data)
            undefined = (df['Vendor Key'] + ': Current Placement Column= ' +
                         df['Current Placement Col'] +
                         ' | Suggested= ' + df['Suggested Col']).to_list()
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
        fix_type = az.Analyze.missing_flat
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis.data:
            df = pd.DataFrame(analysis.data)
            cols = [dctc.VEN, dctc.PKD, dctc.PD, vmc.clicks]
            df = utl.data_to_type(df, str_col=cols)
            if all(x in df.columns for x in cols):
                undefined = (df[dctc.VEN] + ' - ' + df[dctc.PKD] + ' - '
                             + df[dctc.PD] + ': Clicks = '
                             + df[vmc.clicks]).to_list()
            else:
                undefined = []
            msg = ('{} {}\n\n'.format(analysis.message, ', '.join(undefined)))
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
        fix_type = az.Analyze.missing_ad_rate
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis:
            if analysis.data:
                df = pd.DataFrame(analysis.data)
                cols = [dctc.SRV, dctc.AM, dctc.AR]
                undefined = app_utl.column_contents_to_list(df, cols)
                msg = ('{} {}\n\n'.format(analysis.message, ', '
                                          .join(undefined)))
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
        fix_type = az.Analyze.missing_serving
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis:
            if analysis.data:
                df = pd.DataFrame(analysis.data)
                cols = [vmc.vendorkey, dctc.PN, dctc.SRV]
                undefined = app_utl.column_contents_to_list(df, cols)
                msg = ('{} {}\n\n'.format(analysis.message, ', '
                                          .join(undefined)))
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
        fix_type = az.Analyze.non_mp_placement_col
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=fix_type).first()
        if analysis:
            if analysis.data:
                df = pd.DataFrame(analysis.data)
                cols = az.CheckPlacementsNotInMp.cols
                undefined = app_utl.column_contents_to_list(df, cols)
                msg = ('{} {}\n\n'.format(analysis.message, ', '
                                          .join(undefined)))
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


@error_handler
def update_analysis_in_db_reporting_cache(processor_id, current_user_id, df,
                                          dimensions, metrics, filter_dict,
                                          check=False, commit=True):
    cur_processor = db.session.get(Processor, processor_id)
    dimensions_str = '|'.join(dimensions)
    metrics_str = '|'.join(metrics)
    if not filter_dict:
        filter_dict = {}
    filter_dict = {k: v for x in filter_dict for k, v in x.items() if v}
    filter_col_str = '|'.join(filter_dict.keys())
    filter_val = []
    for k, v in filter_dict.items():
        if v:
            new_list = v
            if k == 'eventdate':
                old_date = '%Y-%m-%dT%H:%M:%S.%fZ'
                new_date = '%Y-%m-%d'
                sd = datetime.strptime(v[0], old_date).strftime(new_date)
                ed = datetime.strptime(v[1], old_date).strftime(new_date)
                new_list = [sd, ed]
            filter_val.append(','.join(new_list))
    filter_val_str = '|'.join(filter_val)
    old_analysis = ProcessorAnalysis.query.filter_by(
        processor_id=cur_processor.id, key=az.Analyze.database_cache,
        parameter=dimensions_str, parameter_2=metrics_str,
        filter_col=filter_col_str, filter_val=filter_val_str).first()
    if check:
        return old_analysis
    if old_analysis:
        old_analysis.data = df.to_json()
        old_analysis.date = datetime.today().date()
        if commit:
            db.session.commit()
        cur_analysis = old_analysis
    else:
        new_analysis = ProcessorAnalysis(
            key=az.Analyze.database_cache, parameter=dimensions_str,
            parameter_2=metrics_str, filter_col=filter_col_str,
            filter_val=filter_val_str, data=df.to_json(),
            processor_id=cur_processor.id, date=datetime.today().date())
        db.session.add(new_analysis)
        if commit:
            db.session.commit()
        cur_analysis = new_analysis
    return cur_analysis


def update_analysis_in_db(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
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
            if (not analysis_dict_val and
                    analysis.key not in [az.Analyze.database_cache,
                                         az.Analyze.brandtracker_imports]):
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


def add_text_body(text_body, msg, tab, key=None, param=None, param2=None,
                  split=None, style=None):
    text_dict = {'key': key, 'parameter': param, 'parameter_2': param2,
                 'split_col': split, 'message': '{}\n'.format(msg), 'tab': tab,
                 'format': style, 'selected': 'true'}
    text_body.append(text_dict)
    return text_body


def add_analysis_to_text_body(text_body, analysis, tab, header=None, table=True,
                              email=True):
    analysis_dict = analysis.to_dict()
    analysis_dict['message'] = '{}\n'.format(analysis_dict['message'])
    analysis_dict['tab'] = tab
    analysis_dict['selected'] = 'true'
    if analysis.data and table:
        df = pd.DataFrame(analysis.data)
        if not df.empty:
            lt = app_utl.LiquidTable(df=df, table_name=header,
                                     col_filter=False, chart_btn=False,
                                     search_bar=False)
            analysis_dict['data'] = lt.table_dict
            if email:
                analysis_dict['df'] = df
    else:
        del analysis_dict['data']
    text_body.append(analysis_dict)
    return text_body


def analysis_email_basic(processor_id, current_user_id, text_body, header,
                         full_analysis, analysis_keys, tab=0, param='key',
                         email=True):
    try:
        analysis = [
            x for x in full_analysis if x.__dict__[param] in analysis_keys]
        text_body = add_text_body(text_body, header, tab=tab,
                                  key=analysis_keys, style='HEADING_1')
        for a in analysis:
            text_body = add_analysis_to_text_body(text_body, a, tab, header,
                                                  email=email)
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return []


def analysis_email_kpi(processor_id, current_user_id, text_body, header,
                       full_analysis, analysis_keys, email=True):
    try:
        analysis = [x for x in full_analysis if x.key in analysis_keys]
        text_body = add_text_body(text_body, header, tab=0, key=analysis_keys,
                                  style='HEADING_1')
        kpis = set(x.parameter for x in analysis
                   if x.parameter not in ['0', 'nan'])
        for kpi in kpis:
            text_body = add_text_body(text_body, kpi, tab=1,
                                      key=analysis_keys[0], style='HEADING_2')
            cur_analysis = [x for x in analysis if x.parameter == kpi]
            text_body = add_text_body(text_body, 'Partner', tab=2,
                                      key=analysis_keys[0], param=kpi,
                                      style='HEADING_3')
            par_analysis = [x for x in cur_analysis if x.split_col == dctc.VEN]
            for a in par_analysis:
                text_body = add_analysis_to_text_body(text_body, a, 3, header,
                                                      email=email)
                partners = pd.DataFrame(a.data)[dctc.VEN].to_list()
                for p in partners:
                    text_body = add_text_body(text_body, p, 3,
                                              key=a.key, param=a.parameter,
                                              param2=a.parameter_2,
                                              split=a.split_col,
                                              style='HEADING_4')
                    ind_par_anlaysis = [x for x in cur_analysis
                                        if x.filter_val == p
                                        and x.parameter_2 == a.parameter_2]
                    for ind_par in ind_par_anlaysis:
                        text_body = add_analysis_to_text_body(
                            text_body, ind_par, 4, table=False, email=email)
            text_body = add_text_body(text_body, vmc.date, tab=2,
                                      key=analysis_keys[0], param=kpi,
                                      style='HEADING_3')
            date_analysis = [x for x in cur_analysis if x.split_col == vmc.date]
            for d in date_analysis:
                text_body = add_analysis_to_text_body(text_body, d, 3, header,
                                                      email=email)
        return text_body
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return []


def build_processor_analysis_email(processor_id, current_user_id, email=True):
    try:
        _set_task_progress(0)
        cur_processor = db.session.get(Processor, processor_id)
        analysis = cur_processor.processor_analysis.all()
        text_body = []
        arguments = [
            ('TOPLINE', [az.Analyze.topline_col], analysis_email_basic),
            ('DELIVERY',
             [az.Analyze.delivery_col, az.Analyze.delivery_comp_col],
             analysis_email_basic),
            ('KPI ANALYSIS', [az.Analyze.kpi_col], analysis_email_kpi)
        ]
        if email:
            arguments.append(('REPORTING QA', [az.Analyze.unknown_col,
                                               az.Analyze.raw_file_update_col],
                              analysis_email_basic))
        for arg in arguments:
            text_body = arg[2](
                processor_id, current_user_id, text_body=text_body,
                header=arg[0], full_analysis=analysis,
                analysis_keys=arg[1], email=email)
        _set_task_progress(100)
        return [text_body]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return []


def update_report_in_db(processor_id, current_user_id,
                        new_data=None, report_name='Auto',
                        report_date=datetime.utcnow().date()):
    try:
        _set_task_progress(0)
        if not new_data:
            text_body = build_processor_analysis_email(
                processor_id, current_user_id, email=False)
            new_data = json.dumps({'report': text_body})
            report_date = datetime.utcnow().date()
        old_report = ProcessorReports.query.filter_by(
            processor_id=processor_id, report_name=report_name,
            report_date=report_date).first()
        if old_report:
            old_report.report = new_data
        else:
            processor_report = ProcessorReports(
                processor_id=processor_id, user_id=current_user_id,
                report_name=report_name, report=new_data,
                report_date=report_date)
            db.session.add(processor_report)
        db.session.commit()
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        return False


def write_report_builder(processor_id, current_user_id, new_data=None):
    try:
        _set_task_progress(0)
        cur_processor = db.session.get(Processor, processor_id)
        os.chdir(adjust_path(cur_processor.local_path))
        new_report = json.loads(new_data)
        report_name = new_report['name']
        report_date = new_report['report_date']
        report_data = new_report['report'][0]
        update_report_in_db(processor_id, current_user_id, new_data,
                            report_name, report_date)
        s3 = awss3.S3()
        s3.input_config()
        for ind in range(len(report_data)):
            if report_data[ind]['selected'] == 'false':
                continue
            if 'data' in report_data[ind]:
                if 'imgURI' in report_data[ind]['data']['cols']:
                    name = report_data[ind]['data']['data'][0]['name']
                    key = os.path.join('screenshots', 'charts', name)
                    img_uri = report_data[ind]['data']['data'][0]['imgURI']
                    data = utl.base64_to_binary(img_uri)
                    url = s3.s3_upload_file_get_presigned_url(data, key)
                    report_data[ind]['url'] = url
                else:
                    report_data[ind]['df'] = pd.DataFrame(
                        data=report_data[ind]['data']['data'])
        if 'sendEmail' in new_report['saveOptions']:
            user = User.query.get(current_user_id)
            send_email('[Liquid App] {} | Analysis | {}'.format(
                cur_processor.name,
                datetime.today().date().strftime('%Y-%m-%d')),
                sender=app.config['ADMINS'][0],
                recipients=[user.email],
                text_body=render_template(
                    'email/processor_analysis.txt', user=user,
                    processor_name=cur_processor.name,
                    analysis=report_data),
                html_body=render_template(
                    'email/processor_analysis.html', user=user,
                    processor_name=cur_processor.name,
                    analysis=report_data),
                sync=True)
        if 'saveGoogleDoc' in new_report['saveOptions']:
            title = '-'.join([cur_processor.name, report_name, report_date])
            gs = gsapi.GsApi()
            gs.input_config(gs.default_config)
            gs.get_client()
            doc_id = gs.create_google_doc(title)
            r = gs.add_text(doc_id, report_data)
        _set_task_progress(100)
        return True
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                processor_id, current_user_id), exc_info=sys.exc_info())
        return False


@error_handler
def get_kpis_for_processor(processor_id, current_user_id):
    cur_processor = db.session.get(Processor, processor_id)
    vc = az.ValueCalc()
    analysis = cur_processor.processor_analysis.filter_by(
        key=az.Analyze.kpi_col).all()
    kpis = list(set(x.parameter for x in analysis
                    if x.parameter not in ['0', 'nan', 'CPA'] and
                    'Conv' not in x.parameter))
    if not kpis:
        kpis = ['CPC', 'CPLPV', 'CPBC', 'CPV', 'VCR']
    kpi_formula = [vc.calculations[x] for x in vc.calculations
                   if vc.calculations[x][vc.metric_name] in kpis]
    kpi_cols = [x[vc.formula][::2] for x in kpi_formula]
    kpi_cols = list(set([x for x in kpi_cols for x in x if x]))
    kpi_cols += [x for x in kpis if x in vmc.datacol]
    kpis = [x for x in kpis if x not in vmc.datacol]
    kpi_cols = list(utl.db_df_translation(
        kpi_cols, adjust_path(cur_processor.local_path)).values())
    return kpis, kpi_cols


def parse_date_from_project_number(cur_string, date_opened):
    try:
        sd = cur_string.strip().split('/')
        if len(sd) > 2:
            cur_year = int(sd[2])
            if len(sd[2]) <= 2:
                cur_year += 2000
        else:
            cur_year = date_opened.year
        sd = datetime(cur_year, int(sd[0]), int(sd[1]))
        return sd
    except:
        return None


def get_last_rate_card_for_client(cur_campaign):
    cur_product_name = cur_campaign.product.name
    cur_client_name = cur_campaign.product.client.name
    rate_query = Processor.query \
        .join(Processor.campaign) \
        .join(Campaign.product) \
        .join(Product.client) \
        .filter(Client.name == cur_client_name,
                Product.name == cur_product_name) \
        .order_by(Processor.id.desc()) \
        .limit(1) \
        .with_entities(Processor.rate_card_id, Processor.id) \
        .first()
    if not rate_query:
        rate_query = Processor.query \
            .join(Processor.campaign) \
            .join(Campaign.product) \
            .join(Product.client) \
            .filter(Client.name == cur_client_name) \
            .order_by(Processor.id.desc()) \
            .limit(1) \
            .with_entities(Processor.rate_card_id, Processor.id) \
            .first()
    rate_id, proc_id = rate_query if rate_query else (None, None)
    return rate_id, proc_id


@error_handler
def get_project_numbers(processor_id, running_user=None, spec_args=None,
                        filter_dict=None):
    if os.path.exists(Processor.__table__.name):
        os.chdir(Processor.__table__.name)
    api = gsapi.GsApi()
    api.input_config('gsapi.json')
    api.sheet_id = '1kCTyf6klPrd1Dy7xonyk6TPMSe0gm0FQO2s7IpHP7VU'
    df = api.get_data()
    df = utl.first_last_adj(df, 3, 0)
    df = df.rename_axis(None, axis=1).rename_axis(
        'index', axis=0).reset_index(drop=True)
    df = df.sort_index(ascending=False)
    pn_max = db.session.get(ProjectNumberMax, 1)
    client_col = Client.__name__
    ndf = df[(~df[client_col].isna())]
    if pn_max:
        ndf = ndf[(ndf.index >= pn_max.max_number)]
    pn_col = """# (It's a formula)"""
    date_col = 'Date Opened'
    flight_col = 'FLIGHT DATES'
    aly = az.Analyze(load_chat=True, chat_path=utl.config_path)
    for pn in ndf.to_dict(orient='records'):
        c_project = Project.query.filter_by(project_number=pn[pn_col]).first()
        if not c_project:
            cur_client = Client.query.filter_by(name=pn[client_col]).first()
            if not cur_client:
                cur_client = Client(name=pn[client_col])
                db.session.add(cur_client)
                db.session.commit()
            date_opened = None
            if pn[date_col]:
                try:
                    date_opened = datetime.strptime(pn[date_col], '%m/%d/%y')
                except:
                    pass
            if pn[flight_col]:
                flight = pn[flight_col].split(
                    '-' if '-' in pn[flight_col] else 'to')
                sd = parse_date_from_project_number(flight[0], date_opened)
                if len(flight) > 1:
                    ed = parse_date_from_project_number(flight[1], date_opened)
                else:
                    ed = sd
            else:
                sd = ed = None
            name = pn['Project']
            model_ids, words = aly.chat.find_db_model(Product, name)
            product_name = 'None'
            if model_ids:
                for k in model_ids:
                    cur_product = db.session.get(Product, k)
                    if cur_product.client.name == cur_client.name:
                        product_name = cur_product.name
                        break
            form_product = Product(name=product_name,
                                   client_id=cur_client.id).check_and_add()
            if not name:
                continue
            for char in ['_', '|', ':', '.', "'", '&', '/']:
                name = name.replace(char, ' ')
            form_campaign = Campaign(
                name=pn['Project'],
                product_id=form_product.id).check_and_add()
            new_project = Project(
                project_number=pn[pn_col],
                initial_project_number=pn['initial PN'],
                client_id=cur_client.id, project_name=pn['Project'],
                media=True if pn['Media'] else False,
                creative=True if pn['Creative'] else False,
                date_opened=date_opened, flight_start_date=sd,
                flight_end_date=ed, exhibit=pn['Exhibit #'],
                sow_received=pn["SOW rec'd"],
                billing_dates=pn['Billings + date(s)'], notes=pn['NOTES'],
                campaign_id=form_campaign.id)
            db.session.add(new_project)
            db.session.commit()
            description = ('Automatically generated from '
                           'project number: {}').format(pn[pn_col])
            new_processor = Processor.query.filter_by(name=name).first()
            if not new_processor:
                cu = User.query.filter_by(username='ALI').first()
                if cu:
                    user_id = cu.id
                else:
                    user_id = running_user
                rate_card_id, old_proc_id = get_last_rate_card_for_client(
                    form_campaign)
                new_processor = Processor(
                    name=name, description=description,
                    user_id=user_id, created_at=datetime.utcnow(),
                    start_date=sd, end_date=ed,
                    campaign_id=form_campaign.id)
                if rate_card_id:
                    old_proc = db.session.get(old_proc_id)
                    sync_cols = [Processor.digital_agency_fees,
                                 Processor.trad_agency_fees,
                                 Processor.rate_card_id,
                                 Processor.dcm_service_fees]
                    for col in sync_cols:
                        col = col.name
                        setattr(new_processor, col, old_proc.__dict__[col])
                db.session.add(new_processor)
                db.session.commit()
            new_processor.projects.append(new_project)
            db.session.commit()
    if not ndf.empty:
        pn_max.max_number = max(ndf.index)
        db.session.commit()
    return [df]


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
            'Unhandled exception - User {} running_user - {}'.format(
                user_id, running_user), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


@error_handler
def update_tutorial(user_id, running_user, tutorial_name, new_data,
                    new_data_is_df=False):
    cur_tutorial = Tutorial.query.filter_by(name=tutorial_name).first()
    if not cur_tutorial:
        cur_tutorial = Tutorial(name=tutorial_name)
        db.session.add(cur_tutorial)
        db.session.commit()
    if new_data_is_df:
        df = new_data
    else:
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
    return True


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
            'Unhandled exception - User {} running_user - {}'.format(
                user_id, running_user), exc_info=sys.exc_info())
        return False


def get_raw_file_comparison(processor_id, current_user_id, vk):
    try:
        if vk == '':
            msg = 'No vendor key save the new card first then retry.'
            config_file = {
                'No Vendor Key': {'Old': (False, msg), 'New': (False, msg)}}
        else:
            _set_task_progress(0)
            cur_processor = db.session.get(Processor, processor_id)
            os.chdir(adjust_path(cur_processor.local_path))
            matrix = vm.VendorMatrix()
            aly = az.Analyze(matrix=matrix)
            _set_task_progress(25)
            aly.compare_raw_files(vk)
            _set_task_progress(85)
            file_name = "{}.json".format(vk)
            file_name = os.path.join(utl.tmp_file_suffix, file_name)
            with open(file_name, 'r') as f:
                config_file = json.load(f)
        config_file = {'{:02}|{}'.format(idx, k): v for idx, (k, v) in
                       enumerate(config_file.items())}
        tables = [config_file]
        _set_task_progress(100)
        return tables
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        return [{'Error': {'Old': 'An error occurred with one or both files.',
                           'New': 'An error occurred with one or both files.'}}]


@error_handler
def write_raw_file_from_tmp(processor_id, current_user_id, vk, new_data):
    cur_processor = db.session.get(Processor, processor_id)
    os.chdir(adjust_path(cur_processor.local_path))
    matrix = vm.VendorMatrix()
    data_source = matrix.get_data_source(vk=vk)
    file_name = data_source.p[vmc.filename_true]
    file_type = os.path.splitext(file_name)[1]
    tmp_file_name = data_source.p[vmc.filename_true].replace(
        file_type, 'TMP{}'.format(file_type))
    if os.path.exists(tmp_file_name):
        copy_file(tmp_file_name, file_name, max_attempts=10)
    df = pd.read_json(new_data)
    new_first_line = df.iloc[0][1]
    check_first_row = az.CheckFirstRow(az.Analyze(matrix=matrix))
    check_first_row.adjust_first_row_in_vm(vk, new_first_line, write=True)
    return True


def test_api_connection(processor_id, current_user_id, vk):
    try:
        if vk == '':
            df = pd.DataFrame([{
                'Result': 'No vendor key save the new card first then retry.'}])
            lt = app_utl.LiquidTable(df=df, table_name='modal-body-table',
                                     col_filter=False)
        else:
            _set_task_progress(0)
            cur_processor = Processor.query.get(processor_id)
            os.chdir(adjust_path(cur_processor.local_path))
            matrix = vm.VendorMatrix()
            test = ih.ImportHandler('all', matrix)
            df = test.test_api_calls([vk])
            lt = app_utl.LiquidTable(df=df, table_name='modal-body-table',
                                     highlight_row='Success',
                                     highlight_type='', chart_btn=False)
        _set_task_progress(100)
        return [lt.table_dict]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} VK {}'.format(
                processor_id, current_user_id, vk), exc_info=sys.exc_info())
        df = pd.DataFrame([{'Result': 'CONFIG WAS UNABLE TO BE LOADED.'}])
        lt = app_utl.LiquidTable(df=df, table_name='modal-body-table',
                                 col_filter=False)
        return [lt.table_dict]


def apply_quick_fix(processor_id, current_user_id, fix_id, vk=None):
    try:
        _set_task_progress(0)
        cur_processor = Processor.query.get(processor_id)
        cur_fix = Requests.query.get(fix_id)
        cur_path = adjust_path(os.path.abspath(os.getcwd()))
        os.chdir(adjust_path(cur_processor.local_path))
        matrix = vm.VendorMatrix()
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_processor.id, key=cur_fix.fix_type).first()
        os.chdir(cur_path)
        if cur_fix.fix_type in [az.Analyze.unknown_col]:
            df = get_dictionary(processor_id, current_user_id, vk)[0]
            os.chdir(adjust_path(cur_processor.local_path))
            tdf = pd.DataFrame(analysis.data)
            ds = matrix.get_data_source(vk=vk)
            fpn = ds.p[vmc.fullplacename]
            for col in fpn:
                tdf[col] = tdf[col].str.strip("'")
            tdf = vm.full_placement_creation(
                tdf, vk, vmc.fullplacename, ds.p[vmc.fullplacename])
            fpn += [vmc.fullplacename]
            tdf = tdf[fpn].drop_duplicates(subset=[vmc.fullplacename])
            df = pd.concat([df, tdf], ignore_index=True, sort=False)
        elif cur_fix.fix_type == az.Analyze.raw_file_update_col:
            df = get_vendormatrix(processor_id, current_user_id)[0]
            os.chdir(adjust_path(cur_processor.local_path))
            tdf = pd.DataFrame(analysis.data)
            undefined, msg = get_vendor_keys_of_update_files(tdf)
            for old_vk in undefined:
                new_vk = old_vk.replace('API_', '')
                df[vmc.vendorkey] = df[vmc.vendorkey].replace(old_vk, new_vk)
        elif cur_fix.fix_type == az.Analyze.max_api_length:
            df = get_vendormatrix(processor_id, current_user_id)[0]
            os.chdir(adjust_path(cur_processor.local_path))
            tdf = pd.DataFrame(analysis.data).to_dict(orient='records')
            for x in tdf:
                vk = x[vmc.vendorkey]
                max_date_length = x[az.Analyze.max_api_length]
                ndf = df[df[vmc.vendorkey] == vk].reset_index(drop=True)
                new_sd = datetime.strptime(
                    ndf[vmc.startdate][0], '%Y-%m-%d') + dt.timedelta(
                    days=max_date_length - 3)
                if new_sd.date() >= dt.datetime.today().date():
                    new_sd = dt.datetime.today() - dt.timedelta(days=3)
                new_str_sd = new_sd.strftime('%Y-%m-%d')
                ndf.loc[0, vmc.startdate] = new_str_sd
                ndf.loc[0, vmc.enddate] = ''
                new_vk = '{}_{}'.format('_'.join(vk.split('_')[:2]), new_str_sd)
                ndf.loc[0, vmc.vendorkey] = new_vk
                file_type = os.path.splitext(ndf[vmc.filename][0])[1].lower()
                new_fn = '{}{}'.format(new_vk.replace('API_', '').lower(),
                                       file_type)
                ndf.loc[0, vmc.filename] = new_fn
                idx = df[df[vmc.vendorkey] == vk].index
                df.loc[idx, vmc.vendorkey] = df.loc[
                    idx, vmc.vendorkey][idx[0]].replace('API_', '')
                old_ed = new_sd - dt.timedelta(days=1)
                df.loc[idx, vmc.enddate] = old_ed.strftime('%Y-%m-%d')
                df = pd.concat([df, ndf]).reset_index(drop=True)
        elif cur_fix.fix_type == az.Analyze.double_counting_all:
            df = get_vendormatrix(processor_id, current_user_id)[0]
            os.chdir(adjust_path(cur_processor.local_path))
            tdf = pd.DataFrame(analysis.data).to_dict(orient='records')
            for x in tdf:
                vks = x[vmc.vendorkey].split(',')
                if any('Rawfile' in y for y in vks):
                    vk = [y for y in vks if 'Rawfile' in y][0]
                    idx = df[df[vmc.vendorkey] == vk].index
                    df.loc[idx, x['Metric']] = ''
                elif (any('DCM' in y for y in vks) or
                      any('Sizmek' in y for y in vks)):
                    vks = [y for y in vks if 'DCM' in y]
                    if not vks:
                        vks = [y for y in vks if 'Sizmek' in y]
                    for vk in vks:
                        idx = df[df[vmc.vendorkey] == vk].index
                        if (x['Metric'] == vmc.clicks or
                                x['Metric'] == vmc.impressions):
                            df.loc[idx, 'RULE_1_QUERY'] = (
                                    df.loc[idx, 'RULE_1_QUERY'][idx[0]] + ',' +
                                    x[dctc.VEN])
                        else:
                            if not df.loc[idx, 'RULE_6_QUERY'].any():
                                df.loc[idx, 'RULE_6_FACTOR'] = 0.0
                                df.loc[idx, 'RULE_6_METRIC'] = ('POST' + '::' +
                                                                x['Metric'])
                                df.loc[idx, 'RULE_6_QUERY'] = x[dctc.VEN]
                            else:
                                df.loc[idx, 'RULE_6_METRIC'] = (
                                        df.loc[idx, 'RULE_6_QUERY'][idx[0]] +
                                        '::' + x['Metric'])
                                df.loc[idx, 'RULE_6_QUERY'] = (
                                        df.loc[idx, 'RULE_6_QUERY'][idx[0]] +
                                        ',' + [dctc.VEN])
                else:
                    unavail_msg = ('QUICK FIX UNAVAILABLE. '
                                   'CHECK DUPLICATE DATASOURCES AND RAWFILES')
                    df = pd.DataFrame([{'Result': unavail_msg}])
        elif cur_fix.fix_type == az.Analyze.non_mp_placement_col:
            df = get_translation_dict(processor_id, current_user_id)[0]
            os.chdir(adjust_path(cur_processor.local_path))
            tdf = pd.DataFrame(analysis.data).to_dict(orient='records')
            for x in tdf:
                old_val = x[dctc.PN]
                blank = ''
                trans = [[blank, old_val, blank, blank, blank]]
                tdf = pd.DataFrame(trans, columns=df.columns)
                df = pd.concat([df, tdf], ignore_index=True, sort=False)
        elif cur_fix.fix_type == az.Analyze.placement_col:
            df = get_vendormatrix(processor_id, current_user_id)[0]
            os.chdir(adjust_path(cur_processor.local_path))
            tdf = pd.DataFrame(analysis.data).to_dict(orient='records')
            for x in tdf:
                vk = x[vmc.vendorkey]
                idx = df[df[vmc.vendorkey] == vk].index
                df.loc[idx, vmc.placement] = x['Suggested Col']
        else:
            df = pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])
        _set_task_progress(100)
        return [df]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {} Fix ID {}'.format(
                processor_id, current_user_id, fix_id), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_request_table(processor_id, current_user_id, fix_id):
    try:
        cur_proc = Processor.query.filter_by(id=processor_id).first_or_404()
        analysis = cur_proc.get_requests_processor_analysis(fix_id)
        if analysis and analysis.data:
            df = pd.DataFrame(analysis.data)
            msg = analysis.message
        else:
            df = pd.DataFrame()
            msg = ''
        _set_task_progress(100)
        return [df, msg]
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id), exc_info=sys.exc_info())
        msg = 'DATA WAS UNABLE TO BE LOADED.'
        return [pd.DataFrame([{'Result': msg}]), msg]


@error_handler
def get_sow(plan_id, current_user_id):
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors
    from reportlab.platypus import Paragraph, Table, TableStyle
    from reportlab.lib.units import inch
    cur_plan = db.session.get(Plan, plan_id)
    cur_sow = Sow.query.filter_by(plan_id=cur_plan.id).first()
    if not cur_sow:
        cur_sow = Sow()
        cur_sow.create_from_plan(cur_plan)
        db.session.add(cur_sow)
        db.session.commit()
    file_name = "SOW_{}.pdf".format(plan_id)
    c = canvas.Canvas(file_name, pagesize=letter)
    width, height = A4
    c.setPageSize((width, height))
    c.setFont('Helvetica-Bold', 10)
    t_pos = 780
    c.drawCentredString(290, t_pos, "STATEMENT OF WORK")
    c.line(230, t_pos - 3, 350, t_pos - 3)
    c.drawCentredString(
        290, t_pos + 15, '{} - {} -  - MARKETING CAMPAIGN'.format(
            cur_sow.client_name, cur_sow.campaign))
    c.setFont('Helvetica-Bold', 8)
    c.drawString(72, t_pos - 30, "Project Overview")
    c.setFont('Helvetica-Bold', 7)
    c.drawString(100, t_pos - 50, "Project: {}".format(
        cur_sow.project_name))
    cont = "Advertiser project contact: {}".format(cur_sow.project_contact)
    c.drawString(100, t_pos - 60, cont)
    c.drawString(100, t_pos - 70, "Date submitted: {}".format(
        cur_sow.date_submitted.strftime("%m-%d-%Y")))
    c.drawString(100, t_pos - 80, "Total Project Budget: " + "${}".format(
        cur_sow.total_project_budget))
    c.drawString(350, t_pos - 50,
                 "Liquid project contact: " + cur_sow.liquid_contact)
    c.drawString(350, t_pos - 60, "Liquid project #: {}".format(
        cur_sow.liquid_project))
    c.drawString(100, t_pos - 100, "Flight dates: {} - {}".format(
        cur_sow.start_date.strftime("%m/%d/%Y"),
        cur_sow.end_date.strftime("%m/%d/%Y")))
    data = []
    for cur_phase in cur_plan.phases:
        cur_partners = cur_phase.partners.all()
        new_data = [
            {'Description': p.name, 'Total Net Dollars': p.total_budget,
             'Vendor': p.partner_type} for p in cur_partners]
        data.extend(new_data)
    df = pd.DataFrame(data)
    net_media = 0
    sum_by_cat = pd.DataFrame()
    data1 = [['', '']]
    if not df.empty:
        df['Vendor'] = df['Vendor'].fillna('Digital')
        df = df.groupby(['Description', 'Vendor'])[
            'Total Net Dollars'].sum().reset_index()
        data1 = df[['Description', 'Total Net Dollars']].to_dict(
            orient='records')
        data1 = [[x['Description'], '${:0,.2f}'.format(x['Total Net Dollars'])]
                 for x in data1]
        data1.insert(0, ['Description', 'Total Net Dollars'])
        df['Total Net Dollars'] = df['Total Net Dollars'].astype(float)
        net_media = df['Total Net Dollars'].sum()
        sum_by_cat = df.groupby('Vendor')['Total Net Dollars'].sum()
        sum_by_cat.reset_index(name='Total Net Dollars')
    digital = 0
    program = 0
    trad = 0
    if 'Digital' in sum_by_cat:
        digital = sum_by_cat['Digital'] * 0.075
    if 'Programmatic' in sum_by_cat:
        program = sum_by_cat['Programmatic'] * 0.125
    if 'Traditional' in sum_by_cat:
        trad = sum_by_cat['Traditional'] * 0.045
    ag_fee = digital + trad
    as_cost = cur_sow.ad_serving if cur_sow.ad_serving else 0
    camp_ttl = net_media + ag_fee + float(as_cost) + program
    styles = getSampleStyleSheet()
    style_n = styles["BodyText"]
    last_row = Paragraph(
        ('<b>Total Due To Liquid: Billed upon campaign commencement. '
         'Payment terms are net 30 days.</b>'),
        style_n)

    data2 = [
        ["", ""],
        ["Net Media", '${:0,.2f}'.format(net_media)],
        ["Agency Fees", '${:0,.2f}'.format(ag_fee)],
        ["Adserving Fees", '${:0,.2f}'.format(as_cost)],
        ["Programmatic Agency Fees", '${:0,.2f}'.format(program)],
        ["", ""],
        ["Campaign Total", '${:0,.2f}'.format(camp_ttl)],
        [last_row, '${:0,.2f}'.format(camp_ttl)]
    ]

    all_data = data1 + data2

    grid = [('GRID', (0, 0), (-1, -1), 0.7, colors.black),
            ('FONTNAME', (0, 0), (1, 0), 'Helvetica-Bold'),
            ('BACKGROUND', (0, 0), (1, 0), colors.lightgrey),
            ('FONTNAME', (-1, 0), (0, -1), 'Helvetica')]
    table = Table(all_data, style=TableStyle(grid), colWidths=[250, 220])
    table.setStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black)])

    data_len = len(all_data)
    table.setStyle(TableStyle([('FONTNAME', (0, data_len - 1),
                                (1, data_len - 1), 'Helvetica-Bold'),
                               ('BACKGROUND', (0, data_len - 1),
                                (-1, data_len - 1), colors.lightgrey),
                               ]))
    aw = width
    ah = height
    w1, h1 = table.wrap(aw, ah)  # find required
    if width <= aw and height <= ah:
        table.drawOn(c, inch, height - h1 - inch * 2.5)
    else:
        raise ValueError
    c.save()
    pdf_file = get_file_in_memory(file_name, file_name='sow.pdf')
    os.remove(file_name)
    _set_task_progress(100)
    return [pdf_file]


def make_io_table(data, font_name='Helvetica-Bold', font_size=5, padding=10):
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.pdfbase import pdfmetrics
    from reportlab.platypus import Table
    df = pd.DataFrame(data)
    df.dropna(axis=1, how='all', inplace=True)
    df['planned_units'] = df['planned_units'].apply(
        lambda x: f"{float(x):,.0f}" if pd.notna(x) else x)
    df['total_net_cost'] = df['total_net_cost'].apply(
        lambda x: f"${float(x):,.2f}" if pd.notna(x) else x)
    df['CPU'] = df['CPU'].apply(
        lambda x: f"${float(x):,.2f}" if pd.notna(x) else x)
    data = [df.columns.tolist()] + df.values.tolist()
    table_start_position = 20
    total_width = landscape(A4)[0] - 1 * table_start_position
    max_widths = []
    for col in zip(*data):
        max_content_width = max(
            pdfmetrics.stringWidth(str(cell), font_name, font_size) + padding
            for cell in col if cell is not None)
        max_widths.append(max_content_width)
    remaining_width = total_width - sum(max_widths)
    number_of_columns = len(max_widths)
    additional_width_per_column = remaining_width / number_of_columns
    max_widths = [width + additional_width_per_column for width in max_widths]
    headers = data[0]
    total_row = ['Total']
    for header in headers[1:]:
        col_data = [row[headers.index(header)] for row in data[1:]]
        if header in ['flight start', 'flight end'] or all(
                isinstance(cell, (str,)) and '-' in cell for cell in col_data if
                cell is not None):
            total_row.append(col_data[0])
        elif header == 'total_net_cost':
            total_value = sum(
                float(cell.replace("$", "").replace(
                    ",", "")) for cell in col_data if
                cell is not None and isinstance(cell, str) and "$" in cell)
            total_row.append(f"${total_value:,.2f}")
        elif header == 'planned_units':
            total_value = sum(
                float(cell.replace(",", "")) for cell in col_data
                if cell is not None and isinstance(cell, str))
            total_row.append(f"{total_value:,.0f}")
        else:
            total_row.append('')
    data.append(total_row)
    table = Table(data, colWidths=max_widths, repeatRows=1)
    return table


def io_top_page(canvas, doc, partner_id):
    from reportlab.lib.units import inch
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import Paragraph
    cur_io = InsertionOrder.query.filter_by(partner_id=partner_id).first()
    logo = "app/static/logo-dark.png"
    canvas.drawImage(logo, 0.25 * inch, 6.9 * inch, width=2 * inch,
                     preserveAspectRatio=True)
    canvas.setFont('Helvetica-Bold', 5)
    data = [
        (20, 1, "INSERTION ORDER: {}".format(cur_io.insertion_order)),
        (20, 1.1, "PROJECT NUMBER#: {}".format(cur_io.project_number)),
        (20, 1.2, "DATE: {}".format(cur_io.document_date)),
        (20, 1.4, "Billing Contact: {}".format(cur_io.billing_contact)),
        (20, 1.5, "ATTN: {}".format(cur_io.attn)),
        (20, 1.6, "138 Eucalyptus Drive"),
        (20, 1.7, "El Segundo, CA 90245"),
        (20, 1.8, "T: (310) 450-2653"),
        (20, 1.9, "invoices@liquidadvertising.com"),
        (20, 2.1, "BUY DETAILS (italicized impressions are estimated "
                  "based on daily traffic)"),
        (250, 1, "MEDIA: {}".format(cur_io.media_representative)),
        (250, 1.4, "Publisher Contact: {}".format(cur_io.publisher_contact)),
        (250, 1.5, cur_io.publisher_contact_email),
        (500, 1, "Client: {}".format(cur_io.client)),
        (500, 1.4, "Agency Contact: {}".format(cur_io.agency_contact)),
        (500, 1.5, cur_io.agency_contact_email)
    ]
    for y_pos, x_pos, text in data:
        canvas.drawString(y_pos, doc.pagesize[1] - x_pos * inch, text)
    notes_style = ParagraphStyle(
        name='NotesStyle',
        fontName='Helvetica-Bold',
        fontSize=7,
        leading=7,
        alignment=1
    )
    notes_text = (
        "NOTES TO PUBLISHER:<br/>PROJECT# AND CAMPAIGN NAME MUST " 
        "APPEAR ON ALL INVOICES FOR PAYMENT.<br/>DO NOT COMBINE MULTIPLE "
        "CAMPAIGNS ON A SINGLE INVOICE.<br/>SEND ALL INVOICES TO "
        "INVOICES@LIQUIDADVERTISING.COM.")
    notes_paragraph = Paragraph(notes_text, notes_style)
    notes_w, notes_h = notes_paragraph.wrap(doc.width, doc.topMargin)
    notes_paragraph.drawOn(canvas, doc.leftMargin,
                           doc.pagesize[1] - notes_h - 0.3 * inch)


@error_handler
def get_topline(plan_id, current_user_id):
    cur_plan = db.session.get(Plan, plan_id)
    partners = [x.get_form_dict() for x in cur_plan.get_partners()]
    partner_list, partner_type_list = Partner.get_name_list()
    partner_name = Partner.__table__.name
    partner_id = Partner.id.name
    partner_type = Partner.partner_type.name
    phase_name = re.split('(?<=.)(?=[A-Z])', PlanPhase.__name__)[1]
    total_budget = Partner.total_budget.name
    if not cur_plan.start_date:
        cur_plan.start_date = dt.datetime.today()
        db.session.commit()
    if not cur_plan.end_date:
        cur_plan.end_date = dt.datetime.today()
        db.session.commit()
    sd = cur_plan.start_date
    ed = cur_plan.end_date
    weeks = [sd + dt.timedelta(days=x)
             for i, x in enumerate(range((ed - sd).days)) if i % 7 == 0]
    weeks_str = [dt.datetime.strftime(x, '%Y-%m-%d') for x in weeks]
    form_cols, metric_cols, def_metric_cols = Partner.get_metric_cols()
    cols = [partner_id, partner_type, partner_name, total_budget, phase_name]
    cols += weeks_str + metric_cols
    phase_list = ['-'.join(x.capitalize() for x in x.split('-')) for x in
                  PlanPhase.get_name_list()]
    phase_list = [{phase_name: x} for x in phase_list]
    select_val_dict = {
        partner_name: partner_list,
        partner_type: partner_type_list,
        phase_name: phase_list
    }
    phases = [x.get_form_dict() for x in cur_plan.phases.all()]
    description = 'Plan details broken out by partner.'
    title = 'Plan Table - {}'.format(cur_plan.name)
    lt = app_utl.LiquidTable(
        cols, data=partners, top_rows=phases, totals=True, title=title,
        description=description, columns_toggle=True, accordion=True,
        specify_form_cols=True, select_val_dict=select_val_dict,
        select_box=partner_name, download_table=True, hidden_cols=[partner_id],
        form_cols=form_cols + [partner_name, partner_type, partner_id],
        metric_cols=metric_cols, def_metric_cols=def_metric_cols,
        header=phase_name, highlight_row=total_budget, table_name='Topline')
    return [lt.table_dict]


@error_handler
def write_topline(plan_id, current_user_id, new_data=None):
    cur_plan = db.session.get(Plan, plan_id)
    data = json.loads(new_data)
    topline_list = []
    phase_list = []
    for phase_idx in data:
        phase = data[phase_idx]
        phase_data = phase['Phase']
        phase_dict = {}
        for col in ['phaseSelect', 'total_budget', 'dates', 'id']:
            col_name = '{}{}'.format(col, phase_idx)
            new_data = [x['value'] for x in phase_data
                        if x['name'] == col_name][0]
            if col == 'dates':
                phase_dict = app_utl.get_sd_ed_in_dict(phase_dict, new_data)
            else:
                phase_dict[col] = new_data
        phase_dict['name'] = phase_dict['phaseSelect']
        phase_list.append(phase_dict)
    app_utl.set_db_values(
        cur_plan.id, current_user_id, form_sources=phase_list,
        table=PlanPhase, parent_model=Plan)
    brand_new_ids = []
    date_col = 'dates'
    est_cols = [Partner.estimated_cpm, Partner.estimated_cpc]
    cols = [Partner.partner_type, Partner.__table__, Partner.total_budget,
            Partner.cplpv, Partner.cpbc, Partner.cpv, Partner.cpcv]
    cols += est_cols
    est_str = est_cols[0].name.split('_')[0]
    cols = [x.name for x in cols] + [date_col]
    for phase_idx in data:
        phase_name = [
            x for x in data[phase_idx]['Phase'] if
            x['name'] == 'phaseSelect{}'.format(phase_idx)][0]['value']
        cur_phase = PlanPhase.query.filter_by(name=phase_name,
                                              plan_id=cur_plan.id).first()
        partner_data = data[phase_idx]['partner']
        part_num_list = [int(x['name'].replace('total_budget', ''))
                         for x in partner_data if 'total_budget' in x['name']]
        for part_num in part_num_list:
            tl_dict = {}
            for col in cols:
                if col in [Partner.partner_type.name, Partner.__table__.name]:
                    col = '{}Select'.format(col)
                col_name = col
                if est_str in col:
                    col_name = col_name.replace('{}_'.format(est_str), '')
                col_name = '{}{}'.format(col_name, part_num)
                new_data = [x['value'] for x in partner_data
                            if x['name'] == col_name]
                if new_data:
                    new_data = new_data[0]
                else:
                    continue
                if col == date_col:
                    tl_dict = app_utl.get_sd_ed_in_dict(tl_dict, new_data)
                else:
                    tl_dict[col] = new_data
            tl_dict['name'] = tl_dict['partnerSelect']
            topline_list.append(tl_dict)
        change_log = app_utl.set_db_values(
            cur_phase.id, current_user_id, form_sources=topline_list,
            table=Partner, parent_model=PlanPhase)
        if 'add' in change_log and change_log['add']:
            new_ids = [x['id'] for x in change_log['add']]
            brand_new_ids.extend(new_ids)
    cur_partners = [x.id for x in cur_plan.get_partners()]
    cur_plan.launch_placement_task(cur_partners, words=[], message='',
                                   brand_new_ids=brand_new_ids,
                                   current_user_id=current_user_id)


def download_topline(plan_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_plan = Plan.query.get(plan_id)
        file_name = 'topline_{}.xlsx'.format(cur_plan.id)
        writer = pd.ExcelWriter(file_name)
        for phase in cur_plan.phases:
            df = pd.DataFrame([x.get_form_dict() for x in phase.partners])
            sd = cur_plan.start_date
            ed = cur_plan.end_date
            weeks = [sd + dt.timedelta(days=x)
                     for i, x in enumerate(range((ed - sd).days)) if i % 7 == 0]
            for week in weeks:
                week_str = dt.datetime.strftime(week, '%Y-%m-%d')
                cal_start = week
                cal_end = week + dt.timedelta(days=6)
                df['start_check'] = np.where(cal_end >= pd.to_datetime(
                    df['start_date']).dt.date, True, False)
                df['end_check'] = np.where(cal_start <= pd.to_datetime(
                    df['end_date']).dt.date, True, False)
                df[week_str] = df['start_check'] & df['end_check']
                for col in [('cpm', 'impressions'), ('cpc', 'clicks')]:
                    thousand = 1
                    if col[0] == 'cpm':
                        thousand = 1000
                    df[col[1]] = (df['total_budget'] / df[col[0]]) * thousand
                    df[col[1]] = df[col[1]].astype(int)
            week_str = [dt.datetime.strftime(x, '%Y-%m-%d') for x in weeks]
            col_order = ['partner', 'total_budget'] + week_str + [
                'impressions', 'cpm', 'clicks', 'cpc']
            df = df[col_order]
            for col in ['total_budget', 'cpm', 'cpc']:
                df.loc[:, col] = df[col].apply(lambda x: '${:,.2f}'.format(x))
            df.columns = [' '.join(x.split('_')).upper() for x in df.columns]
            import matplotlib.colors as mcolors
            color_map = [
                (31, 119, 180), (174, 199, 232), (255, 187, 120),
                (152, 223, 138),
                (255, 152, 150), (197, 176, 213), (196, 156, 148),
                (247, 182, 210),
                (199, 199, 199), (219, 219, 141)]
            color_map = [mcolors.rgb2hex([y / 255 for y in x]) for x in
                         color_map]

            styled = df.style.apply(
                lambda x: ['background-color: {}; opacity:.5;'.format(
                    color_map[int(x.name) % 10]) if _ == True else '' for i, _
                           in x.items()], axis=1)
            styled.to_excel(writer, sheet_name=phase.name, index=False)
        writer.close()
        excel_file = get_file_in_memory(file_name, file_name='topline.xlsx')
        os.remove(file_name)
        _set_task_progress(100)
        return [excel_file]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Processor {} User {}'.format(
                plan_id, current_user_id), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_plan_rules(plan_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_plan = db.session.get(Plan, plan_id)
        name = 'PlanRules'
        name_col = 'column_name'
        df = pd.DataFrame([x.get_form_dict() for x in cur_plan.rules])
        hidden_cols = [Partner.total_budget.name, name_col, PlanRule.order.name,
                       PlanRule.name.name, PlanRule.plan_id.name,
                       PlanRule.partner_id.name, name_col]
        select_val_dict = [{PlanRule.type.name: x} for x in
                           ['Create', 'Lookup']]
        select_val_dict = {PlanRule.type.name: select_val_dict}
        lt = app_utl.LiquidTable(
            df=df, title=name, table_name=name,
            form_cols=[PlanRule.rule_info.name, PlanRule.type.name],
            specify_form_cols=True, slider_edit_col=PlanRule.rule_info.name,
            slider_abs_col=Partner.total_budget.name, hidden_cols=hidden_cols,
            select_val_dict=select_val_dict)
        _set_task_progress(100)
        return [lt.table_dict]
    except:
        _set_task_progress(100)
        app.logger.error(
            'Unhandled exception - Plan {} User {}'.format(
                plan_id, current_user_id), exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


@error_handler
def get_plan_placements(plan_id, current_user_id):
    write_name = '.{}'.format(write_plan_placements.__name__)
    write_task = Task.query.filter_by(name=write_name, complete=False,
                                      plan_id=plan_id).first()
    if write_task:
        write_task.wait_for_job()
    cur_plan = db.session.get(Plan, plan_id)
    df = cur_plan.get_placements_as_df()
    name = 'PlanPlacements'
    form_cols, metric_cols, def_metric_cols = Partner.get_metric_cols()
    form_cols += [x for x in df.columns.to_list()]
    for col in metric_cols:
        if col not in df.columns:
            df[col] = 0
    hidden_cols = [PartnerPlacements.id.name, PartnerPlacements.partner_id.name]
    lt = app_utl.LiquidTable(
        df=df, title=name, table_name=name, download_table=True,
        specify_form_cols=True, accordion=False, inline_edit=True,
        form_cols=form_cols, metric_cols=metric_cols,
        def_metric_cols=def_metric_cols, columns_toggle=True,
        hidden_cols=hidden_cols)
    return [lt.table_dict]


@error_handler
def plan_check_placements(plan_id, current_user_id, words, new_g_children,
                          total_db, message='', brand_new_ids=None):
    response = ''
    if total_db.empty:
        vendor_names = []
        if brand_new_ids:
            vendor_names = [db.session.get(Partner, x).name for x in
                            brand_new_ids]
        total_db = PartnerPlacements.get_reporting_db_df(vendor_names)
    for new_g_child_id in new_g_children:
        response += PartnerPlacements().check_gg_children(
            new_g_child_id, words=words, total_db=total_db,
            msg_text='Checking plan placements', message=message,
            running_user_id=current_user_id)
    return response


@error_handler
def check_plan_gg_children(plan_id, current_user_id, parent_id=None, words=None,
                           total_db=pd.DataFrame(), message=''):
    r = PartnerPlacements.check_col_in_words(
        PartnerPlacements, words, parent_id, total_db=total_db,
        message=message)
    PartnerPlacements.create_from_rules(PartnerPlacements, parent_id,
                                        current_user_id)


@error_handler
# noinspection SqlResolve
def get_screenshot_table(processor_id, current_user_id, filter_dict=None):
    cur_processor = db.session.get(Processor, processor_id)
    os.chdir(adjust_path(cur_processor.local_path))
    db_class = exp.DB()
    db_class.input_config('dbconfig.json')
    db_class.connect()
    if filter_dict:
        sd = filter_dict[0][prc_model.Event.eventdate.name][0]
        ed = filter_dict[0][prc_model.Event.eventdate.name][1]
    else:
        ed = dt.datetime.today()
        sd = ed - dt.timedelta(days=3)
    command = """
        SELECT * 
        FROM lqas.ss_view 
        WHERE eventdate BETWEEN '{}' AND '{}';
    """.format(sd, ed)
    db_class.cursor.execute(command)
    data = db_class.cursor.fetchall()
    columns = [i[0] for i in db_class.cursor.description]
    df = pd.DataFrame(data=data, columns=columns)
    df = df.fillna(0)
    filter_dict = {vmc.date: ''}
    lt = app_utl.LiquidTable(df=df, table_name='screenshot',
                             row_on_click='screenshotImage',
                             filter_dict=filter_dict)
    return [lt.table_dict]


@error_handler
def get_screenshot_image(processor_id, current_user_id, vk=None):
    cur_processor = db.session.get(Processor, processor_id)
    os.chdir(adjust_path(cur_processor.local_path))
    s3_class = awss3.S3()
    s3_class.input_config('s3config.json')
    key = [x for x in vk.split('|') if 'https://' in x]
    key = key[0].split('.com/')[1]
    client = s3_class.get_client()
    obj = client.get_object(Bucket=s3_class.bucket, Key=key)
    response = obj['Body'].read()
    return [response]


def get_notes_table(user_id, running_user):
    try:
        _set_task_progress(0)
        name = 'notesTable'
        vendor_col = 'vendor'
        select_cols = [vendor_col, 'country', 'environment', 'kpi']
        select_val_dict = {}
        for col in select_cols:
            col_name = '{}name'.format(col)
            a = ProcessorAnalysis.query.filter_by(
                processor_id=23, key='database_cache',
                parameter=col_name).first()
            if a:
                df = pd.read_json(a.data)
                df = df.rename(columns={col_name: col}).to_dict(
                    orient='records')
                select_val_dict[col] = df
        form_cols = ['note_text', 'created_at', 'username', 'processor_name']
        date_cols = ['start_date', 'end_date']
        form_cols += select_cols
        form_cols += date_cols
        seven_days_ago = dt.datetime.today() - dt.timedelta(days=7)
        df = Notes.query.filter(Notes.created_at > seven_days_ago).all()
        df = pd.DataFrame([x.get_table_dict() for x in df]).fillna('')
        if df.empty:
            df = pd.DataFrame(columns=form_cols)
        else:
            df = df[form_cols]
        lt = app_utl.LiquidTable(
            data=df.to_dict(orient='records'),
            col_list=df.columns.tolist(), table_name=name,
            select_val_dict=select_val_dict, accordion=True,
            form_cols=form_cols, specify_form_cols=True,
            new_modal_button=True)
        lt = lt.table_dict
        _set_task_progress(100)
        return [lt]
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - User {}'.format(user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_single_notes_table(processor_id, current_user_id, vk=None):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        cur_note = cur_proc.notes.filter_by(id=int(vk)).first()
        table_name = 'singleNoteTable{}'.format(vk)
        df = pd.read_json(cur_note.data)
        lt = app_utl.LiquidTable(df=df, table_name=table_name)
        lt = lt.table_dict
        _set_task_progress(100)
        return [lt]
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - Processor {} User {} VK {}'.format(
            processor_id, current_user_id, vk)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def update_all_notes_table(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_proc = db.session.get(Processor, processor_id)
        dimensions = ['dimensions']
        date_cols = ['start_date', 'end_date']
        dim_cols = ['vendor', 'kpi', 'country', 'environment']
        cols = dimensions + date_cols + dim_cols
        for n in cur_proc.notes:
            note_dict = {k: v for k, v in n.to_dict().items() if k in cols}
            if any(note_dict.values()):
                f_dict = {k + 'name': [v] for k, v in note_dict.items()
                          if k in dim_cols and v}
                dimensions = [x for x in f_dict.keys()]
                if any([n.start_date, n.end_date]):
                    str_format = '%Y-%m-%dT%H:%M:%S.%fZ'
                    sd = n.start_date.strftime(str_format)
                    ed = n.end_date.strftime(str_format)
                    f_dict['eventdate'] = [sd, ed]
                f_dict = [{k: v} for k, v in f_dict.items()]
                df = get_data_tables_from_db(
                    processor_id, current_user_id, dimensions=dimensions,
                    metrics=['kpi'], filter_dict=f_dict)[0]
                n.data = df.to_json(orient='records')
                db.session.commit()
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_brandtracker_imports(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        cur_data = cur_proc.get_requests_processor_analysis(
            az.Analyze.brandtracker_imports)
        table_cols = ['GAME TITLE', 'TWITTER HANDLE']
        if cur_data:
            table_dict = cur_data.data
            table_dict = {col: table_dict[col] if col in table_dict else {}
                          for col in table_cols}
        else:
            table_dict = {col: [] for col in table_cols}
        df = pd.DataFrame(table_dict)
        lt = app_utl.LiquidTable(table_name='btImportTable', df=df,
                                 accordion=True, new_modal_button=True,
                                 col_filter=False, chart_btn=False)
        _set_task_progress(100)
        return [lt.table_dict]
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_brandtracker_data(current_user_id, running_user, form_data):
    try:
        _set_task_progress(0)
        # Get all brandtracker processors and their associated reporting data
        campaign = Campaign.query.filter_by(name='BRANDTRACKER').first()
        bt_procs = Processor.query.filter_by(campaign_id=campaign.id).all()
        df = pd.DataFrame()
        metric_cols = ['media_spend', 'youtube_subscribers',
                       'twitter_followers', 'twitch_views', 'twitch_viewers',
                       'subreddit_members', 'player_share', 'nz_awareness',
                       'np_score', 'coverage', 'month_avg_user', 'stickiness',
                       'days_played', 'play_intent']
        for proc in bt_procs:
            tdf = get_data_tables_from_db(
                proc.id, current_user_id,
                dimensions=['productname', 'eventdate'],
                metrics=metric_cols, use_cache=True)[0]
            if not tdf.empty:
                df = pd.concat([df, tdf], ignore_index=True)

        # Filter dataframe to contain only requested data
        c_str = '_comparison'
        date = form_data['primary_date']
        cdate = form_data['comparison_date']
        titles = form_data['titles']
        df = utl.data_to_type(df, date_col=['eventdate'])
        cdf = df[(df['eventdate'].dt.month == cdate.month)
                 & (df['productname'].isin(titles))]
        cdf = cdf.drop(['eventdate'], axis=1)
        cdf = cdf.groupby(['productname']).mean().fillna(0)
        df = df[(df['eventdate'].dt.month == date.month)
                & (df['productname'].isin(titles))]
        df = df.drop(['eventdate'], axis=1)
        df = df.groupby(['productname']).mean().fillna(0)
        df = df.merge(cdf, how='left', left_index=True,
                      right_index=True, suffixes=(None, c_str))
        # Get equations for any calculated columns, then add them to df
        calculated_cols = Brandtracker.get_calculated_fields(c_str=c_str)
        df = df.assign(**calculated_cols)

        # Create map of columns to weights for each dimension
        columns = {}
        weights_dict = {}
        brandtracker_dimensions = ['Influence', 'Engagement', 'Momentum']
        for dim in brandtracker_dimensions:
            weights_dict[dim] = {x['data_column']: float(x['weight'])
                                 for x in form_data[dim] if dim in form_data}
        # Add z-scores and weighted totals to dataframe
        output_df = cal.calculate_weight_z_score(
            df, weights_dict).reset_index(drop=True).fillna('None')
        result = [output_df]
        # Get LiquidTable dictionary for each dimension and append to result
        for dim in brandtracker_dimensions:
            columns[dim] = [x for x in weights_dict[dim].keys()
                            if x in output_df]
            columns[dim].extend(['{}_zscore'.format(x) for x
                                 in columns[dim]])
            columns[dim].sort()
            columns[dim] = ['productname'] + columns[dim] + [dim]
            table_name = '{}Table'.format(dim)
            lt = app_utl.LiquidTable(
                table_name=table_name, df=output_df[columns[dim]],
                col_filter=False, chart_btn=False, specify_form_cols=False)
            result.append(lt.table_dict)
        _set_task_progress(100)
        return result
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - User {}'.format(
            current_user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def get_processor_data_source_table(processor_id, current_user_id):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.raw_columns).first()
        if analysis and analysis.data:
            df = pd.DataFrame(analysis.data)
        else:
            df = pd.DataFrame(columns=[vmc.vendorkey])
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id,
            key=az.Analyze.raw_file_update_col).first()
        if analysis and analysis.data:
            tdf = pd.DataFrame(analysis.data)
        else:
            tdf = pd.DataFrame(columns=[vmc.vendorkey])
        tdf = tdf.rename(columns={'source': vmc.vendorkey})
        if df.empty:
            df = pd.merge(tdf, df, how='outer', on=vmc.vendorkey)
        else:
            df = pd.merge(df, tdf, how='outer', on=vmc.vendorkey)
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.unknown_col).first()
        if analysis and analysis.data:
            tdf = pd.DataFrame(analysis.data)
            tdf = utl.data_to_type(tdf, str_col=[vmc.vendorkey])
            tdf[vmc.vendorkey] = tdf[vmc.vendorkey].str.strip("'")
            cols = [x for x in tdf.columns if x != vmc.vendorkey]
            col = 'Undefined Plan Net'
            tdf[col] = tdf[cols].values.tolist()
            tdf[col] = tdf[col].str.join('_')
            tdf = tdf.drop(cols, axis=1)
            tdf = tdf.groupby([vmc.vendorkey], as_index=False).agg(
                {col: '|'.join})
        else:
            tdf = pd.DataFrame(columns=[vmc.vendorkey])
        df = pd.merge(df, tdf, how='outer', left_on=vmc.vendorkey,
                      right_on=vmc.vendorkey)
        analysis = ProcessorAnalysis.query.filter_by(
            processor_id=cur_proc.id, key=az.Analyze.vk_metrics).first()
        if analysis and analysis.data:
            tdf = pd.DataFrame(analysis.data)
        else:
            tdf = pd.DataFrame(columns=[vmc.vendorkey])
        df = pd.merge(df, tdf, how='outer', left_on=vmc.vendorkey,
                      right_on=vmc.vendorkey)
        lt = app_utl.LiquidTable(df=df, table_name='datasourceTable')
        lt = lt.table_dict
        _set_task_progress(100)
        return [lt]
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


@error_handler
def get_all_tutorials_from_google_doc(processor_id, current_user_id):
    sheet_list = Tutorial.get_all_tutorial_sheets()
    for s in sheet_list:
        get_google_doc_for_tutorial(
            processor_id, current_user_id, s[0], s[1], s[2])
    return True


@error_handler
def get_google_doc_for_tutorial(processor_id, current_user_id, sheet_id=None,
                                note_type='', tutorial_name=''):
    if os.path.exists('processor'):
        os.chdir('processor')
    api = gsapi.GsApi()
    api.input_config('gsapi_googledoc.json')
    if sheet_id:
        api.sheet_id = sheet_id
    df = api.get_data(fields=[api.doc_str])
    df = df[df[api.head_str].notnull()]
    df_dict = df.to_dict(orient='records')
    tutorial_stages = []
    stages_before_question = 5
    tutorial_questions = 0
    for idx, x in enumerate(df_dict):
        header = x[api.head_str]
        content = x[api.cont_str]
        n = Notes.query.filter_by(header=header, note_type=note_type).first()
        if not n:
            new_note = Notes(header=header, note_type=note_type,
                             created_at=datetime.utcnow(),
                             note_text=content, user_id=current_user_id)
            db.session.add(new_note)
            db.session.commit()
        elif n.note_text != content:
            n.note_text = content
            db.session.commit()
        stage = TutorialStage.create_dict(
            tutorial_level=idx + tutorial_questions, header=header,
            message=content, alert_level='info',
            alert="Press 'Save & Continue' to get to the next level!")
        tutorial_stages.append(stage)
        if (idx % stages_before_question) == 0 and idx != 0:
            tutorial_questions += 1
            first_idx = (idx - stages_before_question) + 1
            correct_answer = random.randint(0, stages_before_question - 1)
            choices = '|'.join(
                '{}. {}'.format(yidx + 1, y['header']) for yidx, y in
                enumerate(df_dict[first_idx:idx + 1]))
            question = 'Which {} is described as:\n {}'.format(
                note_type, df_dict[correct_answer + first_idx]['content'])
            stage = TutorialStage.create_dict(
                tutorial_level=idx + tutorial_questions,
                question=question, question_answers=choices,
                correct_answer=correct_answer + 1, alert='CORRECT!',
                sub_header='Question', header=note_type,
                alert_level='success')
            tutorial_stages.append(stage)
    tutorial_stages = pd.DataFrame(tutorial_stages)
    update_tutorial(current_user_id, current_user_id, tutorial_name,
                    new_data=tutorial_stages, new_data_is_df=True)
    return [df]


@error_handler
def get_post_mortems(processor_id, current_user_id):
    os.chdir('processor')
    api = gsapi.GsApi()
    api.input_config('gsapi_googledoc.json')
    api.get_client()
    r = api.client.get(api.drive_url)
    drive = [x for x in r.json()['drives'] if x['name'] == 'Liquid']
    drive_id = drive[0]['id']
    folder_name = 'Post Mortems'
    q = """
        mimeType = 'application/vnd.google-apps.folder' and
        name contains '{}'""".format(folder_name)
    params = {
        'q': q, 'driveId': drive_id, 'includeItemsFromAllDrives': True,
        'corpora': 'drive', 'supportsAllDrives': True}
    r = api.client.get(api.files_url, params=params)
    folder_id = [x for x in r.json()['files']
                 if x['name'] == folder_name][0]['id']
    params = {
        'q': """'{}' in parents""".format(folder_id),
        'driveId': drive_id, 'includeItemsFromAllDrives': True,
        'corpora': 'drive', 'supportsAllDrives': True}
    r = api.client.get(api.files_url, params=params)
    presentations = r.json()['files']
    for presentation in presentations:
        presentation_id = presentation['id']
        app.logger.info('Getting presentation: {}'.format(presentation_id))
        url = '{}/{}'.format(api.slides_url, presentation_id)
        r = api.client.get(url)
        if 'slides' not in r.json():
            continue
        slides = r.json()['slides']
        for slide in slides:
            elems = slide['pageElements']
            slide_text = ''
            slide_header = ''
            for elem in elems:
                if 'shape' in elem and 'text' in elem['shape']:
                    text_elements = elem['shape']['text']['textElements']
                    for te in text_elements:
                        if 'textRun' in te:
                            text = te['textRun']['content']
                            if slide_header:
                                slide_text += text
                            else:
                                slide_header = text
                if 'table' in elem:
                    table_rows = elem['table']['tableRows']
                    for table_row in table_rows:
                        if 'tableCells' not in table_row:
                            continue
                        for cell in table_row['tableCells']:
                            if 'text' in cell:
                                text_elements = cell['text']['textElements']
                                for te in text_elements:
                                    if 'textRun' in te:
                                        text = te['textRun']['content']
                                        slide_text += text
            if slide_text:
                slide_id = slide['slideProperties']['notesPage']['objectId']
                slide_id = slide_id.replace(':notes', '')
                base_url = 'https://docs.google.com/presentation/d/'
                url = '{}{}/edit#slide=id.{}'.format(
                    base_url, presentation_id, slide_id)
                n = Notes.query.filter_by(link=url).first()
                if not n:
                    n = Notes(note_type=folder_name, link=url, user_id=4,
                              note_text=slide_text, header=slide_header)
                    db.session.add(n)
                    db.session.commit()
                else:
                    if n.note_text != slide_text:
                        n.note_text = slide_text
                        db.session.commit()
                    if n.header != slide_header:
                        n.header = slide_header
                        db.session.commit()
    return []


@error_handler
def get_billing_table(processor_id, current_user_id):
    cur_proc = Processor.query.get(processor_id)
    dimensions = ['campaignname', 'vendorname']
    df = get_data_tables_from_db(
        processor_id, current_user_id, dimensions=dimensions,
        metrics=['netcost', 'plannednetcost'], use_cache=True)[0]
    invoice_cost = 'invoicecost'
    file_name = os.path.join(cur_proc.local_path, 'invoices.csv')
    file_name = adjust_path(file_name)
    if os.path.exists(file_name):
        idf = pd.read_csv('invoices.csv')
        cols = dimensions + [invoice_cost]
        if [x for x in cols if x not in df.columns]:
            df[invoice_cost] = 0
        else:
            idf = idf[cols]
            idf[invoice_cost] = idf[invoice_cost].str.split('\n').str[0]
            idf = utl.data_to_type(idf, float_col=[invoice_cost])
            df = df.merge(idf, how='left', on=dimensions)
    else:
        df[invoice_cost] = 0
    for col in [dctc.PNC, cal.NCF, invoice_cost]:
        if col not in df.columns:
            df[col] = 0
    df['plan - netcost'] = df[dctc.PNC] - df[cal.NCF]
    df['invoice - plancost'] = df[invoice_cost] - df[dctc.PNC]
    lt = app_utl.LiquidTable(
        df=df, table_name='billingTable', button_col=[invoice_cost],
        highlight_row=invoice_cost, row_on_click='billingInvoice')
    lt = lt.table_dict
    return [lt]


def get_billing_invoice(processor_id, current_user_id, vk=None):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        vk = '_'.join(vk.split('|')[:2]) + '_'
        file_name = 'invoice_{}.pdf'.format(vk)
        file_name = os.path.join(cur_proc.local_path, file_name)
        invoice_name = 'invoice.pdf'
        if os.path.exists(file_name):
            invoice_name = os.path.join(cur_proc.local_path, invoice_name)
            copy_file(file_name, invoice_name)
        file_name = invoice_name
        pdf_file = get_file_in_memory(file_name, file_name='sow.pdf')
        _set_task_progress(100)
        return [pdf_file]
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def write_billing_invoice(processor_id, current_user_id, new_data=None,
                          object_form=None):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        dimensions = ['campaignname', 'vendorname']
        new_file_name = 'invoice_'
        for col in dimensions:
            val_to_add = [x for x in object_form if col in x['name']]
            if val_to_add:
                val_to_add = str(val_to_add[0]['value'])
                new_file_name += '{}_'.format(val_to_add)
        new_file_name += '.pdf'
        file_name = os.path.join(cur_proc.local_path, new_file_name)
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(new_data, f, length=131072)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


def write_billing_table(processor_id, current_user_id, new_data=None):
    try:
        _set_task_progress(0)
        cur_proc = Processor.query.get(processor_id)
        file_name = os.path.join(cur_proc.local_path, 'invoices.csv')
        file_name = adjust_path(file_name)
        df = pd.read_json(new_data)
        df = pd.DataFrame(df[0][1])
        df.to_csv(file_name)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - Processor {} User {}'.format(
            processor_id, current_user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


@error_handler
def write_plan_rules(plan_id, current_user_id, new_data=None):
    cur_plan = db.session.get(Plan, plan_id)
    df = pd.read_json(new_data)
    df = pd.DataFrame(df[0][1])
    df = df.to_dict(orient='records')
    app_utl.set_db_values(plan_id, current_user_id, df, PlanRule, Plan)
    for phase in cur_plan.phases:
        for part in phase.partners:
            PartnerPlacements.create_from_rules(PartnerPlacements, part.id,
                                                current_user_id)


def update_rules_from_change_log(plan_id, current_user_id, part_id, change_log):
    add_types = ['add', 'delete']
    for add_type in add_types:
        if add_type in change_log and change_log[add_type]:
            add_rule = PlanRule.query.filter_by(
                plan_id=plan_id, partner_id=part_id, type=add_type).first()
            if not add_rule:
                add_rule = PlanRule(plan_id=plan_id, partner_id=part_id,
                                    type=add_type)
                db.session.add(add_rule)
                db.session.commit()
            if add_rule.rule_info != change_log[add_type]:
                add_rule.rule_info = change_log[add_type]
    update_type = 'update'
    if update_type in change_log and change_log[update_type]:
        form_sources = []
        for x in change_log[update_type]:
            cur_place = db.session.get(PartnerPlacements,
                                       x[PartnerPlacements.id.name])
            val = PartnerPlacements.fix_date_from_words(
                col=x['col'], val=x['val'], to_str=True)
            rule_info = {'id': x['id'], 'val': val}
            form_source = {PlanRule.type.name: update_type,
                           PlanRule.place_col.name: x['col'],
                           PlanRule.rule_info.name: rule_info,
                           PlanRule.name.name: '',
                           PlanRule.plan_id.name: str(plan_id),
                           PlanRule.partner_id.name: str(cur_place.partner_id)}
            form_sources.append(form_source)
        additional_filter = {PlanRule.type.name: update_type}
        app_utl.set_db_values(
            plan_id, current_user_id, form_sources, table=PlanRule,
            parent_model=Plan, additional_filter=additional_filter)


def set_metrics_from_dict(form_source, cur_phase, cost_col, metric_cols):
    part_df = [x.get_form_dict() for x in cur_phase.get_current_children()]
    if not part_df:
        part_df, partner_type_list = Partner.get_name_list()
    part_df = pd.DataFrame(part_df)
    est_cols = [Partner.estimated_cpc, Partner.estimated_cpm]
    prefix = est_cols[0].name.split('_')[0]
    est_cols = [x.name.split('_')[-1] for x in est_cols]
    for idx, col in enumerate(metric_cols):
        if col not in form_source.columns:
            sum_col = metric_cols[idx + 1]
            if sum_col not in form_source.columns:
                sum_cols = [Partner.__table__.name, col]
                sum_col_df = part_df[sum_cols]
                form_source = pd.merge(form_source, sum_col_df,
                                       on=Partner.__table__.name, how='left')
                form_source[sum_col] = form_source[col]
            else:
                form_source[col] = form_source[cost_col] / form_source[sum_col]
                if col == vmc.impressions:
                    form_source[col] = form_source[col] / 1000
        if col in est_cols:
            col_name = '{}_{}'.format(prefix, col)
            form_source[col_name] = form_source[col]
    return form_source


def set_partner_from_placements(cur_plan, df, phase_col, cost_col,
                                plan_id, current_user_id, metric_cols):
    mp = cre.MediaPlan
    part_col = [mp.partner_name, Partner.__table__.name]
    part_col = [x for x in part_col if x in df.columns][0]
    sum_cols = [cost_col] + metric_cols
    sum_cols = [x for x in sum_cols if x in df.columns]
    form_sources = df.groupby([part_col, phase_col])[sum_cols].sum()
    form_sources = form_sources.reset_index()
    cur_phases = [x for x in cur_plan.phases.all()]
    brand_new_ids = []
    for cur_phase in cur_phases:
        form_source = form_sources[form_sources[phase_col] == cur_phase.name]
        cols = [Partner.__table__.name, Partner.name.name]
        for col in cols:
            form_source.loc[:, col] = form_source.loc[:, part_col]
        budget_col = Partner.total_budget.name
        form_source.loc[:, budget_col] = form_source.loc[:, cost_col]
        form_source = set_metrics_from_dict(form_source, cur_phase, cost_col,
                                            metric_cols)
        form_source = form_source.to_dict(orient='records')
        change_log = app_utl.set_db_values(
            cur_phase.id, current_user_id, form_sources=form_source,
            table=Partner, parent_model=PlanPhase)
        if 'add' in change_log and change_log['add']:
            new_ids = [x['id'] for x in change_log['add']]
            brand_new_ids.extend(new_ids)
    if brand_new_ids:
        for parent_id in brand_new_ids:
            check_plan_gg_children(plan_id, current_user_id,
                                   parent_id=parent_id)


@error_handler
def write_plan_placements(plan_id, current_user_id, new_data=None,
                          file_type=None):
    if file_type:
        df = utl.import_read_csv(new_data, file_check=False,
                                 file_type=file_type)
    else:
        df = pd.read_json(new_data)
        df = pd.DataFrame(df[0][1])
    df = PartnerPlacements.translate_plan_names(df)
    form_cols, metric_cols, def_metric_cols = Partner.get_metric_cols()
    df = df.fillna('')
    mp = cre.MediaPlan
    cur_plan = db.session.get(Plan, plan_id)
    phase_col = [mp.placement_phase, mp.campaign_name, mp.old_placement_phase,
                 mp.campaign_phase, mp.old_campaign_phase,
                 PlanPhase.__table__.name]
    phase_col += [x.strip() for x in phase_col]
    phase_col += [x.replace(' (If Needed)', '') for x in phase_col]
    phase_col = [x for x in phase_col if x in df.columns][0]
    cost_col = [vmc.cost, PartnerPlacements.total_budget.name, dctc.PNC]
    cost_col = [x for x in cost_col if x in df.columns][0]
    date_cols = [PartnerPlacements.start_date, PartnerPlacements.end_date]
    date_cols = [x.name for x in date_cols if x.name in df.columns]
    df = utl.data_to_type(df, float_col=[cost_col], date_col=date_cols)
    df[phase_col] = df[phase_col].str.replace('_', '-')
    form_sources = df.groupby(phase_col)[cost_col].sum().reset_index()
    form_sources[PlanPhase.name.name] = form_sources[phase_col]
    form_sources = form_sources.to_dict(orient='records')
    app_utl.set_db_values(plan_id, current_user_id, form_sources=form_sources,
                          table=PlanPhase, parent_model=Plan)
    set_partner_from_placements(cur_plan, df, phase_col, cost_col, plan_id,
                                current_user_id, metric_cols)
    col = PartnerPlacements.partner_id.name
    if col not in df.columns:
        poss_cols = [Partner.__name__, cre.MediaPlan.partner_name,
                     Partner.__table__.name]
        poss_cols = [x for x in poss_cols if x in df.columns]
        if poss_cols:
            col = poss_cols[0]
    cur_part = cur_plan.get_partners()
    part_dict = {x.name: x.id for x in cur_part}
    df[col] = df[col].replace(part_dict)
    unique_ids = df[col].unique()
    for part_id in unique_ids:
        tdf = df[df[col] == part_id]
        part_id = int(part_id)
        cur_part = db.session.get(Partner, part_id)
        cur_phase = db.session.get(PlanPhase, cur_part.plan_phase_id)
        col_dict = {x: x.replace(' ', '_').lower() for x in df.columns}
        tdf[Partner.__table__.name] = cur_part.name
        tdf = set_metrics_from_dict(tdf, cur_phase, cost_col, metric_cols)
        tdf = tdf.rename(columns=col_dict)
        tdf = utl.data_to_type(tdf, float_col=metric_cols).fillna(0)
        tdf = tdf.to_dict(orient='records')
        for x in tdf:
            name = PartnerPlacements().create_placement_name(x, cur_part)
            x[Partner.name.name] = name
        change_log = app_utl.set_db_values(
            part_id, current_user_id, form_sources=tdf,
            table=PartnerPlacements, parent_model=Partner)
        update_rules_from_change_log(plan_id, current_user_id, part_id,
                                     change_log)


@error_handler
def write_plan_calc(plan_id, current_user_id, new_data=None):
    df = pd.read_json(new_data)
    df = pd.DataFrame(df[0][1])
    df = df.to_dict(orient='records')
    app_utl.set_db_values(plan_id, current_user_id, df, PlanEffectiveness, Plan)


def download_table(object_id, current_user_id, function_name=None, **kwargs):
    try:
        _set_task_progress(0)
        task_name = function_name.replace('.', '')
        resp = globals()[task_name](object_id, current_user_id, **kwargs)
        df = pd.DataFrame(resp[0]['data'])
        download_file = get_file_in_memory(df)
        _set_task_progress(100)
        return [download_file]
    except:
        _set_task_progress(100)
        msg = 'Unhandled exception - Obj {} User {}'.format(
            object_id, current_user_id)
        app.logger.error(msg, exc_info=sys.exc_info())
        return [pd.DataFrame([{'Result': 'DATA WAS UNABLE TO BE LOADED.'}])]


@error_handler
def add_contacts_from_file(plan_id, current_user_id, new_data, cur_rfp,
                           part_translation):
    df = pd.read_excel(new_data, None)
    sheet_name = [x for x in df.keys() if 'contact' in x.lower()]
    if not sheet_name:
        return False
    df = df[sheet_name[0]]
    new_list = []
    cur_contact = ''
    contact_types = ['Sales Representative', 'AM/Traffic Contact']
    phone_email = ['Phone', 'Email Address']
    for col in df['Unnamed: 1']:
        if col in contact_types:
            cur_contact = col
        if col in phone_email:
            col += ' ({})'.format(cur_contact)
        new_list.append(col)
    df['Unnamed: 1'] = new_list
    df = df[['Unnamed: 1', 'Unnamed: 3']].T.reset_index(drop=True)
    df = utl.first_last_adj(df, 1, 0).reset_index(drop=True)
    cols = Contacts.column_translation()
    partner_col = cols[Contacts.partner_name.name]
    mask = ~df[partner_col].isin(part_translation.keys())
    not_in_dict = df[partner_col].loc[mask].unique()
    for partner_name in not_in_dict:
        cur_part = Partner(name=partner_name)
        db.session.add(cur_part)
        db.session.commit()
        part_translation[partner_name] = cur_part.id
    df[Contacts.partner_id.name] = df[partner_col].replace(part_translation)
    df[Contacts.rfp_file_id.name] = cur_rfp.id
    cols = {v: k for k, v in cols.items()}
    df = df.rename(columns=cols)
    df = df.fillna('None')
    df = df.to_dict(orient='records')
    app_utl.set_db_values(cur_rfp.id, current_user_id, df, Contacts,
                          RfpFile)
    return True


@error_handler
def add_specs_from_file(plan_id, current_user_id, new_data, cur_rfp,
                        part_translation):
    df = pd.read_excel(new_data, None)
    sheet_name = [x for x in df.keys() if 'spec' in x.lower()]
    if not sheet_name:
        return False
    df = df[sheet_name[0]]
    df = utl.first_last_adj(df, 1, 0).reset_index(drop=True)
    df = df[~df.isna().all(axis=1)]
    df = df.fillna(method='ffill').fillna('None')
    cols = Specs.column_translation()
    partner_col = cols[Specs.partner.name]
    df[Specs.partner_id.name] = df[partner_col].replace(part_translation)
    df[Specs.rfp_file_id.name] = cur_rfp.id
    cols = {v: k for k, v in cols.items()}
    df = df.rename(columns=cols)
    df = df.to_dict(orient='records')
    app_utl.set_db_values(cur_rfp.id, current_user_id, df, Specs, RfpFile)
    return True


@error_handler
def set_placements_from_rfp(plan_id, current_user_id, cur_rfp):
    df = [PartnerPlacements.set_from_another(x) for x in cur_rfp.placements]
    df = pd.DataFrame(df)
    partner_col = PartnerPlacements.partner_id.name
    partner_list = df[partner_col].unique()
    for partner_id in partner_list:
        cur_partner = db.session.get(Partner, int(partner_id))
        tdf = df[df[partner_col] == partner_id]
        tdf = tdf.to_dict(orient='records')
        change_log = app_utl.set_db_values(
            cur_partner.id, current_user_id, tdf,
            PartnerPlacements, Partner)
        update_rules_from_change_log(plan_id, current_user_id,
                                     partner_id, change_log)
        return True


@error_handler
def add_rfp_from_file(plan_id, current_user_id, new_data):

    cur_plan = db.session.get(Plan, plan_id)
    df = pd.read_excel(new_data, None)
    sheet_name = [x for x in df.keys() if 'plan' in x.lower()]
    if not sheet_name:
        sheet_name = [x for x in df.keys() if '$' in x.lower()]
        if not sheet_name:
            _set_task_progress(100)
            return False
    df = df[sheet_name[0]]
    df = utl.first_last_adj(df, 2, 0).reset_index(drop=True)
    cols = Rfp.column_translation()
    partner_col = cols[Rfp.partner_name.name]
    df = df[df[partner_col] != 'Example Media '].reset_index(drop=True)
    mask = df.drop(partner_col, axis=1).isna().all(axis=1)
    df = df[~mask].reset_index(drop=True)
    no_fill_cols = [Rfp.planned_impressions, Rfp.planned_units,
                    Rfp.cpm_cost_per_unit, Rfp.planned_net_cost,
                    Rfp.planned_sov]
    no_fill_cols = [cols[x.name] for x in no_fill_cols]
    fill_cols = [x for x in df.columns if x not in no_fill_cols]
    for col in fill_cols:
        df[col] = df[col].fillna(method='ffill').fillna('None')
    name = RfpFile.create_name(df)
    cur_rfp = RfpFile.query.filter_by(
        name=name, plan_id=cur_plan.id).first()
    if not cur_rfp:
        cur_rfp = RfpFile(
            name=name, plan_id=cur_plan.id, user_id=current_user_id)
        db.session.add(cur_rfp)
        db.session.commit()
    float_col = [Rfp.planned_net_cost, Rfp.cpm_cost_per_unit,
                 Rfp.planned_impressions, Rfp.planned_sov,
                 Rfp.planned_units]
    float_col = [cols[x.name] for x in float_col]
    date_col = [cols[Rfp.start_date.name], cols[Rfp.end_date.name]]
    df = utl.data_to_type(df, float_col=float_col, date_col=date_col)
    for col in date_col:
        df[col] = df[col].fillna(method='ffill')
        if col == cols[Rfp.start_date.name]:
            fill_na_val = cur_plan.start_date
        else:
            fill_na_val = cur_plan.end_date
        df[col] = df[col].fillna(fill_na_val)
    for col in float_col:
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].fillna(0)
    part_translation = {}
    for partner_name in name.split('|'):
        cur_part = None
        for cur_phase in cur_plan.phases.all():
            cur_part = Partner.query.filter_by(
                plan_phase_id=cur_phase.id, name=partner_name).first()
            if cur_part:
                break
        if not cur_part:
            tdf = df[df[partner_col] == partner_name]
            total_budget = tdf[cols[Rfp.planned_net_cost.name]].sum()
            total_imps = tdf[cols[Rfp.planned_impressions.name]].sum()
            if total_imps == 0:
                cpm = 0
            else:
                cpm = (total_budget / (total_imps / 1000))
            sd = tdf[cols[Rfp.start_date.name]].dropna().min()
            ed = tdf[cols[Rfp.end_date.name]].dropna().max()
            if pd.isnull(sd):
                sd = cur_phase.start_date
            if pd.isnull(ed):
                ed = cur_phase.end_date
            cur_part = Partner(
                name=partner_name, plan_phase_id=cur_phase.id,
                total_budget=total_budget, start_date=sd, end_date=ed,
                estimated_cpm=cpm)
            db.session.add(cur_part)
            db.session.commit()
        part_translation[partner_name] = cur_part.id
    df[Rfp.partner_id.name] = df[partner_col].replace(part_translation)
    df[Rfp.rfp_file_id.name] = cur_rfp.id
    cols = {v: k for k, v in cols.items()}
    df = df.rename(columns=cols)
    df = df.to_dict(orient='records')
    app_utl.set_db_values(cur_rfp.id, current_user_id, df, Rfp, RfpFile)
    add_specs_from_file(plan_id, current_user_id, new_data, cur_rfp,
                        part_translation)
    add_contacts_from_file(plan_id, current_user_id, new_data, cur_rfp,
                           part_translation)
    set_placements_from_rfp(plan_id, current_user_id, cur_rfp)


@error_handler
def get_rfp(plan_id, current_user_id):
    cur_plan = db.session.get(Plan, plan_id)
    rfp_files = RfpFile.query.filter_by(plan_id=cur_plan.id).all()
    data = []
    for rfp_file in rfp_files:
        place = [x.get_form_dict() for x in rfp_file.placements]
        data.extend(place)
    df = pd.DataFrame(data)
    name = 'RFP'
    lt = app_utl.LiquidTable(
        df=df, title=name, table_name=name, download_table=True,
        specify_form_cols=False, accordion=True)
    return [lt.table_dict]


@error_handler
def get_specs(plan_id, current_user_id):
    cur_plan = db.session.get(Plan, plan_id)
    rfp_files = RfpFile.query.filter_by(plan_id=cur_plan.id).all()
    data = []
    for rfp_file in rfp_files:
        place = [x.get_form_dict() for x in rfp_file.specs]
        data.extend(place)
    df = pd.DataFrame(data)
    name = 'Specs'
    lt = app_utl.LiquidTable(
        df=df, title=name, table_name=name, download_table=True,
        specify_form_cols=False, accordion=True)
    return [lt.table_dict]


@error_handler
def get_contacts(plan_id, current_user_id):
    cur_plan = db.session.get(Plan, plan_id)
    rfp_files = RfpFile.query.filter_by(plan_id=cur_plan.id).all()
    data = []
    for rfp_file in rfp_files:
        place = [x.get_form_dict() for x in rfp_file.contacts]
        data.extend(place)
    df = pd.DataFrame(data)
    name = 'Contacts'
    lt = app_utl.LiquidTable(
        df=df, title=name, table_name=name, download_table=True,
        specify_form_cols=False, accordion=True)
    return [lt.table_dict]


def check_missing_vals(obj_id, cur_obj=Plan):
    msg = ''
    cur_plan = db.session.get(cur_obj, obj_id)
    if not cur_plan:
        return msg
    miss_vals = [k for k, v in cur_plan.__dict__.items() if not v]
    if miss_vals:
        miss_vals = ', '.join(miss_vals)
        msg = 'Fill in the following values: {}'.format(miss_vals)
    return msg


def check_topline(obj_id, cur_partners):
    msg = ''
    cur_plan = db.session.get(Plan, obj_id)
    total_cost = sum(x.total_budget for x in cur_partners)
    if total_cost != cur_plan.total_budget:
        msg = 'Cost from {} partners ({}) not equal total budget {}.'.format(
            len(cur_partners), total_cost, cur_plan.total_budget)
    return msg


def get_partners_api_split(cur_partners):
    api_partners = []
    non_api_partners = []
    api_vals = [x for y in vmc.api_partner_name_translation.values() for x in y]
    for part in cur_partners:
        if part.name in api_vals:
            api_partners.append(part)
        else:
            non_api_partners.append(part)
    return api_partners, non_api_partners


def check_plan_rules(cur_partners, cur_obj=PlanRule,
                     comp_col=PlanRule.rule_info, comp_val=1):
    msg = ''
    miss_partners = []
    if cur_partners:
        for cur_part in cur_partners:
            miss_part = True
            prs = cur_obj.query.filter_by(plan_id=cur_part.id).all()
            if comp_col:
                for pr in prs:
                    if len(pr.__dict__[comp_col.name]) > comp_val:
                        miss_part = False
                        break
            else:
                if not prs:
                    miss_part = False
            if miss_part:
                miss_partners.append(cur_part.name)
    if miss_partners:
        miss_partners = ', '.join(miss_partners)
        msg = 'Add {} for partners {}'.format(cur_obj.__name__, miss_partners)
    return msg


def check_up_relations(obj_id, object_level='Campaign'):
    msg = ''
    upo = UploaderObjects.query.filter_by(
        uploader_id=obj_id, object_level=object_level,
        uploader_type='Facebook').first()
    miss = Uploader().check_relations(upo, as_response=False)
    if miss:
        miss = [x.impacted_column_name for x in miss]
        msg = 'Fill in the following: {}'.format(', '.join(miss))
    return msg


def route_check(cur_plan, route_name, cur_partners, api_partners,
                non_api_partners):
    msg = ''
    cur_up = Uploader.query.filter_by(name=cur_plan.name).first()
    cur_proc = Processor.query.filter_by(name=cur_plan.name).first()
    cur_sow = Sow.query.filter_by(plan_id=cur_plan.id).first()
    route_dict = {
        plan_routes.edit_plan: [check_missing_vals, [cur_plan.id]],
        plan_routes.topline: [check_topline, [cur_plan, cur_partners]],
        plan_routes.edit_sow: [check_missing_vals, [cur_sow, Sow]],
        plan_routes.plan_rules: [check_plan_rules, [api_partners]],
        plan_routes.rfp: [check_plan_rules, [non_api_partners, Rfp, None]],
        main_routes.edit_uploader: [check_missing_vals, [cur_up, Uploader]],
        main_routes.edit_request_processor:
            [check_missing_vals, [cur_up, Uploader]],
    }
    up_routes = [
        main_routes.edit_uploader_campaign, main_routes.edit_uploader_adset,
        main_routes.edit_uploader_ad]
    for x in up_routes:
        obj_level = x.__name__.split('_')[-1].capitalize()
        route_dict[x] = [check_up_relations, [cur_up, obj_level]]
    new_route_dict = {}
    for k, v in route_dict.items():
        prefix = 'plan.'
        kn = k.__name__
        if Processor.__table__.name in kn or Uploader.__table__.name in kn:
            prefix = 'main.'
        new_route_dict['{}{}'.format(prefix, kn)] = v
    if not cur_up and Uploader.__table__.name in route_name:
        msg = 'No Uploader - create one from plan.'
    elif not cur_proc and Processor.__table__.name in route_name:
        msg = 'No Processor - create one from plan.'
    elif route_name in new_route_dict:
        cur_function = new_route_dict[route_name][0]
        cur_args = new_route_dict[route_name][1]
        cur_args = [x.id if hasattr(x, 'id') and isinstance(x.id, int) else x
                    for x in cur_args]
        msg = cur_function(*cur_args)
    if not msg:
        msg = 'COMPLETED'
    return msg


def check_objs(cur_plan, has_facebook=False, cur_user=None):
    cur_up = Uploader.query.filter_by(name=cur_plan.name).first()
    msg = '{}'.format(cur_plan.name)
    ali_chat = az.AliChat()
    ali_chat.db = db
    ali_chat.current_user = cur_user
    if not cur_up and has_facebook:
        cur_up = ali_chat.create_db_model_from_other(Plan, msg, Uploader)
    cur_proc = Processor.query.filter_by(name=cur_plan.name).first()
    if not cur_proc:
        cur_proc = ali_chat.create_db_model_from_other(Plan, msg, Processor)
    cur_sow = Sow.query.filter_by(plan_id=cur_plan.id).first()
    if not cur_sow:
        current_sow = Sow()
        current_sow.create_from_plan(cur_plan)
        db.session.add(current_sow)
        db.session.commit()
    return cur_up, cur_proc, cur_sow


@error_handler
def get_checklist(plan_id, current_user_id):
    cur_user = db.session.get(User, current_user_id)
    cur_plan = db.session.get(Plan, plan_id)
    cur_partners = cur_plan.get_partners()
    api_partners, non_api_partners = get_partners_api_split(cur_partners)
    data = []
    button_types = ['Plan', 'UploaderFacebook', 'ProcessorRequest']
    is_fb = vmc.api_fb_key in [x.name for x in api_partners]
    if not is_fb:
        button_types = [x for x in button_types if x != 'UploaderFacebook']
    check_objs(cur_plan, has_facebook=is_fb, cur_user=cur_user)
    for btn_type in button_types:
        buttons = Processor.get_navigation_buttons(btn_type)
        for button in buttons:
            for k, v in button.items():
                v['object'] = btn_type
                v['name'] = k
                msg = route_check(cur_plan, v['route'], cur_partners,
                                  api_partners, non_api_partners)
                v['check'] = msg
                is_complete = 1 if msg == 'COMPLETED' else 0
                v['complete'] = is_complete
                v.pop('icon')
                data.append(v)
    df = pd.DataFrame(data)
    name = 'Checklist'
    lt = app_utl.LiquidTable(
        df=df, title=name, table_name=name, download_table=True,
        form_cols=['complete'], highlight_row='complete', inline_edit=True,
        link_cols={'route': ''})
    return [lt.table_dict]


def get_filter_dict_values():
    filter_dict = {vmc.date: ''}
    db_models = [Client, User, Product, Campaign, Processor, Project]
    for db_model in db_models:
        if db_model == User:
            name_col = db_model.username
        elif db_model == Project:
            name_col = db_model.project_name
        else:
            name_col = db_model.name
        name_vals = db_model.query.order_by(name_col).all()
        name_vals = [x.__dict__[name_col.name] for x in name_vals]
        filter_dict[db_model.__tablename__] = name_vals
    return filter_dict


@error_handler
def get_project_number(current_user_id, running_user, filter_dict=None):
    get_project_numbers(0, running_user, filter_dict=filter_dict)
    today = datetime.today()
    sd = today - dt.timedelta(days=30)
    ed = today + dt.timedelta(days=7)
    results = db.session.query(Project).filter(
        or_(
            and_(Project.flight_start_date >= sd,
                 Project.flight_start_date <= ed),
            and_(Project.flight_end_date >= sd,
                 Project.flight_end_date <= ed),
            and_(Project.flight_start_date <= sd,
                 Project.flight_end_date >= ed)
        )
    )
    if filter_dict:
        results = app_utl.parse_filter_dict_from_clients(
            results, None, None, filter_dict, db_model=Project)
    else:
        results = results.all()
    filter_dict = get_filter_dict_values()
    week_str = app_utl.LiquidTable.convert_sd_ed_to_weeks(sd, ed)
    data = [x.get_form_dict() for x in results]
    col_list = []
    if data:
        col_list = list(data[0].keys())
    col_list += week_str
    name = 'ProjectNumber'
    lt = app_utl.LiquidTable(
        col_list=col_list, data=data, title=name, table_name=name,
        download_table=True, specify_form_cols=False, accordion=True,
        row_on_click='projectObjects', filter_dict=filter_dict,
        link_cols={Project.project_number.name: 'main.project_edit'})
    return [lt.table_dict]


@error_handler
def get_plan_calc(plan_id, current_user_id):
    headers = "Headers"
    values = ["-0.20", "-0.10", "0.00", "0.10", "0.20"]
    brand_low = PlanEffectiveness.brand_low
    brand_high = PlanEffectiveness.brand_low
    msg_low = PlanEffectiveness.msg_low
    msg_high = PlanEffectiveness.msg_high
    media_low = PlanEffectiveness.media_low
    media_high = PlanEffectiveness.media_high
    d = {PlanEffectiveness.brand: (brand_low, brand_high),
         PlanEffectiveness.msg: (msg_low, msg_high),
         PlanEffectiveness.media: (media_low, media_high)
         }
    df = pd.DataFrame()
    for k, v in d.items():
        tdf = pd.DataFrame({PlanEffectiveness.factors_low: v[0],
                            PlanEffectiveness.factors_high: v[1]})
        tdf[headers] = k
        df = pd.concat([df, tdf], ignore_index=True)
    selected_dict = {}
    for col in brand_low + msg_low + media_low:
        pe = PlanEffectiveness.query.filter_by(plan_id=plan_id,
                                               factor_name=col).first()
        pe = pe.selected_val if pe else 0
        selected_dict[col] = pe
    pick_col = 'cell_pick_col'
    df[pick_col] = df[PlanEffectiveness.factors_low].map(selected_dict)
    cell_pick_cols = []
    for i, value in enumerate(values, 1):
        col_name = f"Value{i}"
        cell_pick_cols.append(col_name)
        df[col_name] = value
    first_cols = [headers, PlanEffectiveness.factors_low]
    last_cols = [PlanEffectiveness.factors_high, pick_col]
    col_order = first_cols + cell_pick_cols + last_cols
    df = df[col_order]
    name = 'Calc'
    lt = app_utl.LiquidTable(df=df, table_name=name,
                             cell_pick_cols=cell_pick_cols, totals=True,
                             total_default_val=3)
    return [lt.table_dict]


@error_handler
def get_project_objects(current_user_id, running_user, vk='', filter_dict=None):
    project_num = ''.join(x.strip() for x in vk.split('|')[0])
    p = Project.query.filter_by(project_number=project_num).first()
    p_objs = p.processor_associated.all() + p.plan_associated.all()
    data = [x.to_dict() for x in p_objs]
    col_list = []
    sd = None
    ed = None
    if data:
        col_list = list(data[0].keys())
        for d in data:
            if not sd or sd > d[Processor.start_date.name]:
                sd = d[Processor.start_date.name]
            if not ed or ed < d[Processor.end_date.name]:
                ed = d[Processor.end_date.name]
        week_str = app_utl.LiquidTable.convert_sd_ed_to_weeks(sd, ed)
        col_list += week_str
    name = 'projectObjects'
    lt = app_utl.LiquidTable(
        col_list=col_list, data=data, title=name, table_name=name,
        download_table=True, specify_form_cols=False, accordion=True,
        row_on_click='projectObjects')
    return [lt.table_dict]


@error_handler
def get_contact_numbers(processor_id, current_user_id):
    os.chdir('processor')
    api = gsapi.GsApi()
    api.input_config('gsapi.json')
    sheet_id = '1IZSzjzAjiKDch9leOYL6cAmh6YeVOuXBeqTZuWD8IOI'
    api.sheet_id = sheet_id
    df = api.get_data()
    df = utl.first_last_adj(df, 1, 0)
    df_dict = df.to_dict(orient='records')
    name = 'rfp_plan'
    rfp_plan = Plan.query.filter_by(name=name).first()
    if not rfp_plan:
        rfp_plan = Plan(name=name)
        db.session.add(rfp_plan)
        db.session.commit()
    cur_phase = rfp_plan.get_current_children()
    if not cur_phase:
        cur_phase = PlanPhase(name=name, plan_id=rfp_plan.id)
        db.session.add(cur_phase)
        db.session.commit()
    else:
        cur_phase = cur_phase[0]
    rfp_file = RfpFile.query.filter_by(
        name=name, plan_id=rfp_plan.id).first()
    if not rfp_file:
        rfp_file = RfpFile(name=name, plan_id=rfp_plan.id,
                           user_id=current_user_id)
        db.session.add(rfp_file)
        db.session.commit()
    for contact in df_dict:
        partner_name = contact[Partner.__table__.name.capitalize()]
        partner_obj = Partner.query.filter_by(
            name=partner_name, plan_phase_id=cur_phase.id).first()
        if not partner_obj:
            partner_obj = Partner(
                name=partner_name, plan_phase_id=cur_phase.id)
            db.session.add(partner_obj)
            db.session.commit()
        c = Contacts.query.filter_by(
            partner_name=partner_name, rfp_file_id=rfp_file.id,
            partner_id=partner_obj.id,
            sales_representative=contact['Contact Name']).first()
        if not c:
            c = Contacts(partner_name=partner_name, rfp_file_id=rfp_file.id,
                         partner_id=partner_obj.id,
                         sales_representative=contact['Contact Name'],
                         date_submitted=dt.datetime.today())
            db.session.add(c)
            db.session.commit()
        c.contact_title = contact['Contact Title']
        c.sales_representative = contact['Contact Name']
        c.email_address1 = contact['Contact Email']
        c.phone1 = contact['Contact Phone']
        db.session.commit()
    return [df]


@error_handler
def get_table_project(object_id, current_user_id, function_name=None, **kwargs):
    task_name = function_name.replace('.', '')
    p = db.session.get(Project, object_id)
    lt = app_utl.LiquidTable()
    return_val = {}
    for proc in p.processor_associated.all():
        resp = globals()[task_name](proc.id, current_user_id, **kwargs)
        if resp:
            resp = resp[0]
            is_return_df = isinstance(return_val, pd.core.frame.DataFrame)
            is_resp_df = isinstance(resp, pd.core.frame.DataFrame)
            if 'liquid_table' in resp and resp['liquid_table']:
                lt.table_dict = lt.combine_table_dicts(
                    lt.table_dict, resp)
                return_val = lt.table_dict
            elif (not is_return_df) and (not return_val):
                return_val = resp
            elif is_resp_df:
                return_val = pd.concat([return_val, resp],
                                       ignore_index=True, sort=False)
            elif isinstance(resp, list):
                return_val.extned(resp)
    if 'download' in function_name:
        if not isinstance(return_val, pd.core.frame.DataFrame):
            return_val = pd.DataFrame(return_val)
        return_val = get_file_in_memory(return_val)
    return [return_val]


@error_handler
def get_rate_cards(processor_id, current_user_id):
    if os.path.exists('processor'):
        os.chdir('processor')
    api = gsapi.GsApi()
    api.input_config('gsapi.json')
    sheet_id = '10S4gHR_LBBTVeEpiLhSDJ0z4gCBAdqFRTywQdP-qS-k'
    api.sheet_id = sheet_id
    df = api.get_data()
    df = utl.first_last_adj(df, 1, 0)
    df = df.reset_index(drop=True)
    pn_col = 'Placement Name/Description'
    target_col = 'Targeting\n(ROS, targeted, etc.)'
    size_col = 'Ad Size\n(dimensions, aspect\nratio, etc.)'
    type_col = 'Ad Type\n(display, video, etc.)'
    device_col = 'Device\n(desktop, mobile, \ncross-device, etc.)'
    country_col = 'Country'
    bm_col = ' Buy Model\n(CPM, flat, etc.)'
    cpu_col = ' CPM / Cost Per Unit'
    cost_col = 'Net Cost (if flat)'
    partner_col = '{} Name'.format(Partner.__table__.name.capitalize())
    df = utl.data_to_type(df, float_col=[cost_col, cpu_col])
    df_dict = df.to_dict(orient='records')
    name = 'Rate Card Database'
    rfp_plan = Plan.query.filter_by(name=name).first()
    if not rfp_plan:
        rfp_plan = Plan(name=name)
        db.session.add(rfp_plan)
        db.session.commit()
    cur_phase = rfp_plan.get_current_children()
    if not cur_phase:
        cur_phase = PlanPhase(name=name, plan_id=rfp_plan.id)
        db.session.add(cur_phase)
        db.session.commit()
    else:
        cur_phase = cur_phase[0]
    rfp_file = RfpFile.query.filter_by(
        name=name, plan_id=rfp_plan.id).first()
    if not rfp_file:
        rfp_file = RfpFile(name=name, plan_id=rfp_plan.id,
                           user_id=current_user_id)
        db.session.add(rfp_file)
        db.session.commit()
    for rate_card in df_dict:
        partner_name = rate_card[partner_col]
        partner_obj = Partner.query.filter_by(
            name=partner_name, plan_phase_id=cur_phase.id).first()
        if not partner_obj:
            partner_obj = Partner(name=partner_name, plan_phase_id=cur_phase.id)
            db.session.add(partner_obj)
            db.session.commit()
        kwargs = {
            Rfp.partner_name.name: rate_card[partner_col],
            Rfp.placement_name_description.name: rate_card[pn_col],
            Rfp.targeting.name: rate_card[target_col],
            Rfp.ad_size_wxh.name: rate_card[size_col],
            Rfp.ad_type.name: rate_card[type_col],
            Rfp.device.name: rate_card[device_col],
            Rfp.country.name: rate_card[country_col],
            Rfp.buy_model.name: rate_card[bm_col],
            Rfp.cpm_cost_per_unit.name: rate_card[cpu_col],
            Rfp.planned_net_cost.name: rate_card[cost_col],
            Rfp.rfp_file_id.name: rfp_file.id,
            Rfp.partner_id.name: partner_obj.id
        }
        place = Rfp.query.filter_by(**kwargs).first()
        if not place:
            place = Rfp(**kwargs)
            db.session.add(place)
            db.session.commit()
        db.session.commit()
    return [df]
