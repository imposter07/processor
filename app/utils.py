import io
import os
import sys
import json
import re
from app import db
import pandas as pd
import datetime as dt
import sqlalchemy as sqa
from app.models import Task, Processor, User, Campaign, Project, Client, \
    Product, Dashboard, Plan, RfpFile, Partner, Post, Uploader, Message, \
    PlanPhase, RateCard
from flask import current_app, render_template, jsonify, request, redirect, \
    url_for
import uploader.upload.creator as cre
import processor.reporting.analyze as az
import processor.reporting.vmcolumns as vmc
from xlrd.biffh import XLRDError
from functools import wraps


def launch_task(cur_class, name, description, running_user, task_class_args,
                *args, **kwargs):
    rq_job = current_app.task_queue.enqueue('app.tasks' + name,
                                            cur_class.id, running_user,
                                            *args, **kwargs)
    task = Task(id=rq_job.get_id(), name=name, description=description,
                user_id=cur_class.user_id, **task_class_args)
    db.session.add(task)
    return task


def error_handler(route_function):
    @wraps(route_function)
    def decorated_function(*args, **kwargs):
        try:
            result = route_function(*args, **kwargs)
            return result
        except:
            args = request.form.to_dict(flat=False)
            msg = 'Unhandled exception {}'.format(json.dumps(args))
            current_app.logger.error(msg, exc_info=sys.exc_info())
            data = {'data': 'error', 'task': '', 'level': 'error', 'args': args}
            return jsonify(data)

    return decorated_function


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


def clean_serialize_dict(data, cols):
    new_dict = {}
    for col in cols:
        new_dict[col] = get_col_from_serialize_dict(data, col)
    filter_idx = [v.replace('static_filters-', '').replace('-filter_col', '')
                  for k, v in data.items() if 'filter_col' in v and 'name' in k]
    new_dict['static_filters'] = []
    for filter_num in filter_idx:
        filter_dict = {}
        for col in ['filter_col', 'filter_val']:
            search_val = 'static_filters-{}-{}'.format(filter_num, col)
            col_vals = get_col_from_serialize_dict(data, search_val)
            filter_dict[col] = col_vals
        new_dict['static_filters'].append(filter_dict)
    return new_dict


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
    if not sd:
        sd = dt.datetime.today().strftime('%Y-%m-%d')
    if len(date_list) > 1:
        ed = date_list[1]
    else:
        ed = sd
    dict_to_add['start_date'] = dt.datetime.strptime(sd, '%Y-%m-%d')
    dict_to_add['end_date'] = dt.datetime.strptime(ed, '%Y-%m-%d')
    return dict_to_add


def sync_new_form_data_with_database(form_dict, old_db_items, db_model,
                                     relation_db_item, form_search_name='name',
                                     delete_children=False):
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
                if delete_children:
                    all_children = p.get_current_children()
                    for c in all_children:
                        gcs = c.get_current_children()
                        for gc in gcs:
                            db.session.delete(gc)
                        db.session.delete(c)
                db.session.delete(p)
    for p in form_dict:
        new_p = db_model()
        new_p.set_from_form(form=p, current_plan=relation_db_item)
        db.session.add(new_p)
        db.session.commit()


def convert_file_to_df(current_file):
    df = pd.read_csv(current_file)
    return df


def parse_filter_dict_from_clients(processors, seven_days_ago, current_request,
                                   filter_dict=None, db_model=Processor):
    if filter_dict:
        current_filters = filter_dict
    else:
        current_filters = json.loads(current_request.form['filter_dict'])
    proc_filter = [
        Processor.__table__.name, Processor, Processor.name.name,
        db_model.id]
    project_filter = [
        Project.__table__.name, Project, Project.project_number.name,
        '']
    if db_model == Project:
        proc_filter[3] = Project.processor_associated
        project_filter[3] = db_model.id
    else:
        project_filter[3] = db_model.projects
    filter_types = [
        (User.username.name, User, User.username.name, db_model.user_id),
        (Campaign.__table__.name, Campaign, Campaign.name.name,
         db_model.campaign_id),
        proc_filter,
        project_filter,
        (Client.__table__.name, Client, Client.name.name, ''),
        (Product.__table__.name, Product, Product.name.name, '')]
    live = [x for x in current_filters if 'live' in x.keys()]
    if live and live[0]['live']:
        processors = processors.filter(
            Processor.end_date > seven_days_ago.date())
    for filter_type in filter_types:
        filt_name = filter_type[0]
        cur_db_model = filter_type[1]
        db_attr = filter_type[2]
        proc_rel = filter_type[3]
        cur_filter = [x for x in current_filters if filt_name in x.keys()]
        if cur_filter and cur_filter[0][filt_name] and processors:
            cur_list = cur_filter[0][filt_name]
            if filt_name == Project.__table__.name:
                cur_list = [x.split('_')[0] for x in cur_list]
            user_list = []
            for x in cur_list:
                query = cur_db_model.query.filter(
                    getattr(cur_db_model, db_attr) == x)
                if query and query.first():
                    user_list.append(query.first().id)
            if filt_name == Client.__table__.name:
                processors = [
                    x for x in processors if x.campaign and
                    x.campaign.product.client_id in user_list]
            elif filt_name == Product.__table__.name:
                processors = [
                    x for x in processors if
                    x.campaign and x.campaign.product_id in user_list]
            elif filt_name == Project.__table__.name and db_model == Processor:
                processors = [x for x in processors
                              if any(e in [y.id for y in x.projects]
                                     for e in user_list)]
            else:
                processors = processors.filter(proc_rel.in_(user_list))
    return processors


def parse_additional_args(proc_arg):
    if 'dashboard_id' in proc_arg and proc_arg['dashboard_id']:
        dash = Dashboard.query.get(proc_arg['dashboard_id'])
        proc_arg['metrics'] = dash.get_metrics()
        proc_arg['dimensions'] = dash.get_dimensions()
    return proc_arg


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
        if not x.campaign:
            continue
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
    except (KeyError, ValueError, XLRDError) as e:
        msg = 'Try again with correct columns file was missing: {}'.format(e)
        current_app.logger.warning(msg)
    return df


def check_and_add_media_plan(media_plan_data, processor_to_edit,
                             object_type=Processor, current_user=None,
                             is_df=False):
    plan_saved = False
    if is_df or media_plan_data:
        if not current_user:
            current_user = User.query.get(processor_to_edit.user_id)
        if is_df:
            df = media_plan_data
        else:
            df = convert_media_plan_to_df(media_plan_data)
        if df.empty:
            return plan_saved
        msg_text = ('Attempting to save media plan for processor: {}'
                    ''.format(processor_to_edit.name))
        processor_to_edit.launch_task(
            '.save_media_plan', msg_text, running_user=current_user.id,
            media_plan=df, object_type=object_type)
        db.session.commit()
        plan_saved = True
    return plan_saved


def remove_special_characters(string):
    result = re.sub(r'[^a-zA-Z0-9 ]', '', string)
    result = re.sub(r'\s', '', result)
    return result


def column_contents_to_list(df, cols):
    un_col = 'undefined'
    for col in cols:
        if col in df.columns:
            if un_col not in df.columns:
                df[un_col] = df[col].astype('U')
            else:
                df[un_col] = df[un_col] + ' - ' + df[col].astype('U')
    cols_as_list = df[un_col].to_list()
    return cols_as_list


def set_form_contents_from_db(cur_obj, sql_choices=None):
    if not sql_choices:
        sql_choices = [(Client, cur_obj.cur_client),
                       (Product, cur_obj.cur_product),
                       (Campaign, cur_obj.cur_campaign)]
    for obj in sql_choices:
        choices = [('', '')]
        choices.extend(set([(x.name, x.name) for x in obj[0].query.all()]))
        obj[1].choices = choices


def create_local_path(cur_obj):
    if not cur_obj.local_path:
        file_path_elems = [
            '/mnt', 'c', 'clients', cur_obj.campaign.product.client.name,
            cur_obj.campaign.product.name, cur_obj.campaign.name, cur_obj.name,
            cur_obj.__table__.name]
        base_path = ''
        for x in file_path_elems:
            base_path = os.path.join(base_path, x)
        tmp_dir = current_app.config['TMP_DIR']
        if tmp_dir:
            base_path = base_path.replace('/mnt', 'mnt')
            base_path = os.path.join(tmp_dir, base_path)
    else:
        base_path = cur_obj.local_path
    return base_path


def object_post_message(proc, usr, text, run_complete=False,
                        request_id=False, object_name='Processor'):
    if len(text) > 139:
        msg_body = text[:139]
    else:
        msg_body = text
    msg = Message(author=usr, recipient=usr, body=msg_body)
    db.session.add(msg)
    usr.add_notification('unread_message_count', usr.new_messages())
    if object_name == Uploader.__name__:
        post = Post(body=text, author=usr, uploader_id=proc.id)
    elif (object_name == Plan.__name__ or
          proc.__table__.name == Plan.__table__.name):
        post = Post(body=text, author=usr, plan_id=proc.id)
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
        proc.last_run_time = dt.datetime.utcnow()
        db.session.commit()


def get_obj_user(object_id, current_user_id, db_model=Processor):
    processor_to_run = db.session.get(db_model, object_id)
    user_that_ran = db.session.get(User, current_user_id)
    return processor_to_run, user_that_ran


def set_db_values(object_id, current_user_id, form_sources, table,
                  parent_model=Processor, additional_filter=None,
                  post_model=None):
    update_msg = 'Attempt {}: {}'.format(table.__name__, form_sources)
    current_app.logger.info(update_msg[:1000])
    cur_processor, user_that_ran = get_obj_user(
        object_id=object_id, current_user_id=current_user_id,
        db_model=parent_model)
    if parent_model == Plan:
        key = table.plan_id.name
    elif parent_model == RfpFile:
        key = table.rfp_file_id.name
    elif parent_model == Partner:
        key = table.partner_id.name
    elif parent_model == PlanPhase:
        key = table.plan_phase_id.name
    elif parent_model == RateCard:
        key = table.rate_card_id.name
    else:
        key = table.processor_id.name
    filter_dict = {key: object_id}
    if additional_filter:
        for k, v in additional_filter.items():
            filter_dict[k] = v
    old_items = table.query.filter_by(**filter_dict).all()
    change_log = {'add': [], 'delete': [], 'update': []}
    form_ids = []
    for form in form_sources:
        cur_item = None
        if 'id' in form:
            cur_id = form['id']
            cur_item = db.session.get(table, cur_id)
        elif (hasattr(table, 'unique_name') and table.unique_name and
              'name' in form and form['name']):
            filter_dict['name'] = form['name']
            cur_item = table.query.filter_by(**filter_dict).first()
        if cur_item:
            form_ids.append(cur_item.id)
            for k, v in form.items():
                cur_dict = cur_item.__dict__
                if k in cur_dict and v != cur_dict and k != 'id':
                    setattr(cur_item, k, v)
                    update_dict = {'col': k, 'id': cur_item.id, 'val': v}
                    change_log['update'].append(update_dict)
        else:
            t = table()
            t.set_from_form(form, cur_processor)
            db.session.add(t)
            db.session.commit()
            item_name = t.name if hasattr(t, 'name') else None
            add_dict = {'id': t.id, 'name': item_name}
            change_log['add'].append(add_dict)
    if old_items:
        for item in old_items:
            try:
                item_id = item.id
            except sqa.orm.exc.ObjectDeletedError:
                continue
            if item_id not in form_ids:
                item_name = item.name if hasattr(item, 'name') else None
                delete_dict = {'id': item.id, 'name': item_name}
                change_log['delete'].append(delete_dict)
                db.session.delete(item)
    db.session.commit()
    msg_text = "{} {} {} set.".format(
        parent_model.__name__, cur_processor.name, table.__name__)
    if parent_model in [RfpFile, Partner, PlanPhase]:
        if parent_model == Partner:
            plan_id = cur_processor.plan.plan_id
        else:
            plan_id = cur_processor.plan_id
        cur_processor = db.session.get(Plan, plan_id)
        parent_model = Plan
    if post_model:
        cur_processor = post_model
        parent_model = type(cur_processor)
    object_post_message(cur_processor, user_that_ran, msg_text,
                        object_name=parent_model.__name__)
    update_msg = 'Updated {}: {}'.format(table.__name__, change_log)
    current_app.logger.info(update_msg[:1000])
    return change_log


def set_fees_from_form(obj, form):
    obj.digital_agency_fees = form.digital_agency_fees.data
    obj.trad_agency_fees = form.trad_agency_fees.data
    obj.rate_card_id = form.rate_card.data.id
    service_fees = int(form.dcm_service_fees.data.replace('%', '')) / 100
    obj.dcm_service_fees = service_fees
    db.session.commit()
    return obj


def obj_fees_route(object_name, current_user, object_type=Processor,
                   kwargs=None, template='create_processor.html'):
    from app.main.forms import FeeForm
    form_description = """
        Set the adserving, reporting and agency fees used by the processor.
        Adserving rates by type can be edited and saved using 'View Rate Card'.
        Old rate cards can be selected and used from the dropdown.
        Note Digital and Traditional Agency Fees should be provided as decimal.
        """
    if not kwargs:
        kwargs = object_type().get_current_processor(
            object_name, current_page='edit_processor_fees', edit_progress=75,
            edit_name='Fees', buttons='ProcessorRequest',
            form_title='FEES', form_description=form_description)
    cur_proc = object_type.query.filter_by(name=object_name).first()
    form = FeeForm()
    if request.method == 'POST':
        form.validate()
        set_fees_from_form(cur_proc, form)
        other_obj = Processor if object_type.__name__ == Plan.__name__ else Plan
        other_obj = cur_proc.has_related_object(other_obj.__name__).first()
        if other_obj:
            set_fees_from_form(other_obj, form)
        creation_text = '{} fees were edited.'.format(object_type.__name__)
        object_post_message(cur_proc, current_user, text=creation_text,
                            object_name=object_type.__name__)
        return form_continue_redirect(kwargs['buttons'], kwargs['edit_name'],
                                      form, cur_proc)
    elif request.method == 'GET':
        form.digital_agency_fees.data = cur_proc.digital_agency_fees
        form.trad_agency_fees.data = cur_proc.trad_agency_fees
        form_rate_card = RateCard.query.filter_by(
            id=cur_proc.rate_card_id).first()
        form.rate_card.data = form_rate_card
        if cur_proc.dcm_service_fees:
            dcm_fee = '{}%'.format(round(cur_proc.dcm_service_fees * 100))
        else:
            dcm_fee = '0%'
        form.dcm_service_fees.data = dcm_fee
    kwargs['form'] = form
    return render_template(template, **kwargs)


def get_next_route_from_buttons(buttons, edit_name):
    cur_route = None
    next_route = None
    for x in buttons:
        cur_name = next(iter(x))
        if cur_route:
            next_route = x[cur_name]['route']
            break
        if edit_name == cur_name:
            cur_route = x[cur_name]['route']
    if not next_route:
        next_route = cur_route
    return cur_route, next_route


def form_continue_redirect(buttons, edit_name, form, cur_obj):
    cr, nr = get_next_route_from_buttons(buttons, edit_name)
    endpoint = cr
    if form.form_continue.data == 'continue':
        endpoint = nr
    return redirect(url_for(endpoint, object_name=cur_obj.name))


def check_and_add_parents():
    name = Client.get_default_name()[0]
    cli = Client(name=name).check_and_add()
    pro = Product(name=name, client_id=cli.id).check_and_add()
    cam = Campaign(name=name, product_id=pro.id).check_and_add()
    return name, cli, pro, cam


class LiquidTable(object):
    id_col = 'liquid_table'

    def __init__(self, col_list=None, data=None, top_rows=None, totals=False,
                 title='', description='', columns_toggle=False,
                 accordion=False, specify_form_cols=True, col_dict=True,
                 select_val_dict=None, select_box=None, form_cols=None,
                 metric_cols=None, def_metric_cols=None, prog_cols=None,
                 trending_cols=None, trending_groupbys=None,
                 trending_metrics=None, trending_periods=None,
                 header=None, highlight_row=None, new_modal_button=False,
                 col_filter=True, search_bar=True, chart_btn=True,
                 chart_func=None, chart_show=False, df=pd.DataFrame(),
                 row_on_click='', button_col=None, table_buttons=None,
                 highlight_type='blank', slider_edit_col='', slider_abs_col='',
                 prog_colors='success', download_table=False, filter_dict=None,
                 hidden_cols=None, link_cols=None, cell_pick_cols=None,
                 metadata=None, total_default_val=0,
                 inline_edit=False, table_name='liquidTable',
                 percent_total_cols=None, percent_total_groupbys=None,
                 chart_args=None,):
        self.col_list = col_list
        self.data = data
        self.top_rows = top_rows
        self.totals = totals
        self.title = title
        self.description = description
        self.columns_toggle = columns_toggle
        self.accordion = accordion
        self.specify_form_cols = specify_form_cols
        self.col_dict = col_dict
        self.select_val_dict = select_val_dict
        self.select_box = select_box
        self.form_cols = form_cols
        self.metric_cols = metric_cols
        self.def_metric_cols = def_metric_cols
        self.prog_cols = prog_cols
        self.prog_colors = prog_colors
        self.trending_cols = trending_cols
        self.trending_metrics = trending_metrics
        self.trending_groupbys = (
            trending_groupbys if trending_groupbys else [[]])
        self.trending_periods = trending_periods if trending_periods else []
        self.percent_total_cols = percent_total_cols
        self.percent_total_groupbys = (
            percent_total_groupbys if percent_total_groupbys else [])
        self.header = header
        self.highlight_row = highlight_row
        self.table_name = table_name
        self.new_modal_button = new_modal_button
        self.col_filter = col_filter
        self.search_bar = search_bar
        self.chart_btn = chart_btn
        self.chart_func = chart_func
        self.chart_show = chart_show
        self.row_on_click = row_on_click
        self.button_col = button_col
        self.table_buttons = table_buttons
        self.highlight_type = highlight_type
        self.download_table = download_table
        self.slider_edit_col = slider_edit_col
        self.slider_abs_col = slider_abs_col
        self.filter_dict = filter_dict
        self.hidden_cols = hidden_cols
        self.link_cols = link_cols
        self.cell_pick_cols = cell_pick_cols
        self.metadata = metadata
        self.total_default_val = total_default_val
        self.inline_edit = inline_edit
        self.chart_args = chart_args
        if self.slider_edit_col:
            self.accordion = True
        self.df = df
        if self.trending_cols or self.percent_total_cols:
            self.calculated_cols()
        self.build_from_df()
        self.form_cols = self.check_form_cols(
            self.form_cols, self.specify_form_cols, self.col_list)
        self.custom_cols = []
        self.rows_name = None
        self.top_rows_name = None
        self.liquid_table = True
        self.table_buttons = self.create_buttons()
        self.cols = self.make_columns(
            self.col_list, self.select_val_dict, self.select_box,
            self.form_cols, self.metric_cols, self.def_metric_cols,
            self.prog_cols, self.trending_cols, self.header, self.highlight_row,
            self.button_col, self.highlight_type, self.slider_edit_col,
            self.slider_abs_col, self.hidden_cols, self.link_cols,
            self.cell_pick_cols)
        self.table_dict = self.make_table_dict(
            self.cols, self.data, self.top_rows, self.totals, self.title,
            self.description, self.columns_toggle, self.accordion,
            self.specify_form_cols, self.col_dict, self.table_name,
            self.new_modal_button, self.col_filter, self.search_bar,
            self.chart_btn, self.chart_func, self.chart_show, self.row_on_click,
            self.table_buttons, self.custom_cols, self.filter_dict,
            self.metadata, self.total_default_val, self.inline_edit,
            self.chart_args)

    def create_buttons(self):
        if not self.table_buttons:
            self.table_buttons = []
        if self.download_table:
            btn = {'icon': {'classList': ["bi", "bi-download"]},
                   'id': 'downloadBtn{}'.format(self.table_name)}
            self.table_buttons.append(btn)
        return self.table_buttons

    def calculated_cols(self):
        value_cal = az.ValueCalc()
        if self.trending_cols:
            # format: trending_cols=['Name of new Trending Col']
            #         trending_groupbys=[['Dimension(s) to Calc Trending On']]
            #         trending_metrics=['Metric to Calc Trending On']
            # Default adds trending arrows
            for i in range(len(self.trending_cols)):
                col_name = self.trending_cols[i]
                metric = self.trending_metrics[i]
                groupby = self.trending_groupbys[i]
                period = (self.trending_periods[i]
                          if i < len(self.trending_periods) else 1)
                self.df = value_cal.calculate_trending(
                    self.df, col_name, metric, groupby, period)
        if self.percent_total_cols:
            # format: percent_total_cols=['Metric to Calc % Total On]
            #         trending_groupbys=[['Dimension(s) to Calc % Total On']]
            # Default does NOT add progress bars
            for i in range(len(self.percent_total_cols)):
                metric = self.percent_total_cols[i]
                groupby = (self.percent_total_groupbys[i]
                           if i < len(self.percent_total_groupbys)
                           else ['eventdate'])
                self.df = value_cal.calculate_percent_total(
                    self.df, metric, groupby)

    def build_from_df(self):
        if self.df.columns.tolist():
            self.df = self.df.fillna('None')
            self.data = self.df.to_dict(orient='records')
            self.col_list = self.df.columns.tolist()

    @staticmethod
    def check_form_cols(form_cols, specify_form_cols, col_list):
        if specify_form_cols and not form_cols:
            form_cols = col_list
        return form_cols

    def make_columns(self, col_list, select_val_dict, select_box, form_cols,
                     metric_cols, def_metric_cols, prog_cols, trending_cols,
                     header, highlight_row, button_col, highlight_type,
                     slider_edit_col, slider_abs_col, hidden_cols, link_cols,
                     cell_pick_cols):
        cols = []
        if col_list:
            for x in col_list:
                cur_col = LiquidTableColumn(name=x)
                if select_val_dict and x in select_val_dict:
                    cur_col.make_select()
                    cur_col.values = select_val_dict[x]
                if select_box and x == select_box:
                    cur_col.add_select_box = True
                    self.rows_name = x
                if form_cols and x in form_cols:
                    cur_col.form = True
                if metric_cols and x in metric_cols:
                    cur_col.type = 'metrics'
                    if def_metric_cols and x in def_metric_cols:
                        cur_col.type = 'default_metrics'
                if header and x == header:
                    cur_col.make_header()
                    self.top_rows_name = x
                if highlight_row and x == highlight_row:
                    ht = cur_col.parse_highlight_type(highlight_type)
                    cur_col.highlight_row = ht
                if slider_edit_col and x == slider_edit_col:
                    cur_col.form = True
                    cur_col.type = cur_col.slider_edit_col_str
                if slider_abs_col and x == slider_abs_col:
                    cur_col.type = cur_col.slider_abs_col_str
                if prog_cols and x in prog_cols:
                    custom_col = cur_col.parse_prog_bars(x, self.prog_colors)
                    self.custom_cols.append(custom_col)
                if trending_cols and x in trending_cols:
                    custom_col = cur_col.parse_trending_col(x)
                    self.custom_cols.append(custom_col)
                if button_col and x in button_col:
                    cur_col.type = 'button_col'
                if hidden_cols and x in hidden_cols:
                    cur_col.hidden = cur_col.hidden_str
                if link_cols and x in link_cols:
                    cur_col.type = LiquidTableColumn.link_col_str
                    cur_col.link = link_cols[x]
                if cell_pick_cols and x in cell_pick_cols:
                    cur_col.type = LiquidTableColumn.cell_pick_col_str
                cur_col.update_dict()
                cols.append(cur_col.col_dict)
        return cols

    def make_table_dict(self, cols, data, top_rows, totals, title, description,
                        columns_toggle, accordion, specify_form_cols, col_dict,
                        table_name, new_modal_button, col_filter, search_bar,
                        chart_btn, chart_func, chart_show, row_on_click,
                        table_buttons, custom_cols, filter_dict, metadata,
                        total_default_val, inline_edit, chart_args):
        table_dict = {'data': data, 'rows_name': self.rows_name, 'cols': cols,
                      'top_rows': top_rows, 'top_rows_name': self.top_rows_name,
                      'totals': totals, 'title': title,
                      'description': description,
                      'columns_toggle': columns_toggle, 'accordion': accordion,
                      'specify_form_cols': specify_form_cols,
                      'col_dict': col_dict, 'name': table_name,
                      self.id_col: self.liquid_table,
                      'new_modal_button': new_modal_button,
                      'col_filter': col_filter, 'search_bar': search_bar,
                      'chart_btn': chart_btn, 'chart_func': chart_func,
                      'chart_show': chart_show, 'row_on_click': row_on_click,
                      'table_buttons': table_buttons,
                      'custom_cols': custom_cols, 'filter_dict': filter_dict,
                      'metadata': metadata,
                      'total_default_val': total_default_val,
                      'inline_edit': inline_edit,
                      'chart_args': chart_args}
        return table_dict

    @staticmethod
    def convert_sd_ed_to_weeks(sd, ed):
        weeks = [sd + dt.timedelta(days=x)
                 for i, x in enumerate(range((ed - sd).days)) if i % 7 == 0]
        week_str = [dt.datetime.strftime(x, '%Y-%m-%d') for x in weeks]
        return week_str

    @staticmethod
    def combine_table_dicts(td_one, td_two):
        if not td_one['data']:
            table_dict = td_two
        elif not td_two['data']:
            table_dict = td_one
        else:
            table_dict = td_one
            table_dict['data'] = dict(
                td_one['data'].items() + td_two['data'].items())
        return table_dict


class LiquidTableColumn(object):
    name_str = 'name'
    type_str = 'type'
    values_str = 'values'
    add_select_box_str = 'add_select_box'
    hidden_str = 'hidden'
    header_str = 'header'
    form_str = 'form'
    highlight_row_str = 'highlight_row'
    slider_edit_col_str = 'slider_edit_col'
    slider_abs_col_str = 'slider_abs_col'
    link_col_str = 'link_col'
    cell_pick_col_str = 'cell_pick_col'

    def __init__(self, name, col_type='', values=None, add_select_box=False,
                 hidden=False, header=False, form=False, highlight_row='',
                 slider_edit_col=''):
        self.name = name
        self.type = col_type
        self.values = values
        self.add_select_box = add_select_box
        self.hidden = hidden
        self.header = header
        self.form = form
        self.highlight_row = highlight_row
        self.slider_edit_col = slider_edit_col
        self.link = ''
        self.col_dict = self.update_dict()

    def update_dict(self):
        self.col_dict = {
            self.name_str: self.name,
            self.type_str: self.type,
            self.values_str: self.values,
            self.add_select_box_str: self.add_select_box,
            self.hidden_str: self.hidden,
            self.header_str: self.header,
            self.form_str: self.form,
            self.highlight_row_str: self.highlight_row,
            self.slider_edit_col_str: self.slider_edit_col,
            self.link_col_str: self.link}
        return self.col_dict

    def make_select(self):
        self.type = 'select'
        self.update_dict()

    def make_header(self):
        self.hidden = True
        self.header = True
        self.make_select()

    @staticmethod
    def parse_highlight_type(highlight_type):
        full_row = True
        true_color = 'table-success'
        false_color = 'table-danger'
        comparison_values = 'true'
        comparator = '==='
        if highlight_type == 'blank':
            true_color = false_color
            false_color = ''
            comparison_values = ["$0", "0", ""]
            comparator = 'includes'
        ht = {
            'comparator': comparator, 'comp_val': comparison_values,
            'true_color': true_color, 'false_color': false_color,
            'full_row': full_row}
        return ht

    @staticmethod
    def parse_prog_bars(col, color):
        custom_func = {'func': 'addProgressBars', 'args': [col, color]}
        return custom_func

    @staticmethod
    def parse_trending_col(col):
        custom_func = {'func': 'addTrendingArrows', 'args': [col]}
        return custom_func
