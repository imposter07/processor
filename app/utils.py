import io
import os
import json
import re
from app import db
import pandas as pd
import datetime as dt
from app.models import Task, Processor, User, Campaign, Project, Client, \
    Product, Dashboard
from flask import current_app, render_template
from flask_babel import _
import uploader.upload.creator as cre
from xlrd.biffh import XLRDError


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
    ed = date_list[1]
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
                    x for x in processors if
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
            '.save_media_plan', _(msg_text),
            running_user=current_user.id,
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


class LiquidTable(object):
    id_col = 'liquid_table'

    def __init__(self, col_list=None, data=None, top_rows=None, totals=False,
                 title='', description='', columns_toggle=False,
                 accordion=False, specify_form_cols=True, col_dict=True,
                 select_val_dict=None, select_box=None, form_cols=None,
                 metric_cols=None, def_metric_cols=None, prog_cols=None,
                 header=None, highlight_row=None, new_modal_button=False,
                 col_filter=True, search_bar=True, chart_btn=True,
                 chart_func=None, chart_show=False, df=pd.DataFrame(),
                 row_on_click='', button_col=None, table_buttons=None,
                 highlight_type='blank', slider_edit_col='', slider_abs_col='',
                 prog_colors='success', download_table=False, filter_dict=None,
                 hidden_cols=None, link_cols=None, table_name='liquidTable'):
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
        if self.slider_edit_col:
            self.accordion = True
        self.df = df
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
            self.prog_cols, self.header, self.highlight_row, self.button_col,
            self.highlight_type, self.slider_edit_col, self.slider_abs_col,
            self.hidden_cols, self.link_cols)
        self.table_dict = self.make_table_dict(
            self.cols, self.data, self.top_rows, self.totals, self.title,
            self.description, self.columns_toggle, self.accordion,
            self.specify_form_cols, self.col_dict, self.table_name,
            self.new_modal_button, self.col_filter, self.search_bar,
            self.chart_btn, self.chart_func, self.chart_show, self.row_on_click,
            self.table_buttons, self.custom_cols, self.filter_dict)

    def create_buttons(self):
        if not self.table_buttons:
            self.table_buttons = []
        if self.download_table:
            btn = {'icon': {'classList': ["fa-solid", "fa-download"]},
                   'id': 'downloadBtn{}'.format(self.table_name)}
            self.table_buttons.append(btn)
        return self.table_buttons

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
                     metric_cols, def_metric_cols, prog_cols, header,
                     highlight_row, button_col, highlight_type,
                     slider_edit_col, slider_abs_col, hidden_cols, link_cols):
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
                if button_col and x in button_col:
                    cur_col.type = 'button_col'
                if hidden_cols and x in hidden_cols:
                    cur_col.hidden = cur_col.hidden_str
                if link_cols and x in link_cols:
                    cur_col.type = LiquidTableColumn.link_col_str
                    cur_col.link = link_cols[x]
                cur_col.update_dict()
                cols.append(cur_col.col_dict)
        return cols

    def make_table_dict(self, cols, data, top_rows, totals, title, description,
                        columns_toggle, accordion, specify_form_cols, col_dict,
                        table_name, new_modal_button, col_filter, search_bar,
                        chart_btn, chart_func, chart_show, row_on_click,
                        table_buttons, custom_cols, filter_dict):
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
                      'custom_cols': custom_cols, 'filter_dict': filter_dict}
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
        true_color = 'shadeCell3'
        false_color = 'shadeCell4'
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
