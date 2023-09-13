from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, TextAreaField, SelectField, \
    FormField, FieldList, HiddenField, DateTimeField, FileField, BooleanField, \
    DecimalField, SelectMultipleField, MultipleFileField, DateField, TimeField

from wtforms_sqlalchemy.fields import QuerySelectField
from wtforms.validators import ValidationError, DataRequired, Length, Regexp
from flask_babel import _, lazy_gettext as _l
from app.models import User, Processor, Client, Product, Campaign, Uploader, \
    RateCard, ProcessorDatasources, ProcessorAnalysis, PartnerPlacements
import processor.reporting.dictcolumns as dctc
import processor.reporting.vmcolumns as vmc
import processor.reporting.export as exp
import uploader.upload.creator as cre


class EditProfileForm(FlaskForm):
    username = StringField(_l('Username'), validators=[DataRequired()])
    about_me = TextAreaField(_l('About me'),
                             validators=[Length(min=0, max=140)])
    submit = SubmitField(_l('Submit'))

    def __init__(self, original_username, *args, **kwargs):
        super(EditProfileForm, self).__init__(*args, **kwargs)
        self.original_username = original_username

    def validate_username(self, username):
        if username.data != self.original_username:
            user = User.query.filter_by(username=self.username.data).first()
            if user is not None:
                raise ValidationError(_('Please use a different username.'))


class PostForm(FlaskForm):
    post = TextAreaField(_l('Say something'), validators=[DataRequired()])
    submit = SubmitField(_l('Submit'))


class SearchForm(FlaskForm):
    q = StringField(_l('Search'), validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        if 'formdata' not in kwargs:
            kwargs['formdata'] = request.args
        if 'csrf_enabled' not in kwargs:
            kwargs['csrf_enabled'] = False
        super(SearchForm, self).__init__(*args, **kwargs)


class MessageForm(FlaskForm):
    message = TextAreaField(_l('Message'), validators=[
        DataRequired(), Length(min=0, max=140)])
    submit = SubmitField(_l('Submit'))


class ProcessorForm(FlaskForm):
    name = StringField(_l('Name'), render_kw={'readonly': False}, validators=[
        DataRequired(), Regexp("[^']", message='Remove special characters')])
    description = StringField(_l('Description'), validators=[
        DataRequired()])
    brandtracker_toggle = BooleanField(_l('Brand Tracker'))
    local_path = StringField(_l('Local Path'), validators=[DataRequired()],
                             render_kw={'readonly': True})
    tableau_workbook = StringField(_l('Tableau Workbook'))
    tableau_view = StringField(_l('Tableau View'))
    tableau_datasource = StringField(_l('Tableau Datasource'))
    start_date = DateField(_l('Start Date'))
    end_date = DateField(_l('End Date'))
    cur_client = SelectField(_l('Client'))
    cur_product = SelectField(_l('Product'))
    cur_campaign = SelectField(_l('Campaign'))
    form_continue = HiddenField('form_continue')

    def validate_name(self, name):
        processor = Processor.query.filter_by(name=name.data).first()
        if processor is not None:
            raise ValidationError(_l('Please use a different name.'))

    def set_choices(self):
        for obj in [(Client, self.cur_client),
                    (Product, self.cur_product), (Campaign, self.cur_campaign)]:
            choices = [('', '')]
            choices.extend(set([(x.name, x.name) for x in obj[0].query.all()]))
            obj[1].choices = choices


class APIForm(FlaskForm):
    vendor_key = StringField('Vendor Key', render_kw={'readonly': True})
    name = StringField('Name', validators=[Regexp(r'\W+')])
    key = SelectField('API Type', choices=[(x, x) for x in vmc.api_keys])
    account_id = StringField('Account ID')
    start_date = DateField('Start Date', format='%Y-%m-%d')
    account_filter = StringField('Filter')
    api_fields = StringField('API Fields')
    test_connection = SubmitField('Test Connection', render_kw={
        'style': 'visibility:hidden'})
    refresh_import_config = SubmitField('Edit Config File',
                                        render_kw={'type': 'button'})
    raw_file = FileField('Raw File')
    delete = SubmitField('Delete', render_kw={'style': 'background-color:red',
                                              'type': 'button'})


class ImportForm(FlaskForm):
    data_source = SelectField(_l('Processor Data Source Filter'))
    refresh_imports = SubmitField('Refresh From Processor')
    add_child = SubmitField(label='Add API', render_kw={'type': 'button'})
    form_continue = HiddenField('form_continue')
    apis = FieldList(FormField(APIForm, label=''))

    def set_apis(self, cur_proc):
        test_conn_col = 'test_connection'
        proc_imports = cur_proc.get_import_form_dicts(reverse_sort_apis=True)
        for imp in proc_imports:
            if imp['key'] in vmc.test_apis:
                imp[test_conn_col] = True
            else:
                imp[test_conn_col] = False
        self.apis = proc_imports
        return proc_imports

    def set_vendor_key_choices(self, current_processor_id):
        choices = [('', '')]
        choices.extend([(x.vendor_key, x.vendor_key) for x in
                        ProcessorDatasources.query.filter_by(
                            processor_id=current_processor_id).all()])
        self.data_source.choices = choices

    def set_vendor_type_choices(self):
        choices = [('', '')]
        choices.extend([(x, x) for x in vmc.api_keys])
        self.data_source.choices = choices


class BrandTrackerImportForm(ImportForm):
    table_data = HiddenField('table_data')

    @staticmethod
    def make_brandtracker_sources(table_imports, card_imports,
                                  current_processor):
        table_cols = ['GAME TITLE', 'TWITTER HANDLE']
        game_title, tw_handle = table_cols
        form_imports = [source for source in card_imports if 'BTCard' in
                        source['name']]
        if not table_imports:
            return form_imports
        start_date = current_processor.start_date
        shared_input = {'start_date': start_date,
                        'account_id': '',
                        'account_filter': '',
                        'api_fields': ''}
        col_data = {col: [] for col in table_imports[0].keys()}
        batch_size = 10
        for col in col_data:
            col_data[col] = [x[col] for x in table_imports if x[col]]
            for batch_num, idx in enumerate(
                    range(0, len(col_data[col]), batch_size)):
                batch_data = col_data[col][idx:idx+batch_size]
                source = shared_input.copy()
                source['name'] = 'batch{}'.format(batch_num)
                if col == game_title:
                    source['key'] = vmc.api_nz_key
                    source['account_id'] = ','.join(batch_data)
                    source['account_filter'] = 'US,CA'
                elif col == tw_handle:
                    source['key'] = vmc.api_tw_key
                    source['api_fields'] = 'USER_STATS:{}'.format(
                        ','.join(batch_data)
                    )
                else:
                    continue
                vk = '_'.join(['API', source['key'], source['name']])
                source['vendor_key'] = vk
                source['original_vendor_key'] = vk
                card = [x for x in card_imports if x['vendor_key'] == vk]
                if card:
                    card = card[0]
                    source = {k: (v if v else card[k])
                              for k, v in source.items()}
                form_imports.append(source)
        return form_imports


class EditProcessorForm(ProcessorForm):
    def __init__(self, original_name, *args, **kwargs):
        super(EditProcessorForm, self).__init__(*args, **kwargs)
        self.original_name = original_name
        self.name.render_kw['readonly'] = True

    def validate_name(self, name):
        if name.data != self.original_name:
            processor = Processor.query.filter_by(name=self.name.data).first()
            if processor is not None:
                raise ValidationError(_l('Please use a different name.'))


class DataSourceForm(FlaskForm):
    vendor_key = StringField(_l('Vendor Key'))
    refresh_raw_data = SubmitField(_l('Show Raw Data'))
    refresh_download_raw_data = SubmitField(
        _l('Download Raw Data'), render_kw={'style': 'background-color:green'})
    full_placement_columns = TextAreaField(_l('Full Placement Columns'))
    placement_columns = StringField(_l('Placement Column'))
    auto_dictionary_placement = SelectField(_l('Auto Dict Placement'), choices=[
        (x, x) for x in [dctc.FPN, dctc.PN]])
    refresh_dictionary_order = SubmitField(_l('Show Dictionary Order'))
    auto_dictionary_order = TextAreaField(_l('Auto Dictionary Order'))
    refresh_delete_dict = SubmitField(
        _l('Delete Dictionary'), render_kw={'style': 'background-color:red'})
    refresh_dictionary = SubmitField(_l('Show Dictionary'))
    active_metrics = TextAreaField(_l('Active Metrics'))
    vm_rules = TextAreaField(_l('Vendor Matrix Rules'))
    original_vendor_key = HiddenField('Original Vendor Key')

    def __init__(self, *args, **kwargs):
        super(DataSourceForm, self).__init__(*args, **kwargs)
        self.original_vendor_key = self.vendor_key


class ProcessorCleanForm(FlaskForm):
    data_source_clean = SelectField(_l('Processor Data Source Filter'))
    # refresh_data_sources = SubmitField(_l('Refresh From Processor'))
    form_continue = HiddenField('form_continue')
    datasources = FieldList(FormField(DataSourceForm, label=''))

    def __init__(self, *args, **kwargs):
        super(ProcessorCleanForm, self).__init__(*args, **kwargs)

    def set_datasources(self, data_source, cur_proc):
        imp_dict = []
        ds = data_source.query.filter_by(processor_id=cur_proc.id).all()
        for source in ds:
            form_dict = source.get_ds_form_dict()
            imp_dict.append(form_dict)
        self.datasources = imp_dict
        return imp_dict

    def set_vendor_key_choices(self, current_processor_id):
        choices = [('', '')]
        choices.extend([(x.vendor_key, x.vendor_key) for x in
                        ProcessorDatasources.query.filter_by(
                            processor_id=current_processor_id).all()])
        self.data_source_clean.choices = choices


class TranslationForm(FlaskForm):
    column_name = SelectField('Column Name',
                              choices=[(x, x) for x in dctc.COLS])
    value = StringField('Current Value')
    new_value = StringField('New Value')


class TranslationBroadForm(FlaskForm):
    add_child = SubmitField(label='Add Translation')
    remove_api = SubmitField('Remove Last Translation')
    translations = FieldList(FormField(TranslationForm))


class PlacementForm(FlaskForm):
    full_placement_columns = SelectMultipleField(_l('Full Placement Columns'))
    placement_columns = SelectField(_l('Placement Column'))
    auto_dictionary_placement = SelectField(_l('Auto Dict Placement'), choices=[
        (x, x) for x in [dctc.FPN, dctc.PN]])
    auto_dictionary_order = SelectMultipleField(_l('Auto Dictionary Order'))

    def __init__(self, *args, **kwargs):
        super(PlacementForm, self).__init__(*args, **kwargs)

    def set_column_choices(self, current_ds_id, ds_dict):
        import processor.reporting.analyze as az
        import pandas as pd
        ds = ProcessorDatasources.query.filter_by(id=current_ds_id).first()
        all_analysis = ProcessorAnalysis.query.filter_by(
            processor_id=ds.processor_id, key=az.Analyze.raw_columns).first()
        if all_analysis and all_analysis.data:
            df = pd.DataFrame(all_analysis.data)
            raw_cols = df[df[vmc.vendorkey] == ds.vendor_key][
                az.Analyze.raw_columns]
        else:
            raw_cols = []
        if len(raw_cols) > 0:
            raw_cols = raw_cols[0]
        else:
            raw_cols = []
        fp_cols = ds_dict['full_placement_columns']
        fp_cols = [x for x in fp_cols if x not in raw_cols and x[:2] != '::']
        choices = [(x, x) for x in fp_cols + raw_cols]
        self.placement_columns.choices = choices
        fp_cols = ds_dict['full_placement_columns']
        fp_cols = [x for x in fp_cols if x not in raw_cols]
        full_choices = [(x, x) for x in fp_cols + raw_cols]
        full_choices.extend([('::' + x, '::' + x) for x in fp_cols + raw_cols
                             if x[:2] != '::' and ('::' + x, '::' + x) not in
                             full_choices])
        for col in reversed(ds_dict['full_placement_columns']):
            full_choices.insert(0, full_choices.pop(
                full_choices.index((col, col))))
        self.full_placement_columns.choices = full_choices
        auto_choices = ds_dict['auto_dictionary_order']
        dict_choices = [x for x in dctc.COLS if x not in auto_choices]
        choices = [(x, x) for x in auto_choices + dict_choices]
        self.auto_dictionary_order.choices = choices


class ProcessorExportForm(FlaskForm):
    tableau_workbook = StringField(_l('Tableau Workbook'))
    tableau_view = StringField(_l('Tableau View'))
    tableau_datasource = StringField(_l('Tableau Datasource'))
    schedule = BooleanField(_l('Schedule'), )
    schedule_start = DateField(_l('Schedule Start'), format='%Y-%m-%d')
    schedule_end = DateField(_l('Schedule End'), format='%Y-%m-%d')
    run_time = TimeField(_l('Run Time'))
    interval = SelectField(_l('Hourly Interval'),
                           choices=[(x, x) for x in range(1, 25)], default='24')
    form_continue = HiddenField('form_continue')


class ProcessorRequestForm(FlaskForm):
    name = StringField(_l('Name'), validators=[
        DataRequired(), Regexp("[^']", message='Remove special characters')])
    brandtracker_toggle = BooleanField(_l('Brand Tracker'))
    description = StringField(_l('Description'), validators=[
        DataRequired()])
    plan_path = StringField(_l('Media Plan Path'), validators=[DataRequired()])
    start_date = DateField(_l('Start Date'), validators=[DataRequired()])
    end_date = DateField(_l('End Date'), validators=[DataRequired()])
    first_report = DateField(_l('First Report Date'))
    cur_client = SelectField(_l('Client'), validators=[DataRequired()])
    cur_product = SelectField(_l('Product'), validators=[DataRequired()])
    cur_campaign = SelectField(_l('Campaign'), validators=[DataRequired()])
    client_name = None
    product_name = None
    campaign_name = None
    form_continue = HiddenField('form_continue')

    def validate_name(self, name):
        processor = Processor.query.filter_by(name=name.data).first()
        if processor is not None:
            raise ValidationError(_l('Please use a different name.'))

    def set_choices(self):
        for obj in [(Client, self.cur_client),
                    (Product, self.cur_product), (Campaign, self.cur_campaign)]:
            choices = [('', '')]
            choices.extend(set([(x.name, x.name) for x in obj[0].query.all()]))
            obj[1].choices = choices


class EditProcessorRequestForm(ProcessorRequestForm):
    def __init__(self, original_name, *args, **kwargs):
        super(EditProcessorRequestForm, self).__init__(*args, **kwargs)
        self.original_name = original_name

    def validate_name(self, name):
        if name.data != self.original_name:
            processor = Processor.query.filter_by(name=self.name.data).first()
            if processor is not None:
                raise ValidationError(_l('Please use a different name.'))


class ProcessorPlanForm(FlaskForm):
    plan_properties = SelectMultipleField(
        _('Plan Properties'), choices=[
            (x, x) for x in Processor.get_plan_properties()],
        description='Select tasks you want the uploaded plan to accomplish.')
    plan = FileField(_l('Media Plan'))
    plan_property_view = SelectField(
        _('View Plan Properties'), choices=[
            (x, x) for x in [''] + Processor.get_plan_properties()],
        description='Select to view current properties as a table.')
    form_continue = HiddenField('form_continue')


class AccountForm(FlaskForm):
    key = SelectField(
        'Account Type', choices=[(x, x) for x in vmc.api_keys])
    account_id = StringField('Account ID')
    campaign_id = StringField(
        'Campaign ID or Name',
        description='For many accounts this value is optional. '
                    ' Double check your account in the walkthrough '
                    '(question mark on left sidebar next to comments).')
    delete = SubmitField('Delete', render_kw={'style': 'background-color:red'})


class GeneralAccountForm(FlaskForm):
    add_child = SubmitField(label='Add Account')
    form_continue = HiddenField('form_continue')
    accounts = FieldList(FormField(AccountForm, label=''))

    def set_accounts(self, data_source, cur_proc):
        account_dict = []
        proc_acts = data_source.query.filter_by(processor_id=cur_proc.id).all()
        for act in proc_acts:
            if act.key is not None:
                form_dict = act.get_form_dict()
                account_dict.append(form_dict)
        self.accounts = account_dict
        return account_dict


class FeeForm(FlaskForm):
    digital_agency_fees = DecimalField(_('Digital Agency Fees'))
    trad_agency_fees = DecimalField(_('Traditional Agency Fees'))
    rate_card = QuerySelectField(_l('Rate Card'),
                                 query_factory=lambda: RateCard.query.all(),
                                 get_label='name', validators=[DataRequired()])
    refresh_rate_card = SubmitField('View Rate Card')
    dcm_service_fees = SelectField('DCM Service Fee', choices=[
        (x, x) for x in ['0%', '5%', '10%', '15%']])
    form_continue = HiddenField('form_continue')


class ConversionForm(FlaskForm):
    conversion_name = StringField('Conversion Name')
    conversion_type = SelectField('Conversion Type',
                                  choices=[(x, x) for x in vmc.datafloatcol])
    key = SelectField(
        'Conversion Platform', choices=[(x, x) for x in vmc.api_keys])
    dcm_category = StringField('DCM Group Name')
    delete = SubmitField('Delete', render_kw={'style': 'background-color:red'})


class GeneralConversionForm(FlaskForm):
    add_child = SubmitField(label='Add Conversion')
    refresh_edit_conversions = SubmitField('Edit As Spreadsheet')
    form_continue = HiddenField('form_continue')
    conversions = FieldList(FormField(ConversionForm, label=''))

    def set_conversions(self, data_source, cur_proc):
        conv_dict = []
        proc_convs = data_source.query.filter_by(processor_id=cur_proc.id).all()
        for conv in proc_convs:
            if conv.conversion_name is not None:
                form_dict = conv.get_form_dict()
                conv_dict.append(form_dict)
        self.conversions = conv_dict
        return conv_dict


class ProcessorRequestFinishForm(FlaskForm):
    owner = QuerySelectField(_l('Owner'), allow_blank=True,
                             query_factory=lambda: User.query.all(),
                             get_label='username', validators=[DataRequired()])
    followers = SelectMultipleField(_l('Followers'))
    form_continue = HiddenField('form_continue')

    def set_user_choices(self):
        choices = [('', '')]
        choices.extend([(x.username, x.username) for x in
                        User.query.all()])
        self.followers.choices = choices
        return True

    def add_current_users(self, db_model, cur_proc):
        usr_dict = []
        proc_users = db_model.query.get(cur_proc.id).processor_followers
        for usr in proc_users:
            if usr.username is not None:
                usr_dict.append(usr.username)
                if cur_proc.user_id == usr.id:
                    self.owner.data = usr
        self.followers.data = usr_dict


class ProcessorFixForm(FlaskForm):
    fix_type = SelectField('Fix Type', choices=[
        ('Change Dimension', 'Change Dimension'),
        ('Change Metric', 'Change Metric'),
        ('Missing Metric', 'Missing Metric'),
        ('New File', 'New File'),
        ('Upload File', 'Upload File'),
        ('Update Plan', 'Update Plan'),
        ('Change Tableau', 'Change Tableau'),
        ('Spend Cap', 'Spend Cap'),
        ('Custom', 'Custom')])
    cname = SelectField(
        'Column Name', choices=[('', '')] + [(x, x) for x in dctc.COLS] +
                               [(x, x) for x in vmc.datacol])
    wrong_value = StringField(_l('Wrong Value'))
    correct_value = StringField(_l('Correct Value'))
    filter_column_name = SelectField(
        label='Filter Column Name', choices=[''] + [x for x in dctc.COLS])
    filter_column_value = StringField('Filter Column Value')
    fix_description = TextAreaField(_l('Describe Fix'))
    data_source = SelectField(_l('Processor Data Source'))
    new_file = FileField(_l('New File'))
    form_continue = HiddenField('form_continue')

    def set_vendor_key_choices(self, current_processor_id):
        choices = [('', '')]
        choices.extend([(x.vendor_key, x.vendor_key) for x in
                        ProcessorDatasources.query.filter_by(
                            processor_id=current_processor_id).all()])
        self.data_source.choices = choices


class ProcessorRequestCommentForm(FlaskForm):
    post = TextAreaField(_l('Comment on Fix'))
    form_continue = HiddenField('form_continue')


class ProcessorContinueForm(FlaskForm):
    form_continue = HiddenField('form_continue')


class ProcessorNoteForm(FlaskForm):
    note_text = TextAreaField(_l('Note Text'))
    notification = SelectField(
        choices=[(x, x) for x in ['', 'Daily', 'Weekly', 'Monthly']])
    notification_day = SelectField(
        choices=[(x, x) for x in ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
                                  'Friday', 'Saturday', 'Sunday']])
    vendor = SelectField('Vendor')
    country = SelectField('Country')
    environment = SelectField('Environment')
    kpi = SelectField('KPI')
    start_date = DateField('Start Date')
    end_date = DateField('End Date')
    dimensions = SelectField(
        'Dimensions', choices=[(x, x) for x in [''] + dctc.COLS])
    form_continue = HiddenField('form_continue')

    def set_vendor_key_choices(self, current_processor_id):
        choices = [('', '')]
        choices.extend([(x.vendor_key, x.vendor_key) for x in
                        ProcessorDatasources.query.filter_by(
                            processor_id=current_processor_id).all()])
        self.data_source.choices = choices


class UploaderForm(FlaskForm):
    name = StringField(_l('Name'), validators=[
        DataRequired()])
    description = StringField(_l('Description'), validators=[
        DataRequired()])
    fb_account_id = StringField(_l('Facebook Account ID'))
    aw_account_id = StringField(_l('Adwords Account ID'))
    dcm_account_id = StringField(_l('DCM Account ID'))
    cur_client = QuerySelectField(_l('Client'), allow_blank=True,
                                  query_factory=lambda: Client.query.all(),
                                  get_label='name')
    new_client = StringField(_l('Add New Client'))
    cur_product = QuerySelectField(_l('Product'), allow_blank=True,
                                   query_factory=lambda: Product.query.all(),
                                   get_label='name')
    new_product = StringField(_l('Add New Product'))
    cur_campaign = QuerySelectField(_l('Campaign'), allow_blank=True,
                                    query_factory=lambda: Campaign.query.all(),
                                    get_label='name')
    new_campaign = StringField(_l('Add New Campaign'))
    media_plan = FileField('Media Plan')
    client_name = None
    product_name = None
    campaign_name = None
    form_continue = HiddenField('form_continue')

    def validate_name(self, name):
        processor = Processor.query.filter_by(name=name.data).first()
        if processor is not None:
            raise ValidationError(_l('Please use a different name.'))

    def validate_new_client(self, new_client):
        if new_client.data:
            self.client_name = new_client.data
            new_client = Client.query.filter_by(name=new_client.data).first()
            if new_client is not None:
                raise ValidationError(_l('Client already exists, '
                                         'select from dropdown.'))
        else:
            self.client_name = self.cur_client.data.name

    def validate_new_product(self, new_product):
        if new_product.data:
            self.product_name = new_product.data
            new_product = Product.query.filter_by(
                name=self.product_name).first()
            if new_product is not None:
                raise ValidationError(_l('Product already exists, '
                                         'select from dropdown.'))
        else:
            self.product_name = self.cur_product.data.name

    def validate_new_campaign(self, new_campaign):
        if new_campaign.data:
            self.campaign_name = new_campaign.data
            new_campaign = Campaign.query.filter_by(
                name=self.campaign_name).first()
            if new_campaign is not None:
                raise ValidationError(_l('Campaign already exists,'
                                         ' select from dropdown.'))
        else:
            self.campaign_name = self.cur_campaign.data.name


class EditUploaderForm(UploaderForm):
    def __init__(self, original_name, *args, **kwargs):
        super(EditUploaderForm, self).__init__(*args, **kwargs)
        self.original_name = original_name

    def validate_name(self, name):
        if name.data != self.original_name:
            uploader = Uploader.query.filter_by(name=self.name.data).first()
            if uploader is not None:
                raise ValidationError(_l('Please use a different name.'))


class RelationForm(FlaskForm):
    impacted_column_name = StringField(
        'Column Name', render_kw={'readonly': True})
    unresolved_relations = StringField(
        'Unresolved Relations', render_kw={'readonly': True})
    relation_constant = StringField(
        'Relation Constant', description='Only populate if you want a '
                                         'value to appear for all line items.')
    position = SelectMultipleField(
        'Position in Name', choices=[('', '')] + [(x, x) for x in range(25)],
        description='Ex. in the name Example_Name, Example is position 0, '
                    'Name is position 1 etc.')
    refresh_edit_relation = SubmitField('Edit as Spreadsheet')


class EditUploaderMediaPlanForm(FlaskForm):
    name_create_type = SelectField(
        _l('Name Creation Type'),
        choices=[(x, x) for x in ['Media Plan', 'File', 'Match Table']])
    media_plan_column_choices = [
        (x, x) for x in [
            cre.MediaPlan.campaign_id, cre.MediaPlan.campaign_name,
            cre.MediaPlan.placement_phase, cre.MediaPlan.campaign_phase,
            cre.MediaPlan.partner_name, cre.MediaPlan.country_name,
            cre.MediaPlan.placement_name,
            PartnerPlacements.targeting_bucket.name,
            PartnerPlacements.environment.name]]
    media_plan_columns = SelectMultipleField(
        _l('Media Plan Columns'), choices=media_plan_column_choices,
        default=[(x, x) for x in
                 [cre.MediaPlan.partner_name, cre.MediaPlan.campaign_id]])
    partner_name_filter = TextAreaField(
        _l('Partner Name Filter'), default='Facebook|Instagram')
    refresh_uploader_current_name = SubmitField('View Current Names')
    refresh_uploader_full_relation = SubmitField('View Full Relation File')
    form_continue = HiddenField('form_continue')
    relations = FieldList(FormField(RelationForm, label=''))

    @staticmethod
    def set_relations(data_source, cur_upo):
        relation_dict = []
        up_rel = data_source.query.filter_by(
            uploader_objects_id=cur_upo.id).all()
        for rel in up_rel:
            form_dict = rel.get_form_dict()
            relation_dict.append(form_dict)
        return relation_dict


class EditUploaderNameCreateForm(FlaskForm):
    name_create_type = SelectField(
        _l('Name Creation Type'),
        choices=[(x, x) for x in ['Media Plan', 'File', 'Match Table']])
    name_creator = FileField(_l('Name Creator File'))
    refresh_uploader_current_name = SubmitField('View Current Names')
    duplication_type = SelectField(
        _l('Duplication Type'),
        choices=[(x, x) for x in ['None', 'All', 'Custom']])
    upload_filter = FileField(_l('Duplication Filter File'))
    refresh_uploader_full_relation = SubmitField('View Full Relation File')
    form_continue = HiddenField('form_continue')
    relations = FieldList(FormField(RelationForm, label=''))


class EditUploaderCreativeForm(FlaskForm):
    refresh_uploader_creative_files = SubmitField('View Creative Files')
    creative_file = FileField(_l('Creative File'))
    form_continue = HiddenField('form_continue')


class UploaderDuplicateForm(FlaskForm):
    new_name = StringField(_('New Name'), validators=[DataRequired()])
    form_continue = HiddenField('form_continue')


class ProcessorDuplicateForm(FlaskForm):
    new_name = StringField(_('New Name'), validators=[DataRequired()])
    new_start_date = DateField('New Start Date', format='%Y-%m-%d',
                               validators=[DataRequired()])
    new_end_date = DateField('New End Date', format='%Y-%m-%d',
                             validators=[DataRequired()])
    old_processor_run = BooleanField('Run Old Processor?')
    form_continue = HiddenField('form_continue')


class StaticFilterForm(FlaskForm):
    filter_col = SelectField(
        'Dimensions', choices=[(x, x) for x in exp.ScriptBuilder().dimensions])
    filter_val = SelectMultipleField(
        'Dimension Values', choices=[(x, x) for x in ['All Values']])
    filter_delete = SubmitField(
        'Delete', render_kw={'style': 'background-color:red'})


class ProcessorDashboardForm(FlaskForm):
    name = StringField(_('Name'))
    chart_type = SelectField(
        'Chart Type',
        choices=[(x, x) for x in ['Bar', 'Area', 'Line', 'Lollipop']])
    dimensions = SelectField(
        'Dimensions', choices=[(x, x) for x in exp.ScriptBuilder().dimensions])
    metrics = SelectMultipleField(
        'Metrics', choices=[(x, x) for x in exp.ScriptBuilder().metrics])
    add_child = SubmitField(label='Add Static Filter')
    form_continue = HiddenField('form_continue')
    static_filters = FieldList(FormField(StaticFilterForm, label=''))

    def set_filters(self, data_source, cur_upo):
        relation_dict = []
        up_rel = data_source.query.filter_by(
            dashboard_id=cur_upo.id).all()
        for rel in up_rel:
            form_dict = rel.get_form_dict()
            relation_dict.append(form_dict)
        self.static_filters = relation_dict
        return relation_dict


class ProcessorCleanDashboardForm(FlaskForm):
    add_raw_file = FileField(_l('Add New Raw File'))
    select_plot = SelectField(
        'Select Plot', choices=[('', ''), ('Date', 'Date')] +
                               [(x, x) for x in dctc.COLS])


class ProcessorMetricForm(FlaskForm):
    metric_name = SelectField('Metric Name',
                              choices=[(x, x) for x in vmc.datafloatcol +
                                       [vmc.datecol]])
    metric_values = SelectMultipleField('Metric Values', choices=[('', '')])
    delete_metric = SubmitField(
        'Delete', render_kw={'style': 'background-color:red'})


class ProcessorMetricsForm(FlaskForm):
    add_sub_form_metric = SubmitField(label='Add Static Filter')
    proc_metrics = FieldList(FormField(ProcessorMetricForm, label=''))


class ProcessorDeleteForm(FlaskForm):
    processor_name = StringField(
        'Processor Name', render_kw={'readonly': True},
        description='WARNING - Clicking this will delete this processor. \n'
                    'This can not be undone.')
    delete_metric = SubmitField(
        'Delete', render_kw={'style': 'background-color:red'})


class ProcessorDuplicateAnotherForm(FlaskForm):
    old_proc = QuerySelectField(_l('Processor To Duplicate'),
                                query_factory=lambda: Processor.query.all(),
                                get_label='name')
    new_name = StringField(_('New Name'), validators=[DataRequired()])
    new_start_date = DateField('New Start Date', format='%Y-%m-%d',
                               validators=[DataRequired()])
    new_end_date = DateField('New End Date', format='%Y-%m-%d',
                             validators=[DataRequired()])
    new_proc = HiddenField('New Proc')
    form_continue = HiddenField('form_continue')


class ProcessorAutoAnalysisForm(FlaskForm):
    auto_analysis_select = SelectField(
        _l('Analysis'),
        choices=[(x, x) for x in ['Topline', 'Delivery', 'KPI', 'QA', 'All']])
    datasources = FieldList(FormField(DataSourceForm, label=''))
    form_continue = HiddenField('form_continue')


class ProcessorReportBuilderForm(FlaskForm):
    name = SelectField(_('Report Name'))
    report_date = DateField(_l('Report End Date'), format='%Y-%m-%d')
    datasources = FieldList(FormField(DataSourceForm, label=''))
    form_continue = HiddenField('form_continue')

    def __init__(self, names, default_name, default_date, *args, **kwargs):
        super(ProcessorReportBuilderForm, self).__init__(*args, **kwargs)
        self.name.choices = names
        self.name.default = default_name
        self.report_date.data = default_date


class WalkthroughUploadForm(FlaskForm):
    new_file = FileField(_l('New File'))


class UploadTestForm(FlaskForm):
    new_file = FileField(_l('New File'))
    submit_form = SubmitField(_l('SUBMIT BUTTON'))


class ScreenshotForm(FlaskForm):
    cur_client = SelectField(_l('Client'))
    cur_product = SelectField(_l('Product'))
    cur_campaign = SelectField(_l('Campaign'))
    placement = SelectField(_l('Placement'))
    creative = SelectField(_l('Creative'))

    def set_choices(self):
        for obj in [(Client, self.cur_client),
                    (Product, self.cur_product), (Campaign, self.cur_campaign)]:
            choices = [('', '')]
            choices.extend(set([(x.name, x.name) for x in obj[0].query.all()]))
            obj[1].choices = choices
