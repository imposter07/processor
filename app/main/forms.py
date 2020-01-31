from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, TextAreaField, SelectField, \
    FormField, FieldList, HiddenField, DateTimeField, FileField, BooleanField, \
    DecimalField
from wtforms.fields.html5 import DateField, TimeField
from wtforms.ext.sqlalchemy.fields import QuerySelectField
from wtforms.validators import ValidationError, DataRequired, Length
from flask_babel import _, lazy_gettext as _l
from app.models import User, Processor, Client, Product, Campaign, Uploader,\
    RateCard
import processor.reporting.dictcolumns as dctc
import processor.reporting.vmcolumns as vmc


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
    name = StringField(_l('Name'), validators=[
        DataRequired()])
    description = StringField(_l('Description'), validators=[
        DataRequired()])
    local_path = StringField(_l('Local Path'), validators=[DataRequired()])
    tableau_workbook = StringField(_l('Tableau Workbook'))
    tableau_view = StringField(_l('Tableau View'))
    start_date = DateField(_l('Start Date'))
    end_date = DateField(_l('End Date'))
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


class APIForm(FlaskForm):
    name = StringField('Name')
    key = SelectField('API Type', choices=[(x, x) for x in vmc.api_keys])
    account_id = StringField('Account ID')
    start_date = DateField('Start Date', format='%Y-%m-%d')
    account_filter = StringField('Filter')
    api_fields = StringField('API Fields')
    raw_file = FileField('Raw File')
    delete = SubmitField('Delete')
    vendor_key = HiddenField('Vendor Key')


class ImportForm(FlaskForm):
    add_child = SubmitField(label='Add API')
    remove_api = SubmitField('Remove Last API')
    refresh_imports = SubmitField('Refresh From Processor')
    form_continue = HiddenField('form_continue')
    apis = FieldList(FormField(APIForm, label='{}'.format(APIForm.name)))

    def set_apis(self, data_source, cur_proc):
        imp_dict = []
        proc_imports = data_source.query.filter_by(
            processor_id=cur_proc.id).all()
        for imp in proc_imports:
            if imp.name is not None:
                form_dict = imp.get_import_form_dict()
                imp_dict.append(form_dict)
        self.apis = imp_dict
        return imp_dict


class EditProcessorForm(ProcessorForm):
    def __init__(self, original_name, *args, **kwargs):
        super(EditProcessorForm, self).__init__(*args, **kwargs)
        self.original_name = original_name

    def validate_name(self, name):
        if name.data != self.original_name:
            processor = Processor.query.filter_by(name=self.name.data).first()
            if processor is not None:
                raise ValidationError(_l('Please use a different name.'))


class DataSourceForm(FlaskForm):
    vendor_key = StringField(_l('Vendor Key'))
    refresh_raw_data = SubmitField(_l('Show Raw Data'))
    full_placement_columns = TextAreaField(_l('Full Placement Columns'))
    placement_columns = StringField(_l('Placement Column'))
    auto_dictionary_placement = SelectField(_l('Auto Dict Placement'), choices=[
        (x, x) for x in [dctc.FPN, dctc.PN]])
    refresh_dictionary_order = SubmitField(_l('Show Dictionary Order'))
    auto_dictionary_order = TextAreaField(_l('Auto Dictionary Order'))
    refresh_delete_dict = SubmitField(_l('Delete Dictionary'))
    refresh_dictionary = SubmitField(_l('Show Dictionary'))
    active_metrics = TextAreaField(_l('Active Metrics'))
    vm_rules = TextAreaField(_l('Vendor Matrix Rules'))
    original_vendor_key = HiddenField('Original Vendor Key')

    def __init__(self, *args, **kwargs):
        super(DataSourceForm, self).__init__(*args, **kwargs)
        self.original_vendor_key = self.vendor_key


class ProcessorCleanForm(FlaskForm):
    refresh_data_sources = SubmitField(_l('Refresh From Processor'))
    form_continue = HiddenField('form_continue')
    datasources = FieldList(FormField(DataSourceForm, label=''))

    def set_datasources(self, data_source, cur_proc):
        imp_dict = []
        ds = data_source.query.filter_by(processor_id=cur_proc.id).all()
        for source in ds:
            form_dict = source.get_ds_form_dict()
            imp_dict.append(form_dict)
        self.datasources = imp_dict
        return imp_dict


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
    column_name = SelectField('Column Names')


class FullPlacementForm(FlaskForm):
    add_child = SubmitField(label='Add Column')
    remove_api = SubmitField('Remove Column')
    placements = FieldList(FormField(PlacementForm))


class ProcessorExportForm(FlaskForm):
    tableau_workbook = StringField(_l('Tableau Workbook'))
    tableau_view = StringField(_l('Tableau View'))
    schedule = BooleanField(_l('Schedule'), )
    schedule_start = DateField(_l('Schedule Start'), format='%Y-%m-%d')
    schedule_end = DateField(_l('Schedule End'), format='%Y-%m-%d')
    run_time = TimeField(_l('Run Time'))
    interval = SelectField(_l('Hourly Interval'),
                           choices=[(x, x) for x in range(1, 25)], default='24')
    form_continue = HiddenField('form_continue')


class ProcessorRequestForm(FlaskForm):
    name = StringField(_l('Name'), validators=[
        DataRequired()])
    description = StringField(_l('Description'), validators=[
        DataRequired()])
    plan_path = StringField(_l('Media Plan Path'), validators=[DataRequired()])
    media_plan = FileField('Media Plan')
    start_date = DateField(_l('Start Date'))
    end_date = DateField(_l('End Date'))
    first_report = DateField(_l('First Report Date'))
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


class EditProcessorRequestForm(ProcessorRequestForm):
    def __init__(self, original_name, *args, **kwargs):
        super(EditProcessorRequestForm, self).__init__(*args, **kwargs)
        self.original_name = original_name

    def validate_name(self, name):
        if name.data != self.original_name:
            processor = Processor.query.filter_by(name=self.name.data).first()
            if processor is not None:
                raise ValidationError(_l('Please use a different name.'))


class AccountForm(FlaskForm):
    key = SelectField(
        'Account Type', choices=[(x, x) for x in vmc.api_keys])
    account_id = StringField('Account ID')
    username = StringField('Username',
                           description='Only include if shared login.')
    password = StringField('Password',
                           description='Only include if shared login.')
    campaign_id = StringField('Campaign ID or Name')
    delete = SubmitField('Delete')


class GeneralAccountForm(FlaskForm):
    add_child = SubmitField(label='Add Account')
    remove_account = SubmitField('Remove Last Account')
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
    rate_card = QuerySelectField(_l('Rate Card'), allow_blank=True,
                                 query_factory=lambda: RateCard.query.all(),
                                 get_label='name')
    refresh_rate_card = SubmitField('View Rate Card')
    dcm_service_fees = SelectField('DCM Service Fee', choices=[
        ('0%', '0%'), ('10%', '10%'), ('15%', '15%')])
    form_continue = HiddenField('form_continue')


class ConversionForm(FlaskForm):
    conversion_name = StringField('Conversion Name')
    conversion_type = SelectField('Conversion Type',
                                  choices=[(x, x) for x in vmc.datafloatcol])
    key = SelectField(
        'Conversion Platform', choices=[(x, x) for x in vmc.api_keys])
    dcm_category = StringField('DCM Category')
    delete = SubmitField('Delete')


class GeneralConversionForm(FlaskForm):
    add_child = SubmitField(label='Add Conversion')
    remove_conversion = SubmitField('Remove Last Conversion')
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


class AssignUserForm(FlaskForm):
    assigned_user = QuerySelectField(_l('User'), allow_blank=True,
                                     query_factory=lambda: User.query.all(),
                                     get_label='username')
    user_level = SelectField(
        'User Level', choices=[(x, x) for x in ['Follower', 'Owner']])
    delete = SubmitField('Delete')


class ProcessorRequestFinishForm(FlaskForm):
    add_child = SubmitField(label='Add User')
    remove_user = SubmitField('Remove Last User')
    form_continue = HiddenField('form_continue')
    assigned_users = FieldList(FormField(AssignUserForm, label=''))

    def set_users(self, data_source, cur_proc):
        usr_dict = []
        proc_users = data_source.query.get(cur_proc.id).processor_followers
        for usr in proc_users:
            if usr.username is not None:
                form_dict = {'assigned_user': usr,
                             'user_level': 'Follower'}
                if cur_proc.user_id == usr.id:
                    form_dict['user_level'] = 'Owner'
                usr_dict.append(form_dict)
        self.assigned_users = usr_dict
        return usr_dict


class ProcessorFixDimensionForm(FlaskForm):
    column_name = SelectField('Column Name',
                              choices=[(x, x) for x in dctc.COLS])
    old_value = StringField(_l('Wrong Value'))
    new_value = StringField(_l('Correct Value'))
    delete = SubmitField('Delete')


class ProcessorRequestFixForm(FlaskForm):
    add_child_fix = SubmitField(label='Add Fix')
    remove_fix = SubmitField('Remove Last Fix')
    form_continue = HiddenField('form_continue')
    key = SelectField('Fix Type', choices=[
        ('Change Dimension', 'Change Dimension'),
        ('Change Metric', 'Change Metric'),
        ('Custom', 'Custom')])
    current_fixes = FieldList(FormField(ProcessorFixDimensionForm, label=''))


class UploaderForm(FlaskForm):
    name = StringField(_l('Name'), validators=[
        DataRequired()])
    description = StringField(_l('Description'), validators=[
        DataRequired()])
    local_path = StringField(_l('Local Path'), validators=[DataRequired()])
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
