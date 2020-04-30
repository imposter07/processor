from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, TextAreaField, SelectField, \
    FormField, FieldList, HiddenField, DateTimeField, FileField, BooleanField, \
    DecimalField, SelectMultipleField, MultipleFileField
from wtforms.fields.html5 import DateField, TimeField
from wtforms.ext.sqlalchemy.fields import QuerySelectField
from wtforms.validators import ValidationError, DataRequired, Length
from flask_babel import _, lazy_gettext as _l
from app.models import User, Processor, Client, Product, Campaign, Uploader,\
    RateCard, ProcessorDatasources
import processor.reporting.dictcolumns as dctc
import processor.reporting.vmcolumns as vmc
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
    delete = SubmitField('Delete', render_kw={'style': 'background-color:red'})
    vendor_key = StringField('Vendor Key', render_kw={'readonly': True})


class ImportForm(FlaskForm):
    data_source = SelectField(_l('Processor Data Source Filter'))
    refresh_imports = SubmitField('Refresh From Processor')
    add_child = SubmitField(label='Add API')
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

    def set_vendor_key_choices(self, current_processor_id):
        choices = [('', '')]
        choices.extend([(x.vendor_key, x.vendor_key) for x in
                        ProcessorDatasources.query.filter_by(
                            processor_id=current_processor_id).all()])
        self.data_source.choices = choices


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
    data_source = SelectField(_l('Processor Data Source Filter'))
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

    def set_vendor_key_choices(self, current_processor_id):
        choices = [('', '')]
        choices.extend([(x.vendor_key, x.vendor_key) for x in
                        ProcessorDatasources.query.filter_by(
                            processor_id=current_processor_id).all()])
        self.data_source.choices = choices


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
    rate_card = QuerySelectField(_l('Rate Card'), allow_blank=True,
                                 query_factory=lambda: RateCard.query.all(),
                                 get_label='name', validators=[DataRequired()])
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


class AssignUserForm(FlaskForm):
    assigned_user = QuerySelectField(_l('User'), allow_blank=True,
                                     query_factory=lambda: User.query.all(),
                                     get_label='username')
    user_level = SelectField(
        'User Level', choices=[(x, x) for x in ['Follower', 'Owner']])
    delete = SubmitField('Delete', render_kw={'style': 'background-color:red'})


class ProcessorRequestFinishForm(FlaskForm):
    add_child = SubmitField(label='Add User')
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


class ProcessorFixForm(FlaskForm):
    fix_type = SelectField('Fix Type', choices=[
        ('Change Dimension', 'Change Dimension'),
        ('Change Metric', 'Change Metric'),
        ('Missing Metric', 'Missing Metric'),
        ('New File', 'New File'),
        ('Upload File', 'Upload File'),
        ('Update Plan', 'Update Plan'),
        ('Change Tableau', 'Change Tableau'),
        ('Custom', 'Custom')])
    column_name = SelectField(
        'Column Name',  choices=[('', '')] + [(x, x) for x in dctc.COLS] +
                                [(x, x) for x in vmc.datacol])
    wrong_value = StringField(_l('Wrong Value'))
    correct_value = StringField(_l('Correct Value'))
    filter_column_name = SelectField(
        'Filter Column Name', choices=[('', '')] + [(x, x) for x in dctc.COLS])
    filter_column_value = StringField('Filter Column Value')
    fix_description = TextAreaField(_l('Describe Fix'))
    data_source = SelectField(_l('Processor Data Source'))
    new_file = FileField(_l('New File'))
    form_continue = HiddenField('form_continue')
    # delete = SubmitField(_l('Delete'))

    def set_vendor_key_choices(self, current_processor_id):
        choices = [('', '')]
        choices.extend([(x.vendor_key, x.vendor_key) for x in
                        ProcessorDatasources.query.filter_by(
                            processor_id=current_processor_id).all()])
        self.data_source.choices = choices


class ProcessorRequestCommentForm(FlaskForm):
    post = TextAreaField(_l('Comment on Fix'))
    form_continue = HiddenField('form_continue')


class ProcessorRequestFixForm(FlaskForm):
    # post = TextAreaField(_l('Comment on Fix'))
    form_continue = HiddenField('form_continue')

    def set_fixes(self, data_source, cur_proc):
        fix_dict = []
        proc_fixes = data_source.query.filter_by(processor_id=cur_proc.id).all()
        for fix in proc_fixes:
            if fix.fix_type is not None:
                form_dict = fix.get_form_dict()
                fix_dict.append(form_dict)
        self.fixes = fix_dict
        return fix_dict


class UploaderForm(FlaskForm):
    name = StringField(_l('Name'), validators=[
        DataRequired()])
    description = StringField(_l('Description'), validators=[
        DataRequired()])
    account_id = StringField(_l('Account ID'))
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
        choices=[(x, x) for x in ['Media Plan', 'File']])
    media_plan_column_choices = [
        (x, x) for x in [cre.MediaPlan.campaign_id,
                         cre.MediaPlan.campaign_name,
                         cre.MediaPlan.placement_phase,
                         cre.MediaPlan.campaign_phase,
                         cre.MediaPlan.partner_name,
                         cre.MediaPlan.country_name,
                         cre.MediaPlan.placement_name]]
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
        choices=[(x, x) for x in ['Media Plan', 'File']])
    create_file = FileField(_l('Create File'))
    refresh_uploader_current_name = SubmitField('View Current Names')
    refresh_uploader_full_relation = SubmitField('View Full Relation File')
    form_continue = HiddenField('form_continue')
    relations = FieldList(FormField(RelationForm, label=''))

"""
class EditUploaderAdsetMediaPlanForm(FlaskForm):
    name_create_type = SelectField(
        _l('Name Creation Type'),
        choices=[(x, x) for x in ['Media Plan', 'File']])
    media_plan_column_choices = [
        (x, x) for x in [cre.MediaPlan.campaign_id,
                         cre.MediaPlan.campaign_name,
                         cre.MediaPlan.placement_phase,
                         cre.MediaPlan.campaign_phase,
                         cre.MediaPlan.partner_name,
                         cre.MediaPlan.country_name,
                         cre.MediaPlan.placement_name]]
    media_plan_columns = SelectMultipleField(
        _l('Media Plan Columns'), choices=media_plan_column_choices,
        default=[(x, x) for x in [cre.MediaPlan.placement_name]])
    partner_name_filter = TextAreaField(
        _l('Partner Name Filter'), default='Facebook|Instagram')
    refresh_uploader_adset_name = SubmitField('View Current Adset Names')
    refresh_full_adset_relation = SubmitField('View Full Relation File')
    form_continue = HiddenField('form_continue')
    relations = FieldList(FormField(RelationForm, label=''))


class EditUploaderAdsetCreateForm(FlaskForm):
    name_create_type = SelectField(
        _l('Name Creation Type'),
        choices=[(x, x) for x in ['Media Plan', 'File']])
    create_file = FileField(_l('Create File'))
    refresh_uploader_campaign_name = SubmitField('View Current Adset Names')
    refresh_full_campaign_relation = SubmitField('View Full Relation File')
    form_continue = HiddenField('form_continue')
    relations = FieldList(FormField(RelationForm, label=''))
"""

class EditUploaderCreativeForm(FlaskForm):
    refresh_uploader_creative_files = SubmitField('View Creative Files')
    creative_file = FileField(_l('Creative File'))
    form_continue = HiddenField('form_continue')


class ProcessorDuplicateForm(FlaskForm):
    new_name = StringField(_('New Name'), validators=[DataRequired()])
    new_start_date = DateField('New Start Date', format='%Y-%m-%d',
                               validators=[DataRequired()])
    new_end_date = DateField('New End Date', format='%Y-%m-%d',
                             validators=[DataRequired()])
    form_continue = HiddenField('form_continue')
