from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, TextAreaField, SelectField, \
    FormField, FieldList, HiddenField
from wtforms.fields.html5 import DateField
from wtforms.ext.sqlalchemy.fields import QuerySelectField
from wtforms.validators import ValidationError, DataRequired, Length
from flask_babel import _, lazy_gettext as _l
from app.models import User, Processor, Client, Product, Campaign
import processor.reporting.dictcolumns as dctc


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
    key = SelectField(
        'API Type', choices=[
            ('Facebook', 'Facebook'), ('Adwords', 'Adwords'),
            ('Sizmek', 'Sizmek'), ('Twitter', 'Twitter'), ('TTD', 'TTD'),
            ('Snapchat', 'Snapchat'), ('DCM', 'DCM'), ('DBM', 'DBM'),
            ('Redshell', 'Redshell'), ('Reddit', 'Reddit'),
            ('Netbase', 'Netbase'), ('GA', 'GA'), ('Revcontent', 'Revcontent'),
            ('AppsFlyer', 'AppsFlyer')])
    account_id = StringField('Account ID')
    start_date = DateField('Start Date', format='%Y-%m-%d')
    account_filter = StringField('Filter')
    api_fields = StringField('API Fields')
    refresh_delete = SubmitField('Delete')
    vendor_key = HiddenField('Vendor Key')


class ImportForm(FlaskForm):
    add_child = SubmitField(label='Add API')
    remove_api = SubmitField('Remove Last API')
    refresh_imports = SubmitField('Refresh From Processor')
    form_continue = HiddenField('form_continue')
    apis = FieldList(FormField(APIForm, label='{}'.format(APIForm.name)))

    def set_apis(self, data_source, cur_proc):
        imp_dict = []
        proc_imports = data_source.query.filter_by(processor_id=cur_proc.id).all()
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
    refresh_delete_dict = SubmitField(_l('Delete Dictionary'))
    refresh_dict = SubmitField(_l('Show Dictionary'))
    vendor_key = StringField(_l('Vendor Key'))
    full_placement_columns = TextAreaField(_l('Full Placement Columns'))
    placement_columns = StringField(_l('Placement Column'))
    auto_dictionary_placement = SelectField(_l('Auto Dict Placement'), choices=[
        (x, x) for x in [dctc.FPN, dctc.PN]])
    auto_dictionary_order = TextAreaField(_l('Auto Dictionary Order'))
    active_metrics = TextAreaField(_l('Active Metrics'))
    vm_rules = TextAreaField(_l('Vendor Matrix Rules'))
    original_vendor_key = HiddenField('Original Vendor Key')

    def __init__(self, *args, **kwargs):
        super(DataSourceForm, self).__init__(*args, **kwargs)
        self.original_vendor_key = self.vendor_key


class ProcessorCleanForm(FlaskForm):
    refresh_data_sources = SubmitField(_l('Refresh From Processor'))
    refresh_show_data_tables = SubmitField(_l('Show Data Tables'))
    refresh_edit_translation = SubmitField(_l('Edit Translation Dictionary'))
    form_continue = HiddenField('form_continue')
    datasources = FieldList(FormField(DataSourceForm))

    def set_datasources(self, data_source, cur_proc):
        imp_dict = []
        ds = data_source.query.filter_by(processor_id=cur_proc.id).all()
        for source in ds:
            form_dict = source.get_ds_form_dict()
            imp_dict.append(form_dict)
        self.apis = imp_dict
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
