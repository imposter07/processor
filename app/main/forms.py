from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, TextAreaField, SelectField, \
    FormField, FieldList
from wtforms.fields.html5 import DateField
from wtforms.ext.sqlalchemy.fields import QuerySelectField
from wtforms.validators import ValidationError, DataRequired, Length
from flask_babel import _, lazy_gettext as _l
from app.models import User, Processor, Client, Product, Campaign


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
    submit = SubmitField(_l('Save & Quit'))
    submit_continue = SubmitField(_l('Save & Continue'))

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
    api_name = SelectField('API Type', choices=[('Facebook', 'Facebook'),
                                                ('Adwords', 'Adwords')])
    account_id = StringField('Account ID')
    start_date = DateField('Start Date', format='%Y-%m-%d')


class ImportForm(FlaskForm):
    apis = FieldList(FormField(APIForm))
    add_child = SubmitField(label='Add API')
    remove_api = SubmitField('Remove Last API')
    submit = SubmitField(_l('Save & Quit'))
    submit_continue = SubmitField(_l('Save & Continue'))


class EditProcessorForm(ProcessorForm):
    def __init__(self, original_name, *args, **kwargs):
        super(EditProcessorForm, self).__init__(*args, **kwargs)
        self.original_name = original_name

    def validate_name(self, name):
        if name.data != self.original_name:
            processor = Processor.query.filter_by(name=self.name.data).first()
            if processor is not None:
                raise ValidationError(_l('Please use a different name.'))
