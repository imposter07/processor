from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, TextAreaField, SelectField
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
    description = TextAreaField(_l('Description'), validators=[
        DataRequired()])
    local_path = TextAreaField(_l('Local Path'), validators=[DataRequired()])
    tableau_workbook = StringField(_l('Tableau Workbook'))
    tableau_view = StringField(_l('Tableau View'))
    client_id = QuerySelectField(_l('Client'), allow_blank=True,
                                 query_factory=lambda: Client.query.all(),
                                 get_label='name')
    new_client = StringField(_l('Add New Client'))
    product_id = QuerySelectField(_l('Product'), allow_blank=True,
                                  query_factory=lambda: Product.query.all(), get_label='name')
    new_product = StringField(_l('Add New Product'))
    campaign_id = QuerySelectField(_l('Campaign'), allow_blank=True,
                                   query_factory=lambda: Campaign.query.all(), get_label='name')
    new_campaign = StringField(_l('Add New Campaign'))
    submit = SubmitField(_l('Save & Quit'))
    submit_continue = SubmitField(_l('Save & Continue'))

    def validate_name(self, name):
        processor = Processor.query.filter_by(name=name.data).first()
        if processor is not None:
            raise ValidationError(_l('Please use a different name.'))


class EditProcessorForm(FlaskForm):
    name = StringField(_l('Name'), validators=[
        DataRequired()])
    description = TextAreaField(_l('Description'), validators=[
        DataRequired()])
    local_path = TextAreaField(_l('Local Path'), validators=[DataRequired()])
    tableau_workbook = StringField(_l('Tableau Workbook'))
    tableau_view = StringField(_l('Tableau View'))
    client_id = QuerySelectField(_l('Client'), allow_blank=True,
                                 query_factory=lambda: Client.query.all(),
                                 get_label='name')
    new_client = StringField(_l('Add New Client'))
    product_id = QuerySelectField(_l('Product'), allow_blank=True,
                                  query_factory=lambda: Product.query.all(), get_label='name')
    new_product = StringField(_l('Add New Product'))
    campaign_id = QuerySelectField(_l('Campaign'), allow_blank=True,
                                   query_factory=lambda: Campaign.query.all(), get_label='name')
    new_campaign = StringField(_l('Add New Campaign'))
    submit = SubmitField(_l('Save & Quit'))
    submit_continue = SubmitField(_l('Save & Continue'))

    def __init__(self, original_name, *args, **kwargs):
        super(EditProcessorForm, self).__init__(*args, **kwargs)
        self.original_name = original_name
        # if campaign_id:
        #    self.campaign_id.default = campaign_id

    def validate_name(self, name):
        if name.data != self.original_name:
            processor = Processor.query.filter_by(name=self.name.data).first()
            if processor is not None:
                raise ValidationError(_l('Please use a different name.'))
