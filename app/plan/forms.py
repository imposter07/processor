from flask_wtf import FlaskForm
from wtforms import SelectField, HiddenField, StringField, \
    DateField, TextAreaField, DecimalField, FloatField, SelectMultipleField, \
    IntegerField

from wtforms.validators import ValidationError, DataRequired, Regexp
from flask_babel import lazy_gettext as _l
from app.models import Plan, Client, Product, Campaign


class PlanForm(FlaskForm):
    name = StringField(_l('Name'), validators=[
        DataRequired(), Regexp("[^']", message='Remove special characters')])
    description = StringField(_l('Description'), validators=[
        DataRequired()])
    client_requests = TextAreaField(_l('Client Requests'))
    restrictions = TextAreaField(_l('Restrictions'))
    objective = TextAreaField(_l('Objective'))
    start_date = DateField(_l('Start Date'))
    end_date = DateField(_l('End Date'))
    total_budget = DecimalField(_l('Total Budget'))
    cur_client = SelectField(_l('Client'))
    cur_product = SelectField(_l('Product'))
    cur_campaign = SelectField(_l('Campaign'))
    client_name = None
    product_name = None
    campaign_name = None
    form_continue = HiddenField('form_continue')

    def validate_name(self, name):
        processor = Plan.query.filter_by(name=name.data).first()
        if processor is not None:
            raise ValidationError(_l('Please use a different name.'))

    def set_choices(self):
        for obj in [(Client, self.cur_client),
                    (Product, self.cur_product), (Campaign, self.cur_campaign)]:
            choices = [('', '')]
            choices.extend(set([(x.name, x.name) for x in obj[0].query.all()]))
            obj[1].choices = choices


class EditPlanForm(PlanForm):
    def __init__(self, original_name, *args, **kwargs):
        super(EditPlanForm, self).__init__(*args, **kwargs)
        self.original_name = original_name

    def validate_name(self, name):
        if name.data != self.original_name:
            processor = Plan.query.filter_by(name=self.name.data).first()
            if processor is not None:
                raise ValidationError(_l('Please use a different name.'))


class PlanToplineForm(FlaskForm):
    form_continue = HiddenField('form_continue')


class CreateSowForm(FlaskForm):
    project_name = StringField('Project', validators=[DataRequired()])
    project_contact = StringField('Advertiser project contact',
                                  validators=[DataRequired()])
    date_submitted = DateField('Date submitted', validators=[DataRequired()])
    liquid_contact = StringField('Liquid project contact',
                                 validators=[DataRequired()])
    liquid_project = IntegerField('Liquid project number',
                                  validators=[DataRequired()])
    start_date = DateField('Start date', validators=[DataRequired()])
    end_date = DateField('End date', validators=[DataRequired()])
    client_name = StringField('Client name', validators=[DataRequired()])
    campaign = StringField('Campaign', validators=[DataRequired()])
    address = StringField('Liquid address', validators=[DataRequired()])
    phone = StringField('Liquid phone', validators=[DataRequired()])
    fax = StringField('Liquid fax', validators=[DataRequired()])
    total_project_budget = DecimalField('Total budget',
                                        validators=[DataRequired()])
    ad_serving = DecimalField('Ad serving cost', validators=[DataRequired()])
    vendor = SelectMultipleField('Vendors', choices=['Facebook', 'Samsung',
                                                     'The Trade Desk', 'TikTok',
                                                     'DCM'])
    form_continue = HiddenField('form_continue')
