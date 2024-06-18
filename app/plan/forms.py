import json
from flask_wtf import FlaskForm
from wtforms import SelectField, HiddenField, StringField, \
    DateField, TextAreaField, DecimalField, FloatField, SelectMultipleField, \
    IntegerField, FileField, FieldList, FormField, SubmitField, MonthField
from wtforms.validators import ValidationError, DataRequired, Regexp
from flask_babel import lazy_gettext as _l
from app.models import Plan, Client, Product, Campaign, Processor, \
    Brandtracker, ProcessorAnalysis
import processor.reporting.analyze as az
import processor.reporting.vmcolumns as vmc


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

    def set_form(self, plan):
        self.name.data = plan.name
        self.description.data = plan.description
        self.client_requests.data = plan.client_requests
        self.restrictions.data = plan.restrictions
        self.objective.data = plan.objective
        self.start_date.data = plan.start_date
        self.end_date.data = plan.end_date
        self.total_budget.data = plan.total_budget
        self_campaign = Campaign.query.filter_by(
            id=plan.campaign_id).first_or_404()
        self_product = Product.query.filter_by(
            id=self_campaign.product_id).first_or_404()
        self_client = Client.query.filter_by(
            id=self_product.client_id).first_or_404()
        self.cur_campaign.data = self_campaign.name
        self.cur_product.data = self_product.name
        self.cur_client.data = self_client.name


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
    address = StringField('Liquid address', validators=[DataRequired()])
    phone = StringField('Liquid phone', validators=[DataRequired()])
    fax = StringField('Liquid fax', validators=[DataRequired()])
    ad_serving = DecimalField('Ad serving cost', validators=[DataRequired()])
    form_continue = HiddenField('form_continue')


class CreateIoForm(FlaskForm):
    insertion_order = StringField('Insertion_order',
                                  validators=[DataRequired()])
    project_number = IntegerField('Project number', validators=[DataRequired()])
    document_date = DateField('Date', validators=[DataRequired()])
    billing_contact = StringField('Billing contact',
                                  validators=[DataRequired()])
    attn = StringField('Attn', validators=[DataRequired()])
    media_representative = StringField('Media representative',
                                       validators=[DataRequired()])
    publisher_contact = StringField('Publisher contact',
                                    validators=[DataRequired()])
    publisher_contact_email = StringField('Publisher contact email',
                                          validators=[DataRequired()])
    client = StringField('Client', validators=[DataRequired()])
    agency_contact = StringField('Agency contact', validators=[DataRequired()])
    agency_contact_email = StringField('Agency contact email',
                                       validators=[DataRequired()])
    campaign_name = StringField('String field')


class IoDashboardForm(FlaskForm):
    io_partner_key = SelectField(_l('Partners'))
    form_continue = HiddenField('form_continue')
    partners = FormField(CreateIoForm, label='')

    def set_partner_choices(self, plan_id):
        plan = Plan.query.filter_by(
                            id=plan_id).first()
        choices = [(x.name, x.name) for x in plan.get_current_partners()]
        self.io_partner_key.choices = choices


class RfpForm(FlaskForm):
    form_continue = HiddenField('form_continue')
    add_rfp_file = FileField(_l('Add New RFP'))


class PartnerPlacementForm(FlaskForm):
    form_continue = HiddenField('form_continue')
    add_plan_file = FileField(_l('Replace Current Placements'))


class CompetitiveSpendForm(FlaskForm):
    form_continue = HiddenField('form_continue')


class ImpactScoreForm(FlaskForm):
    form_continue = HiddenField('form_continue')


class CategoryComponentForm(FlaskForm):
    data_column = SelectField(_l('Data Column'), validators=[DataRequired()])
    weight = DecimalField(_l('Weight'), validators=[DataRequired()])
    delete = SubmitField('Delete', render_kw={'style': 'background-color:red',
                                              'type': 'button'})

    def set_column_choices(self, columns=None):
        choices = [('', '')]
        if columns:
            choices.extend([(x, x) for x in set(columns)])
        self.data_column.choices = choices


class PlotCategoryForm(FlaskForm):
    dimension_name = HiddenField('dimension_name')
    add_child = SubmitField(label='Add Data Point',
                            render_kw={'type': 'button'})
    components = FieldList(FormField(CategoryComponentForm, label=''))
    form_continue = HiddenField('form_continue')

    def set_column_choices(self):
        for comp in self.components:
            cols = vmc.brand_cols
            calc_cols = Brandtracker.get_calculated_fields().keys()
            cols.extend(calc_cols)
            comp.set_column_choices(cols)


class BrandtrackerForm(FlaskForm):
    update_data = SubmitField(label='Update Tables and Charts',
                              render_kw={'type': 'button'})
    primary_date = MonthField(_l('Date'), validators=[DataRequired()])
    comparison_date = MonthField(_l('Comparison Date'),
                                 validators=[DataRequired()])
    titles = SelectMultipleField(_l('Titles'))
    categories = SelectField(
        _l('Dimension'), choices=[(x, x) for x in ['', 'Influence',
                                                   'Engagement',
                                                   'Momentum']])
    form_continue = HiddenField('form_continue')

    def set_title_choices(self):
        bt_camps = Campaign.query.filter_by(name='BRANDTRACKER').all()
        choices = [('', '')]
        if bt_camps:
            bt_procs = []
            for c in bt_camps:
                cur_proc = Processor.query.filter_by(campaign_id=c.id).all()
                bt_procs.extend(cur_proc)
            all_titles = []
            for proc in bt_procs:
                proc_data = proc.processor_analysis.filter_by(
                    key=az.Analyze.database_cache,
                    parameter='productname|eventdate').order_by(
                    ProcessorAnalysis.date.desc()).first()
                if not proc_data:
                    continue
                proc_data = json.loads(proc_data.data)
                titles = [v for v in proc_data['productname'].values()]
                all_titles.extend(titles)
            if all_titles:
                choices.extend([(x, x) for x in set(all_titles)])
        self.titles.choices = choices
