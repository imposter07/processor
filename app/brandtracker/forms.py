import json
from flask_wtf import FlaskForm
from wtforms import SelectField, HiddenField, DecimalField, \
    SelectMultipleField, FieldList, FormField, SubmitField, MonthField
from wtforms.validators import DataRequired
from flask_babel import lazy_gettext as _l
from app.models import Campaign, Processor, Brandtracker, ProcessorAnalysis
import processor.reporting.vmcolumns as vmc
import processor.reporting.analyze as az


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
        campaign = Campaign.query.filter_by(name='BRANDTRACKER').first()
        bt_procs = Processor.query.filter_by(campaign_id=campaign.id).all()
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
        choices = [('', '')]
        if all_titles:
            choices.extend([(x, x) for x in set(all_titles)])
        self.titles.choices = choices
