from flask_wtf import FlaskForm
from wtforms import SelectField, FileField, HiddenField
from flask_babel import lazy_gettext as _l


class TutorialUploadForm(FlaskForm):
    tutorial_select = SelectField(
        _l('Tutorial Select'), choices=[(x, x) for x in ['Processor Basic']])
    new_file = FileField(_l('New File'))


class TutorialContinueForm(FlaskForm):
    form_continue = HiddenField('form_continue')