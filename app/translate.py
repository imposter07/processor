import boto3
from flask import current_app
from flask_babel import _


def translate(text, source_language, dest_language):
    if 'AWS_ACCESS_KEY_ID' not in current_app.config or \
            not current_app.config['AWS_ACCESS_KEY_ID']:
        return _('Error: the translation service is not configured.')
    client = boto3.client(service_name='translate',
                          region_name=current_app.config['AWS_REGION_NAME'],
                          use_ssl=True,
                          aws_access_key_id=
                          current_app.config['AWS_ACCESS_KEY_ID'],
                          aws_secret_access_key=
                          current_app.config['AWS_SECRET_ACCESS_KEY'])
    result = client.translate_text(Text=text,
                                   SourceLanguageCode=source_language,
                                   TargetLanguageCode=dest_language)
    translation = result.get('TranslatedText')
    return translation
