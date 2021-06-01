import os
import json
import shutil
import pandas as pd
from app import create_app, db
from app.models import ProcessorDatasources, Processor




def adjust_path(path):
    for x in [['S:', '/mnt/s'], ['C:', '/mnt/c'], ['c:', '/mnt/c'],
              ['\\', '/']]:
        path = path.replace(x[0], x[1])
    return path


app = create_app()
app.app_context().push()

from processor.main import main
file_names = ['db_fields.csv', 'dbschema.csv', 'db_df_translation.csv']
old_path = '/home/ubuntu/lqapp/'
processors = Processor.query.all()
df = pd.DataFrame()
for cur_proc in processors:
    print('{}'.format(cur_proc.name))
    try:
        os.chdir(adjust_path(cur_proc.local_path))
    except:
        print('COULD NOT GET TO PATH GOING NEXT')
        continue
    try:
        ul_df = pd.read_csv('config/upload_id_file.csv')
    except:
        print('COULD NOT READ FILE GOING NEXT')
        continue
    upload_id = ul_df['uploadid'][0]
    cur_dict = {'processor': [cur_proc.name], 'upload_id': [upload_id]}
    tdf = pd.DataFrame(cur_dict)
    df = df.append(tdf)
df.to_csv(old_path + 'app_processor.csv')
