import os
import json
import shutil
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
old_path = '/home/ubuntu/lqapp/processor/config'
processors = Processor.query.all()
for cur_proc in processors:
    print('{}'.format(cur_proc.name))
    try:
        os.chdir(adjust_path(cur_proc.local_path))
    except:
        print('COULD NOT GET TO PATH GOING NEXT')
        continue
    for fn in file_names:
        try:
            shutil.copy(os.path.join(old_path, fn), os.path.join('config', fn))
        except PermissionError as e:
            print('permission error {} {}'.format(fn, e))
        except OSError as e:
            print('os error {} {}'.format(fn, e))
    print('running updaate...{}'.format(cur_proc.name))
    main('--update all --noprocess')
    print('run complete. {}'.format(cur_proc.name))
