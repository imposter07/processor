import os
import json
from app import create_app, db
from app.models import ProcessorDatasources



def adjust_path(path):
    for x in [['S:', '/mnt/s'], ['C:', '/mnt/c'], ['c:', '/mnt/c'],
              ['\\', '/']]:
        path = path.replace(x[0], x[1])
    return path


app = create_app()
app.app_context().push()
file_name_start = 'szkconfig'
vendor_key = 'Sizmek'
change_param = 'password'
new_param_val = ''

processor_ds = ProcessorDatasources.query.filter(
    ProcessorDatasources.key == vendor_key)
for ds in processor_ds:
    print('{} - {}'.format(ds.processor.name, ds.key))
    try:
        os.chdir(adjust_path(ds.processor.local_path))
    except:
        print('COULD NOT GO')
        continue
    szk_files = [f for f in os.listdir('config')
                 if f[:len(file_name_start)]==file_name_start]
    for szk_file in szk_files:
        with open(os.path.join('config', szk_file), 'r') as f:
            config_file = json.load(f)
        config_file[change_param] = new_param_val
        with open(os.path.join('config', szk_file), 'w') as f:
            json.dump(config_file, f)

