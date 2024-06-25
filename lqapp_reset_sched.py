import os
import sys
import json
import time
import shutil
import datetime
import pandas as pd
from flask import render_template
from rq import get_current_job
from app import create_app, db
from app.email import send_email
from app.models import User, Post, Task, Processor, Message, \
    ProcessorDatasources, TaskScheduler
from app.main.forms import ImportForm
import processor.reporting.vmcolumns as vmc


app = create_app()
app.app_context().push()


from datetime import datetime
current_jobs = app.scheduler.get_jobs()
for x in current_jobs:
    app.scheduler.cancel(x)

scheduled = TaskScheduler.query.filter(TaskScheduler.end_date > datetime.today())

import datetime as dt
import pytz
from datetime import datetime
eastern = pytz.timezone('US/Eastern')
for x in scheduled:
    first_run = datetime.combine(datetime.today() + dt.timedelta(days=1),
                                 x.scheduled_time)
    first_run = eastern.localize(first_run)
    first_run = first_run.astimezone(pytz.utc)
    interval_sec = int(x.interval) * 60 * 60
    repeat = (x.end_date - x.start_date).days * (24 / int(x.interval))
    job = app.scheduler.schedule(
        scheduled_time=first_run,
        func='app.tasks' + x.name,
        kwargs={'processor_id': x.processor_id,
                'current_user_id': x.user_id,
                'processor_args': 'full'},
        interval=int(interval_sec),
        repeat=int(repeat),
        timeout=32400,
        result_ttl=None
    )
    sched = TaskScheduler.query.filter_by(processor_id=x.processor_id).first()
    if sched:
        db.session.delete(sched)
        db.session.commit()
    name = '.full_run_processor'
    processor = Processor.query.get(x.processor_id)
    description = 'Scheduling processor: {}'.format(processor.name)
    print(description)
    schedule = TaskScheduler(
        id=job.get_id(), name=name, description=description,
        created_at=datetime.utcnow(), scheduled_time=x.scheduled_time,
        start_date=x.start_date, end_date=x.end_date, interval=x.interval,
        user_id=x.user_id, processor=processor)
    db.session.add(schedule)
    task = Task(id=job.get_id(), name=name, description=description,
                user_id=x.user_id, processor=processor, complete=True)
    db.session.add(task)
    db.session.commit()


