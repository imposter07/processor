import os
import sys
import json
import time
import shutil
from flask import render_template
from rq import get_current_job
from app import create_app, db
from app.email import send_email
from app.models import User, Post, Task, Processor, Message
from distutils.dir_util import copy_tree
app = create_app()
app.app_context().push()


def _set_task_progress(progress):
    job = get_current_job()
    if job:
        job.meta['progress'] = progress
        job.save_meta()
        task = Task.query.get(job.get_id())
        task.user.add_notification('task_progress', {'task_id': job.get_id(),
                                                     'progress': progress})
        if progress >= 100:
            task.complete = True
        db.session.commit()


def export_posts(user_id):
    try:
        user = User.query.get(user_id)
        _set_task_progress(0)
        data = []
        i = 0
        total_posts = user.posts.count()
        for post in user.posts.order_by(Post.timestamp.asc()):
            data.append({'body': post.body,
                         'timestamp': post.timestamp.isoformat() + 'Z'})
            time.sleep(5)
            i += 1
            _set_task_progress(100 * i // total_posts)

        send_email('[LQApp] Your blog posts',
                   sender=app.config['ADMINS'][0], recipients=[user.email],
                   text_body=render_template('email/export_posts.txt',
                                             user=user),
                   html_body=render_template('email/export_posts.html',
                                             user=user),
                   attachments=[('posts.json', 'application/json',
                                 json.dumps({'posts': data}, indent=4))],
                   sync=True)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def adjust_path(path):
    path = path.replace('S:', '/mnt/share')
    path = path.replace('C:', '/mnt/c')
    path = path.replace('c:', '/mnt/c')
    path = path.replace('\\', '/')
    return path


def processor_post_message(proc, usr, text):
    msg = Message(author=usr, recipient=usr, body=text)
    db.session.add(msg)
    usr.add_notification('unread_message_count', usr.new_messages())
    post = Post(body=text, author=usr, processor_id=proc.id)
    db.session.add(post)
    db.session.commit()


def run_processor(processor_id, current_user_id):
    try:
        processor_to_run = Processor.query.get(processor_id)
        user_that_ran = User.query.get(current_user_id)
        _set_task_progress(0)
        file_path = adjust_path(processor_to_run.local_path)
        os.chdir(file_path)
        from main import main
        main('--noprocess')
        msg_text = ("{} finished running.".format(processor_to_run.name))
        processor_post_message(processor_to_run, user_that_ran, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())


def create_processor(processor_id, current_user_id, base_path):
    try:
        new_processor = Processor.query.get(processor_id)
        user_create = User.query.get(current_user_id)
        old_path = adjust_path(base_path)
        new_path = adjust_path(new_processor.local_path)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        copy_tree(old_path, new_path)
        msg_text = "Processor was created."
        processor_post_message(new_processor, user_create, msg_text)
        _set_task_progress(100)
    except:
        _set_task_progress(100)
        app.logger.error('Unhandled exception', exc_info=sys.exc_info())
