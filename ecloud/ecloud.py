import settings, pyone, subprocess, tempfile, os, time, warnings, traceback 
from pony.orm import db_session, select, commit
from ecdb import Task, TaskResult, FileTransfer, Worker, Context, TaskCommand
from xmlrpc.client import ProtocolError
# sending mail
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

# DECORATORS

def retry_on_exception(exception, sleep=1, max_attempts=10):

    def decorator(f):

        def wrapper(*args, attempt=0, **kwargs):

            if not attempt > max_attempts:
                try:
                    return f(*args, **kwargs)
                except exception as e:
                    warnings.warn('Failed to do %s (attempt=%d/%d). Args: %s, kwargs: %s. Request returned error: %s' % (f.__name__, attempt, max_attempts, args, kwargs, e))
                    time.sleep(sleep)
                    return wrapper(*args, attempt=attempt+1, **kwargs)

        return wrapper

    return decorator


@db_session()
def queue_task(task, merge_strategy=None):

    if merge_strategy is None:
        return Task.from_dict(task, status=Task.QUEUED)

    t = Task.get(lambda t: t.task_id == task['key'])
    if merge_strategy == 'keep_succeeded':
        # Task exists and did not succeed (yet)
        if t is not None and (not t.result.success if t.result else True):
            t.delete()
            commit()
            return Task.from_dict(task, status=Task.QUEUED)
        elif t is None:
            return Task.from_dict(task, status=Task.QUEUED)
    elif merge_strategy == 'replace':
        if t is None:
            return Task.from_dict(task, status=Task.QUEUED)
        else:
            print('Replacing %s from DB with task from dict.' % t.task_id)
            t.delete()
            commit()
            return Task.from_dict(task, status=Task.QUEUED)

    return t

def send_file(address, name, contents):

    tfile = tempfile.NamedTemporaryFile('w')

    with tfile as f:
        f.write(contents)
        f.flush()
        p = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', tfile.name, '%s:%s' % (address, name)])
        p.communicate()

 
 # From: https://stackoverflow.com/a/3363254
def send_mail(send_from, send_to, subject, text, files=None,
              server="127.0.0.1"):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)


    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()
