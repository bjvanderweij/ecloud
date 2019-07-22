import logging, smtplib, os

logger = logging.getLogger('ecloud')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def get_pub_key(path='.ssh/id_rsa.pub'):
    """Read public key from path (default='.ssh/id_rsa.pub')."""
    pub_key = os.path.join(os.environ['HOME'], path)
    if os.path.exists(pub_key):
        with open(pub_key) as f:
            return f.read()
    else:
        logger.warning('No public key found at {}. Generate one with ssh-keygen.')

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
