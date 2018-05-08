import os

ROOT_DIR='/home/ubuntu/elastic-cloud/'
HOME_DIR='/home/ubuntu/'

DEBUG = True

MAX_WORKERS = 30
WORKER_TEMPLATE = 9115
VIRTUAL_NETWORK_ID = 143
BOSS_TEMPLATE = 9099

MAX_API_ATTEMPTS = 5

#DATABASE = {'provider':'sqlite', 'filename':os.path.join(ROOT_DIR, 'ecloud.sqlite')}
#DATABASE = {'provider':'postgres', 'user':'ecloud', 'password':'ecloud', 'database':'ecloud', 'host':'localhost'}
DATABASE = {'provider':'mysql', 'user':'ecloud', 'password':'ecloud', 'database':'ecloud', 'host':'localhost'}
DATASTORE_DIR = '/data'

EMAIL_ENABLED = False
EMAIL_USERNAME = ''
EMAIL_PASSWORD = ''
EMAIL_SERVER = ''

ONE_API_ENDPOINT = 'https://api.hpccloud.surfsara.nl/RPC2'

with open(os.path.join(ROOT_DIR, 'auth')) as f:
    username, password = [l for l in f]

ONE_USERNAME=username.strip()
ONE_PASSWORD=password.strip()

