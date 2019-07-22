import os

ROOT_DIR='/home/ubuntu/ecloud/'
HOME_DIR='/home/ubuntu/'

BROKER_URL = 'boss.rhythm-uva.surf-hosted.nl'
WORKER_WAKEUP_TIME = 30

DEBUG = True

MAX_WORKERS = 30
WORKER_TEMPLATE = 12193
DATASTORE_TEMPLATE = 9101
VIRTUAL_NETWORK_ID = 143
BOSS_TEMPLATE = 12182

MAX_API_ATTEMPTS = 5

#DATABASE = {'provider':'sqlite', 'filename':os.path.join(ROOT_DIR, 'ecloud.sqlite')}
#DATABASE = {'provider':'postgres', 'user':'ecloud', 'password':'ecloud', 'database':'ecloud', 'host':'localhost'}
DATABASE = {'provider':'mysql', 'user':'ecloud', 'password':'ecloudecloud', 'database':'ecloud', 'host':'localhost'}
DATASTORE_DIR = '/data'
LOCAL_DATASTORE_DIR = 'data/'

EMAIL_ENABLED = False
EMAIL_USERNAME = ''
EMAIL_PASSWORD = ''
EMAIL_SERVER = ''

ONE_API_ENDPOINT = 'https://api.hpccloud.surfsara.nl/RPC2'

auth_path = os.path.join(ROOT_DIR, 'auth')
if os.path.exists(auth_path):
    with open(auth_path) as f:
        username, password = [l for l in f]
    ONE_USERNAME=username.strip()
    ONE_PASSWORD=password.strip()
else:
    print('No authentication details found in {}'.format(auth_path))

