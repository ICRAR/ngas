import base64
from logging import getLogger

from requests.auth import HTTPBasicAuth

log = getLogger(__name__)

def ngas_subscriber_auth(filename, url):
    log.info("Using subscription plugin")
    return HTTPBasicAuth('ngas-int', base64.b64encode(b'ngas-int'))
