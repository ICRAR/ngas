import base64
from requests.auth import HTTPBasicAuth


def ngas_subscriber_auth_None(filename, url):
    """
    Don't do anything special with subscriptions
    """
    return None


def ngas_subscriber_auth_basic(filename, url):
    """
    Use HTTP Basic Auth to connect to server
    """
    return HTTPBasicAuth('ngas-int', base64.b64encode(b'ngas-int'))


def ngas_subscriber_auth_digest(filename, url):
    """
    Use HTTP Digest Auth to connect to server
    """
    return HTTPDigestAuth('ngas-int', base64.b64encode(b'ngas-int'))


def ngas_subscriber_auth_switch(filename, url):
    """
    Use different settings based on url

    https://toolbelt.readthedocs.io/en/latest/authentication.html#authhandler
    would be an alternative to switching based on url
    """
    if "ngas1" in url:
        return HTTPDigestAuth('ngas-int1', base64.b64encode(b'ngas-int1'))
    return HTTPDigestAuth('ngas-int', base64.b64encode(b'ngas-int'))


ngas_subscriber_auth = ngas_subscriber_auth_None
