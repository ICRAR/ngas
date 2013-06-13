import logging,os

logger = logging.getLogger('ngasdaemonlogger')
try:
    hdlr = logging.FileHandler('/var/log/ngasdaemonlogger.log')
except IOError as e:
    try:
        os.makedirs('%s/var/log' % os.environ['HOME'])
    except OSError:
        pass
    hdlr = logging.FileHandler('%s/var/log/ngasdaemonlogger.log' % 
                               os.environ['HOME'])
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

class ngaslog:                
        def __init__(self, logtype, message):
                self.logtype = logtype
                self.logmessage = message
                if self.logtype == 'ERROR':
                        logger.error(self.logmessage)
                else:
                        logger.info(self.logmessage)

