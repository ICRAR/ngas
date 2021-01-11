"""SMTP server for testing"""
import collections
import email
import smtpd
import sys
import threading

_to_email_message = email.message_from_string
if sys.version_info[0] == 3:
    _to_email_message = email.message_from_bytes


class InMemorySMTPServerBase(object):
    """SMTP server that saves messages into a public list"""

    message = collections.namedtuple('message', 'mailfrom rcpts data')

    def __init__(self, port):
        self.port = port
        self.messages = []
        # email sending on the server can be asynchronous from the instructions
        # used to trigger them in the tests. We tried using only a Condition
        # instead of a Condition/event pair, but Condition.wait() didn't signal
        # timeouts until python 3.2
        self.recv_cond = threading.Condition()
        self.recv_evt = threading.Event()

    def pop(self, timeout=10):
        with self.recv_cond:
            while not self.messages:
                self.recv_cond.wait(timeout=timeout)
                if not self.recv_evt.is_set():
                    raise RuntimeError('email expected but none arrived')
                self.recv_evt.clear()
            return _to_email_message(self.messages.pop().data)

    def close(self):
        smtpd.SMTPServer.close(self)
        self.thread.join(timeout=1)
        if self.thread.is_alive():
            raise RuntimeError('asyncore loop still running')

    def receive(self, mailfrom, rcpttos, data):
        with self.recv_cond:
            self.messages.append(InMemorySMTPServerBase.message(mailfrom, rcpttos, data))
            self.recv_evt.set()
            self.recv_cond.notify_all()

try:
    import smtpd
except ImportError:
    smtpd = None

if smtpd:

    import asyncore

    class InMemorySMTPServer(InMemorySMTPServerBase, smtpd.SMTPServer):
        """SMTP server based on smtpd system module, removed in python 3.10"""

        def __init__(self, port):
            # decode_data is new in 3.5, defaults to True in 3.5, False in 3.6+
            # Thus, we need to explicitly give it to reliably use message_from_bytes
            # later
            kwargs = {}
            if sys.version_info[0:2] >= (3, 5):
                kwargs['decode_data'] = False
            InMemorySMTPServerBase.__init__(self, port)
            smtpd.SMTPServer.__init__(self, ('127.0.0.1', port), None, **kwargs)
            self.server_thread = threading.Thread(target=asyncore.loop, args=(0.1,))
            self.server_thread.daemon = True
            self.server_thread.start()

        def close(self):
            smtpd.SMTPServer.close(self)
            self.server_thread.join(timeout=1)
            if self.server_thread.is_alive():
                raise RuntimeError('asyncore loop still running')

        def process_message(self, peer, mailfrom, rcpttos, data, **_):
            self.receive(mailfrom, rcpttos, data)

else:

    from aiosmtpd.controller import Controller

    class InMemorySMTPServer(InMemorySMTPServerBase, Controller):
        """SMTP server based on aiosmtpd package"""

        class DummyAwaitable(object):
            def __await__(self):
                return self
            def __iter__(self):
                return self
            def __next__(self):
                raise StopIteration('250 OK')

        class ReceivingHandler(object):
            def __init__(self, server):
                self.server = server

            def handle_DATA(self, _server, _session, envelope):
                self.server.receive(envelope.mail_from, envelope.rcpt_tos, envelope.content.replace(b"\r\n", b'\n'))
                return InMemorySMTPServer.DummyAwaitable()

        def __init__(self, port):
            handler = InMemorySMTPServer.ReceivingHandler(self)
            InMemorySMTPServerBase.__init__(self, port)
            Controller.__init__(self, handler, hostname='127.0.0.1', port=port, enable_SMTPUTF8=False)
            self.start()

        def close(self):
            self.stop()