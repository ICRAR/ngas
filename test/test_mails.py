#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
import email

from ngamsLib import ngamsHighLevelLib

from . import ngamsTestLib
import six


class SimpleMailingTests(ngamsTestLib.ngamsTestSuite):

    def test_mail_is_sent(self):
        """Send a simple email, check it is sent (not necessarily received)"""

        rcpts = ['alice@localhost.localdomain']
        self.start_smtp_server()

        def assert_mail_sent(subject, msg_data):
            self.assertEqual(0, len(self.smtp_server.messages))
            ngamsHighLevelLib.sendEmail(
                self.env_aware_cfg(), subject, rcpts, 'me@localhost.com',
                msg_data)
            self.assertEqual(1, len(self.smtp_server.messages))
            msg = self.smtp_server.messages.pop()
            self.assertListEqual(msg.rcpts, rcpts);
            if six.PY3:
                msg = email.message_from_bytes(msg.data)
            else:
                msg = email.message_from_string(msg.data)
            self.assertIn(subject, msg['subject'])
            self.assertIn(msg_data, msg.get_payload())

        assert_mail_sent('my subject', 'my message')
        assert_mail_sent('my subject', '.my data with a dot')
        assert_mail_sent('my subject with a pile of \u0001f4a9',
                         '.my data with a dot and\na new line')
        assert_mail_sent('my subject with a pile of \u0001f4a9',
                         '.my data with a dot and\na new line and a pile of \u0001f4a9')