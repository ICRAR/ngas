#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
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

#******************************************************************************
#
# "@(#) $Id: ngamsSmtpLib.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/10/2003  Created
#

"""
Private implement of the SMTP Lib allowing to send a message contained in a
file.
"""

import os, smtplib


class ngamsSMTP(smtplib.SMTP):
    """
    This is a sub-class of the smtplib.SMTP class. The purpose is to make
    it possible to send the email contents from a file, in case a larger
    amount of data is send.
    """

    def __init__(self,
                 host = '',
                 port = 0):
        """
        Constructor method, invoking also the parent constructor.
        
        host:    Host ID for the SMTP host (string).
        
        port:    Port number for the SMTP host (integer).
        """
        smtplib.SMTP.__init__(self, host, port)
        self.__msgInFile = 0
        self.__dataFile  = None

    
    def data(self,
             msg):
        """
        SMTP 'DATA' command -- sends message data to server.

        Automatically quotes lines beginning with a period per rfc821.
        Raises SMTPDataError if there is an unexpected reply to the
        DATA command; the return value from this method is the final
        response code received when the all data is sent.

        msg:      Message in a string buffer or a filename referring to
                  a file containin the data to be send (string).

        Returns:  Tuple with the status code + a message from the SMTP
                  host (tuple/integer, string).
        """
        self.putcmd("data")
        (code, repl) = self.getreply()
        if self.debuglevel >0 : print "data:", (code, repl)
        if code != 354:
            raise SMTPDataError(code, repl)
        else:
            if (not self.__msgInFile):
                # Data contained in the message in memory.
                q = smtplib.quotedata(msg)
                if q[-2:] != smtplib.CRLF: q = q + smtplib.CRLF
                q = q + "." + smtplib.CRLF
                self.send(q)
            else:
                # Data to send is contained in a file.
                fo = open(msg)
                while (1):
                    buf = fo.read(16384)
                    if (buf == ""): break
                    qBuf = smtplib.quotedata(buf)
                    self.send(buf)
                fo.close()
                self.send(smtplib.CRLF + "." + smtplib.CRLF)
                
            (code, msg) = self.getreply()
            if (self.debuglevel > 0): print "data:", (code, msg)
            return (code, msg)


    def mail(self,
             sender,
             options = []):
        """
        SMTP 'mail' command -- begins mail xfer session.

        sender:   Send of the mail (string).

        options:  List with mail options in the form: '<par>=<val>'
                  (list/string).

        Returns:  See smptplib.SMTP().getreply().
        """
        # NG/AMS: Replace the size=<Size> with the size of the data file
        # in case data is contained in a file.
        for i in range(len(options)):
            option = options[i]
            if (option.find("size=") and self.__msgInFile):
                int(os.stat(self.__dataFile)[6])
                options[i] = "size=" + str(os.stat(self.__dataFile)[6])
                break
        
        optionlist = ''
        if options and self.does_esmtp:
            optionlist = ' ' + ' '.join(options)
        self.putcmd("mail", "FROM:%s%s" % (smtplib.quoteaddr(sender),
                                           optionlist))
        return self.getreply()
                
                
    def sendMail(self,
                 fromAddr,
                 toAddrs,
                 msg,
                 mail_options = [],
                 rcpt_options = [],
                 msgInFile = 0):
        """
        Send an email to the given addresses. If the data is contained in
        a file, this can be indicated by setting msgInFile=1.

        fromAddr:      From mail address (string).
        
        toAddrs:       To mail addresses as a comma separated list, or a list
                       of addresses (list/string).
        
        msg:           Message to send or name of file where the data to send
                       is contained (string).
        
        mailOptions:   Mail options - not used (list).
        
        rcptOptions:   RCPT options - not used (list)
        
        msgInFile:     Indicates if message to send is stored in a file
                       (integer/0|1).

        Returns:       See smptplib.SMTP().sendmail().
        """
        self.__msgInFile = msgInFile
        if (msgInFile): self.__dataFile = msg
        return self.sendmail(fromAddr, toAddrs, msg, mail_options=[],
                             rcpt_options=[])


if __name__ == '__main__':
    """
    Main program.
    """
    msg = "Subject:  TEST MESSAGE\n" +\
          "Content-type: text/plain\n\n" +\
          "THIS IS A SMALL TEST!!"
    
    srv = ngamsSMTP("smtphost.hq.eso.org")
    if (0):
        srv.sendMail("From: " + "jknudstr@eso.org",
                     "Bcc: " + "jknudstr@eso.org", msg)
    else:
        fn = "/tmp/mailmsg.del"
        fo = open(fn, "w")
        fo.write(msg)
        fo.close()
        srv.sendMail("From: " + "jknudstr@eso.org",
                     "Bcc: " + "jknudstr@eso.org",
                     fn, [], [], 1)


# EOF
