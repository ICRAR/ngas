/*
 *    ALMA - Atacama Large Millimiter Array
 *    (c) European Southern Observatory, 2002
 *    Copyright by ESO (in the framework of the ALMA collaboration),
 *    All rights reserved
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, 
 *    MA 02111-1307  USA
 *
 *    Created on Mar 17, 2005
 *
 */

// $Author: apersson $
// $Date: 2009/02/12 12:12:09 $
// $Log: NGAMSJClientTest.java,v $
// Revision 1.4  2009/02/12 12:12:09  apersson
// *** empty log message ***
//
// Revision 1.3  2009/01/19 09:22:50  apersson
// Completely reworked the test suite.
//
// Revision 1.2  2007/07/25 13:27:55  hmeuss
// adapted to new package structure
//
// Revision 1.1.1.1  2007/07/25 09:22:08  jknudstr
// Creation
//
// Revision 1.1.1.1  2007/06/22 12:08:45  jknudstr
// Creation
//
// Revision 1.2  2005/09/05 08:09:15  hmeuss
// using full path for almadev1 now
//
// Revision 1.1  2005/03/17 15:01:21  hmeuss
// Migrated to new directory structure
// 
package alma.ngas.client;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alma.ngas.client.JClient;

import junit.framework.TestCase;

/**
 * @author apersson
 */
public class NGAMSJClientTest extends TestCase {
    private String myLocalHost;

    private static final int TEST_PORT1 = 53241;

    private static final int TEST_PORT2 = 53242;

    private static final int REDIRECT_TEST_PORT = 53240;

    public NGAMSJClientTest(String name) throws Exception {
        super(name);
        this.myLocalHost = InetAddress.getLocalHost().getHostAddress();
    }

    public void testPrepareCommand() throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException {
        final Method method = JClient.class.getDeclaredMethod("prepareCommand",
                String.class, LinkedHashMap.class);
        method.setAccessible(true);

        List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
        list.add(new InetSocketAddress(this.myLocalHost, TEST_PORT1));
        JClient client = new JClient(list, null);
        String command = "TEST_COMMAND";
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        String expected = "TEST_COMMAND";
        String actual = (String) method.invoke(client, command, map);
        assertTrue("Returned string was [" + actual + "], expected was ["
                + expected + "]", expected.equals(actual));

        map = new LinkedHashMap<String, String>();
        map.put("parameter1", null);
        expected = "TEST_COMMAND?parameter1";
        actual = (String) method.invoke(client, command, map);
        assertTrue("Returned string was [" + actual + "], expected was ["
                + expected + "]", expected.equals(actual));

        map = new LinkedHashMap<String, String>();
        map.put("parameter1", "value1");
        expected = "TEST_COMMAND?parameter1=value1";
        actual = (String) method.invoke(client, command, map);
        assertTrue("Returned string was [" + actual + "], expected was ["
                + expected + "]", expected.equals(actual));

        map = new LinkedHashMap<String, String>();
        map.put("parameter1", null);
        map.put("parameter2", "value2");
        expected = "TEST_COMMAND?parameter1&parameter2=value2";
        actual = (String) method.invoke(client, command, map);
        assertTrue("Returned string was [" + actual + "], expected was ["
                + expected + "]", expected.equals(actual));

        map = new LinkedHashMap<String, String>();
        map.put("parameter1", "value1");
        map.put("parameter2", null);
        expected = "TEST_COMMAND?parameter1=value1&parameter2";
        actual = (String) method.invoke(client, command, map);
        assertTrue("Returned string was [" + actual + "], expected was ["
                + expected + "]", expected.equals(actual));

        map = new LinkedHashMap<String, String>();
        map.put("parameter1", "value1");
        map.put("parameter2", "value2");
        expected = "TEST_COMMAND?parameter1=value1&parameter2=value2";
        actual = (String) method.invoke(client, command, map);
        assertTrue("Returned string was [" + actual + "], expected was ["
                + expected + "]", expected.equals(actual));

    }

    public void testCommands() throws Exception {
        FakeServer server = new FakeServer(TEST_PORT1);
        server.start();
        Thread.sleep(1000);
        try {
            Logger logger = Logger.getAnonymousLogger();
            logger.setLevel(Level.SEVERE);
            List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
            list.add(new InetSocketAddress(this.myLocalHost, TEST_PORT1));
            JClient client = new JClient(list, logger);

            /*
             * Since the fake server does not return any xml the log will get
             * error messages regarding premature end of file. These can be
             * ignored.
             */
            client.init();
            String expected = "INIT";
            String actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.setProcessingParameters("nisse");
            client.init();
            expected = "INIT&processing_pars=nisse";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.init();
            expected = "INIT";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.exit();
            expected = "EXIT";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.online();
            expected = "ONLINE";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.clone("nisse");
            expected = "CLONE?file_id=nisse";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.clone("orvar", "nisse");
            expected = "CLONE?disk_id=orvar&file_id=nisse";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.clone("orvar", "nisse", "bosse");
            expected = "CLONE?disk_id=orvar&file_id=nisse&file_version=bosse";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.label("orvar");
            expected = "LABEL?slot_id=orvar";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.label("orvar", "nisse");
            expected = "LABEL?slot_id=orvar&host_id=nisse";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.offline();
            expected = "OFFLINE";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.offlineForce();
            expected = "OFFLINE?force";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.register("orvar");
            expected = "REGISTER?path=orvar";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.register("orvar", "nisse");
            expected = "REGISTER?path=orvar&mime_type=nisse";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.remdisk("orvar", false);
            expected = "REMDISK?disk_id=orvar&execute=0";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.remdisk("orvar", true);
            expected = "REMDISK?disk_id=orvar&execute=1";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.remfile("orvar", "nisse", "bosse", false);
            expected = "REMFILE?disk_id=orvar&file_id=nisse&file_version=bosse&execute=0";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.remfile("orvar", "nisse", "bosse", true);
            expected = "REMFILE?disk_id=orvar&file_id=nisse&file_version=bosse&execute=1";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.remfile("orvar", "nisse", false);
            expected = "REMFILE?disk_id=orvar&file_id=nisse&execute=0";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.remfile("orvar", "nisse", true);
            expected = "REMFILE?disk_id=orvar&file_id=nisse&execute=1";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.remfile("nisse", false);
            expected = "REMFILE?file_id=nisse&execute=0";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.remfile("nisse", true);
            expected = "REMFILE?file_id=nisse&execute=1";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.status();
            expected = "STATUS";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.diskStatus("orvar");
            expected = "STATUS?disk_id=orvar";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.fileStatus("nisse");
            expected = "STATUS?file_id=nisse";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.flushLog();
            expected = "STATUS?flush_log";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.configStatus();
            expected = "STATUS?configuration_file";
            actual = server.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));
        } finally {
            server.terminate();
            /*
             * Sleep to allow thread the time to shutdown in case the server is
             * reused in following test cases. Risk of bind() exception
             * otherwise.
             */
            Thread.sleep(1000);
        }
    }

    public void testStatus() throws Exception {
        FakeServer server = new FakeServer(TEST_PORT1);
        server.start();
        Thread.sleep(1000);
        try {
            Logger logger = Logger.getAnonymousLogger();
            logger.setLevel(Level.SEVERE);
            List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
            list.add(new InetSocketAddress(this.myLocalHost, TEST_PORT1));
            JClient client = new JClient(list, logger);

            Status status = client.status();

            String expected = "CompletionTimeValue";
            String actual = status.getCompletionTime();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "DateValue";
            actual = status.getDate();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "HostIdValue";
            actual = status.getHostId();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "LastRequestStatUpdateValue";
            actual = status.getLastRequestStatUpdate();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "MessageValue";
            actual = status.getMessage();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "RequestIdValue";
            actual = status.getRequestId();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "RequestTimeValue";
            actual = status.getRequestTime();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "StateValue";
            actual = status.getState();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "StatusValue";
            actual = status.getStatus();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "SubStateValue";
            actual = status.getSubState();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "VersionValue";
            actual = status.getVersion();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "ArchiveValue";
            actual = status.getArchive();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "AvailableMbValue";
            actual = status.getAvailableMb();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "BytesStoredValue";
            actual = status.getBytesStored();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "DiskChecksumValue";
            actual = status.getDiskChecksum();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "CompletedValue";
            actual = status.getCompleted();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "CompletionDateValue";
            actual = status.getCompletionDate();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "DiskIdValue";
            actual = status.getDiskId();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "InstallationDateValue";
            actual = status.getInstallationDate();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "LastCheckValue";
            actual = status.getLastCheck();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "LogicalNameValue";
            actual = status.getLogicalName();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "ManufacturerValue";
            actual = status.getManufacturer();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "MountPointValue";
            actual = status.getMountPoint();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "MountedValue";
            actual = status.getMounted();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "NumberOfFilesValue";
            actual = status.getNumberOfFiles();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "SlotIdValue";
            actual = status.getSlotId();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "TotalDiskWriteTimeValue";
            actual = status.getTotalDiskWriteTime();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "TypeValue";
            actual = status.getType();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "FileChecksumValue";
            actual = status.getFileChecksum();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "ChecksumPlugInValue";
            actual = status.getChecksumPlugIn();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "CompressionValue";
            actual = status.getCompression();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "CreationDateValue";
            actual = status.getCreationDate();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "FileIdValue";
            actual = status.getFileId();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "FileNameValue";
            actual = status.getFileName();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "FileSizeValue";
            actual = status.getFileSize();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "FileStatusValue";
            actual = status.getFileStatus();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "FileVersionValue";
            actual = status.getFileVersion();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "FormatValue";
            actual = status.getFormat();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "IgnoreValue";
            actual = status.getIgnore();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "IngestionDateValue";
            actual = status.getIngestionDate();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "TagValue";
            actual = status.getTag();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            expected = "UncompressedFileSizeValue";
            actual = status.getUncompressedFileSize();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

        } finally {
            server.terminate();
            /*
             * Sleep to allow thread the time to shutdown in case the server is
             * reused in following test cases. Risk of bind() exception
             * otherwise.
             */
            Thread.sleep(1000);
        }
    }

    public void testRedirect1() throws Exception {
        redirectTest(1);
    }

    public void testRedirect20() throws Exception {
        redirectTest(20);
    }

    public void testRedirect21() throws Exception {
        redirectTest(21);
    }

    private void redirectTest(int inRepeats) throws Exception {
        RedirectServer redirectServer = new RedirectServer(inRepeats);
        FakeServer server = new FakeServer(TEST_PORT1);
        try {
            server.start();
            Thread.sleep(1000);
            try {
                redirectServer.start();
                Thread.sleep(1000);
                Logger logger = Logger.getAnonymousLogger();
                logger.setLevel(Level.SEVERE);
                List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
                list
                        .add(new InetSocketAddress(myLocalHost,
                                REDIRECT_TEST_PORT));
                JClient client = new JClient(list, logger);

                Status status = client.status();
                if (status.getOK()) {
                    String expected = "STATUS";
                    String actual = server.getReceivedCommand();
                    if (inRepeats < 21) {
                        assertTrue("Returned string was [" + actual
                                + "], expected was [" + expected + "]",
                                expected.equals(actual));
                    } else {
                        throw new RuntimeException(status.getMsg());
                    }
                } else {
                    if (inRepeats < 21) {
                        throw new RuntimeException(status.getMsg());
                    }
                }
            } finally {
                redirectServer.terminate();
                /*
                 * Sleep to allow thread the time to shutdown in case the server
                 * is reused in following test cases. Risk of bind() exception
                 * otherwise.
                 */
                Thread.sleep(1000);
            }
        } finally {
            server.terminate();
            /*
             * Sleep to allow thread the time to shutdown in case the server is
             * reused in following test cases. Risk of bind() exception
             * otherwise.
             */
            Thread.sleep(1000);
        }
    }

    public void testServerFailOver() throws Exception {
        FakeServer server1 = null;
        FakeServer server2 = null;
        try {
            Logger logger = Logger.getAnonymousLogger();
            List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
            list.add(new InetSocketAddress(this.myLocalHost, TEST_PORT1));
            list.add(new InetSocketAddress(this.myLocalHost, TEST_PORT2));
            JClient client = new JClient(list, logger);

            Status status = client.status();
            assertTrue("Expected was that no servers are available.", !status
                    .getOK());

            server1 = new FakeServer(TEST_PORT1);
            server2 = new FakeServer(TEST_PORT2);
            server1.start();
            server2.start();
            Thread.sleep(1000);
            logger.setLevel(Level.SEVERE);

            /*
             * Even though the servers have started they should still not be
             * considered available since there is a 10 minute retry timeout.
             */
            status = client.status();
            assertTrue("Expected was that no servers are available.", !status
                    .getOK());

            client.getConnectionManager().setRetryTime(0);
            client.status();
            String expected = "STATUS";
            String actual = server1.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));
            /*
             * Verify that the fake server resets the received command.
             */
            expected = null;
            actual = server1.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected == actual);

            client.status();
            expected = "STATUS";
            actual = server2.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

            client.status();
            expected = "STATUS";
            actual = server1.getReceivedCommand();
            assertTrue("Returned string was [" + actual + "], expected was ["
                    + expected + "]", expected.equals(actual));

        } finally {
            server1.terminate();
            server2.terminate();
            /*
             * Sleep to allow thread the time to shutdown in case the server is
             * reused in following test cases. Risk of bind() exception
             * otherwise.
             */
            Thread.sleep(1000);
        }
    }

    private class FakeServer extends Thread {

        private boolean terminate = false;

        private String receivedCommand;

        private int portNumber;

        public FakeServer(int inPortNumber) {
            this.portNumber = inPortNumber;
        }

        public synchronized String getReceivedCommand() {
            String outString = this.receivedCommand;
            this.receivedCommand = null;
            return outString;
        }

        @Override
        public void run() {
            ServerSocketChannel channel = null;
            try {
                channel = ServerSocketChannel.open();
                channel.configureBlocking(false);
                InetAddress inetAddress = InetAddress.getLocalHost();
                InetSocketAddress socketAddress = new InetSocketAddress(
                        inetAddress, this.portNumber);
                channel.socket().bind(socketAddress);
                while (!terminate) {
                    SocketChannel sc = channel.accept();
                    Thread.sleep(100);
                    if (sc != null) {
                        Socket socket = sc.socket();
                        LineNumberReader in = new LineNumberReader(
                                new InputStreamReader(socket.getInputStream()));
                        String input = in.readLine();
                        /*
                         * Strip initial "GET " and trailing " HTTP/1.0". No
                         * need to synchronize this with the client call since
                         * the code sequence can be considered synchronous.
                         * Using wait without a timeout will actually cause a
                         * deadlock.
                         */
                        this.receivedCommand = input.substring(4, input
                                .length() - 9);
                        OutputStreamWriter out = new OutputStreamWriter(socket
                                .getOutputStream());
                        out.write("HTTP/1.0 200 OK\n");
                        out.write("Content-Type: text/html\n");
                        out.write("\n");
                        out.write("<DocTag>\n");
                        out.write("<Status ");
                        out.write("CompletionTime=\"CompletionTimeValue\" ");
                        out.write("Date=\"DateValue\" ");
                        out.write("HostId=\"HostIdValue\" ");
                        out
                                .write("LastRequestStatUpdate=\"LastRequestStatUpdateValue\" ");
                        out.write("Message=\"MessageValue\" ");
                        out.write("RequestId=\"RequestIdValue\" ");
                        out.write("RequestTime=\"RequestTimeValue\" ");
                        out.write("State=\"StateValue\" ");
                        out.write("Status=\"StatusValue\" ");
                        out.write("SubState=\"SubStateValue\" ");
                        out.write("Version=\"VersionValue\" ");
                        out.write("/>\n");

                        out.write("<DiskStatus ");
                        out.write("Archive=\"ArchiveValue\" ");
                        out.write("AvailableMb=\"AvailableMbValue\" ");
                        out.write("BytesStored=\"BytesStoredValue\" ");
                        out.write("Checksum=\"DiskChecksumValue\" ");
                        out.write("Completed=\"CompletedValue\" ");
                        out.write("CompletionDate=\"CompletionDateValue\" ");
                        out.write("DiskId=\"DiskIdValue\" ");
                        out
                                .write("InstallationDate=\"InstallationDateValue\" ");
                        out.write("LastCheck=\"LastCheckValue\" ");
                        out.write("LogicalName=\"LogicalNameValue\" ");
                        out.write("Manufacturer=\"ManufacturerValue\" ");
                        out.write("MountPoint=\"MountPointValue\" ");
                        out.write("Mounted=\"MountedValue\" ");
                        out.write("NumberOfFiles=\"NumberOfFilesValue\" ");
                        out.write("SlotId=\"SlotIdValue\" ");
                        out
                                .write("TotalDiskWriteTime=\"TotalDiskWriteTimeValue\" ");
                        out.write("Type=\"TypeValue\" ");
                        out.write("/>\n");

                        out.write("<FileStatus ");
                        out.write("Checksum=\"FileChecksumValue\" ");
                        out.write("ChecksumPlugIn=\"ChecksumPlugInValue\" ");
                        out.write("Compression=\"CompressionValue\" ");
                        out.write("CreationDate=\"CreationDateValue\" ");
                        out.write("FileId=\"FileIdValue\" ");
                        out.write("FileName=\"FileNameValue\" ");
                        out.write("FileSize=\"FileSizeValue\" ");
                        out.write("FileStatus=\"FileStatusValue\" ");
                        out.write("FileVersion=\"FileVersionValue\" ");
                        out.write("Format=\"FormatValue\" ");
                        out.write("Ignore=\"IgnoreValue\" ");
                        out.write("IngestionDate=\"IngestionDateValue\" ");
                        out.write("Tag=\"TagValue\" ");
                        out
                                .write("UncompressedFileSize=\"UncompressedFileSizeValue\" ");
                        out.write("/>\n");
                        out.write("</DocTag>\n");
                        /*
                         * Important to close stream, otherwise the client might
                         * not terminate the input and hang waiting for more
                         * data.
                         */
                        out.close();
                    }
                }
            } catch (Exception e) {
                Logger.getAnonymousLogger().log(Level.SEVERE,
                        "Connection issue", e);
            } finally {
                try {
                    channel.socket().close();
                } catch (Exception e) {
                }
                try {
                    channel.close();
                } catch (Exception e) {
                }
            }
        }

        public void terminate() {
            this.terminate = true;
        }
    }

    private class RedirectServer extends Thread {

        private boolean terminate = false;

        private int myRepeats;

        public RedirectServer(int inRepeats) {
            this.myRepeats = inRepeats;
        }

        @Override
        public void run() {
            ServerSocketChannel ssc = null;
            try {
                ssc = ServerSocketChannel.open();
                ssc.configureBlocking(false);
                InetAddress inetAddress = InetAddress.getLocalHost();
                InetSocketAddress socketAddress = new InetSocketAddress(
                        inetAddress, REDIRECT_TEST_PORT);
                ssc.socket().bind(socketAddress);
                while (!terminate) {
                    SocketChannel sc = ssc.accept();
                    Thread.sleep(100);
                    if (sc != null) {
                        Socket socket = sc.socket();
                        int port = REDIRECT_TEST_PORT;
                        this.myRepeats--;
                        if (this.myRepeats == 0) {
                            port = TEST_PORT1;
                        }
                        OutputStreamWriter out = new OutputStreamWriter(socket
                                .getOutputStream());
                        out.write("HTTP/1.0 301 Moved Permanently\n");
                        out.write("Location: " + inetAddress.getHostAddress()
                                + ":" + port + "\n");
                        out.write("\n");
                        /*
                         * Important to close stream, otherwise the client might
                         * not terminate the input and hang waiting for more
                         * data.
                         */
                        out.close();
                    }
                }
            } catch (Exception e) {
                Logger.getAnonymousLogger().log(Level.SEVERE,
                        "Connection issue", e);
            } finally {
                try {
                    ssc.socket().close();
                } catch (Exception e) {
                }
                try {
                    ssc.close();
                } catch (Exception e) {
                }
            }
        }

        public void terminate() {
            this.terminate = true;
        }
    }
}
