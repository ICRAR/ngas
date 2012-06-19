/*******************************************************************************
 * ESO/ALMA
 *
 * "@(#) $Id: JClient.java,v 1.7 2009/10/29 14:24:14 cmoins Exp $"
 * 
 * Who       When        What
 * --------  ----------  -------------------------------------------------------
 * F.Woolfe  2003        First version together with S.Zampieri/ESO/DFS.
 * jknudstr  2007/06/20  Gave overhaul, included in ALMA NG/AMS Module.
 * apersson  2008-09-27  Removed a really weird and clumsy construction using String arrays.
 *
 * For communicating with NGAS; allows you to specify commands. These
 * are sent using HTTP and an instance of {@link Status} is returned
 * which represents the response from NGAS.
 */
package alma.ngas.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Info from NGAS about incoming files. Specifically, files that are received by
 * the miniserver as a result of a SUBSCRIBE command are sent via HTTP POST.
 */
class FileInfo {
    /**
     * The mime type extracted from the HTTP header.
     */
    String mimeType;

    /**
     * The file name extracted from the HTTP header.
     */
    String fileName;

    /**
     * Constructs an instance of this class, with particular information stored
     * in it.
     * 
     * @param mimeType
     *            The mime-type.
     * @param fileName
     *            The filename.
     */
    FileInfo(String mimeType, String fileName) {
        this.mimeType = mimeType;
        this.fileName = fileName;
    }
}

// EOF

public class JClient {
    /**
     * The number of bytes sent / retrieved at a time during data transfers.
     */
    private final static int BLOCK_SIZE = 1024;

    public static void main(String[] args) {
        Logger logger = Logger.getLogger("ArchiverTest logger");
        logger.setLevel((Level) Level.ALL);
        List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
        list.add(new InetSocketAddress("ga004786", 7777));
        JClient client = new JClient(list, logger);
        Status status = client.status();
        System.out.println("XML Status Document:\n" + status.getXML());
        status = client.retrieve("TEST.2001-05-08T15:25:00.123",
                "TEST.2001-05-08T15:25:00.123.fits.Z");
        System.out.println("XML Status Document:\n" + status.getXML());
        status = client.retrieve("NON-EXISTING", "");
        System.out.println("XML Status Document:\n" + status.getXML());
        try {
            FileOutputStream outStream = new FileOutputStream(
                    "NCU.2003-11-11T11:11:11.111.fits.Z");
            status = client.retrieveStream("NCU.2003-11-11T11:11:11.111",
                    outStream);
            System.out.println("XML Status Document:\n" + status.getXML());
        } catch (FileNotFoundException e) {
            logger.warning("FileNotFoundException while retrieving file. "
                    + "Error: " + e.toString());
        }
        // Status status = client.subscribe(null, null, null,
        // "2000-01-01", null);
        // miniServer = new MiniServer(7777, blockSize);
        // Thread miniServerThread = new Thread(miniServer);
        // miniServerThread.start();
    }

    /**
     * The port that the miniServer uses to listen for files coming from NGAS.
     */
    private int miniServerPort;

    /**
     * The url of the ngams host that we want to connect to.
     */
    private String ngamsHost;

    /**
     * The port number on which ngams listens for incoming requests.
     */
    private int ngamsPort;

    /**
     * True if and only if localUrl is subscribed to NGAMS.
     */
    private boolean subscribed = false;

    /**
     * When a subscribe command is sent a {@link MiniServer} is created and
     * launched in a new thread; this then listens for HTTP ARCHIVE requests
     * coming from NGAS.
     */
    private static MiniServer miniServer;

    /**
     * JClient uses log4j; this logger logs events.
     */
    public Logger logger;

    /**
     * The url to which JClient wants to subscribe.
     */
    public String url;

    /**
     * The path where incoming files are stored.
     */
    private String dataRaw;

    private ConnectionManager myConnectionManager = ConnectionManager
            .getInstance();

    private String myProcessing;

    private String myProcessingParameters;

    /**
     * 
     * @param inConnectionList
     *            List containing InetSocketAddress objects pointing to all NGAS
     *            servers that can be accessed by the client.
     * @param inMiniServerPort
     *            The local port number on which to set up a miniServer (used
     *            only for subscribe)
     * @param inDataRaw
     *            The directory to put incoming file in
     * @param inURL
     *            The url to which NGAMS will send files, if we subscribe; if
     *            this is null it is taken to be the local URL by default
     * @param inLogger
     *            The log4j logger to log events with (must be properly set up
     *            before passing it to JClient).
     */
    public JClient(List<InetSocketAddress> inConnectionList,
            int inMiniServerPort, String inDataRaw, String inURL,
            Logger inLogger) {
        this.logger = inLogger;
        this.myConnectionManager.setConnectionList(inConnectionList);
        // TODO remove this handling once the archive methods use the connection
        // manager.
        this.ngamsHost = inConnectionList.get(0).getHostName();
        this.ngamsPort = inConnectionList.get(0).getPort();
        this.miniServerPort = inMiniServerPort;
        this.dataRaw = inDataRaw;
        // If the localUrl is given as null, take the local address as default.
        try {
            if (inURL == null)
                inURL = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            String msg = "The parameter URL to the constructor JClient was "
                    + "specified as null. This means the constructor attempts to "
                    + "find the URL of the local machine using the method "
                    + "InetAddress.getLocalHost().getCanonicalHostName(). Note "
                    + "that JClient needs the URL of the local machine for "
                    + "handling SUBSCRIBE commands. In this case a miniServer "
                    + "object is set up by JClient in order to listen for files "
                    + "HTTP POST command. An error occured while trying to find "
                    + "the URL of the local machine. The exception message is: "
                    + e.getMessage();
            logger.severe(msg);
        }
        this.url = "http://" + inURL + ":" + inMiniServerPort + "/ARCHIVE";
    }

    /**
     * 
     * @param inConnectionList
     *            List containing InetSocketAddress objects pointing to all NGAS
     *            servers that can be accessed by the client.
     * @param inLogger
     *            The log4j logger to log events with (must be properly set up
     *            before passing it to JClient).
     */
    public JClient(List<InetSocketAddress> inConnectionList, Logger inLogger) {
        if (inConnectionList.isEmpty()) {
            throw new IllegalArgumentException(
                    "The supplied connection list may not be empty.");
        }
        this.myConnectionManager.setConnectionList(inConnectionList);
        // TODO remove this handling once the archive methods use the connection
        // manager.
        this.ngamsHost = inConnectionList.get(0).getHostName();
        this.ngamsPort = inConnectionList.get(0).getPort();
        this.logger = inLogger;
    }

    ConnectionManager getConnectionManager() {
        return this.myConnectionManager;
    }

    /**
     * Called by the public archive methods. Contains the code that actually
     * connects to NGAS.
     * 
     * @param filename
     *            The name of the file to archive.
     * @param contentType
     *            The mime type of the file to archive.
     * @param noVersioning
     *            Used to deal with the possibility of many files with the same
     *            ID. Please see NGAMS users manual.
     * @return Object representing the response of NGAMS to the command sent.
     */
    private Status _archive(String filename, String contentType,
            String noVersioning) {
        try {
            String contentDisposition = "attachment;";
            int wait = 1;
            File myFile = new File(filename);
            long filesize = myFile.length();
            String basename = myFile.getName();
            URL url = null;
            url = new URL("http", ngamsHost, ngamsPort, "ARCHIVE");
            HttpURLConnection con = null;
            con = (HttpURLConnection) url.openConnection();
            HttpURLConnection.setFollowRedirects(true);
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", "NG/AMS J-API");
            con.setRequestProperty("Content-Type", contentType);
            con.setRequestProperty("Content-Length", String.valueOf(filesize));
            contentDisposition += "filename=\"" + basename + "\";";
            contentDisposition += "wait=\"" + wait + "\";";
            contentDisposition += (noVersioning == "1") ? ""
                    : " no_versioning=0";
            con.setRequestProperty("Content-Disposition", contentDisposition);
            con.connect();
            DataInputStream in = new DataInputStream(
                    new FileInputStream(myFile));
            DataOutputStream out = new DataOutputStream(con.getOutputStream());
            byte[] buf = new byte[BLOCK_SIZE];
            int nRead = 0;
            while ((nRead = in.read(buf, 0, BLOCK_SIZE)) > 0) {
                out.write(buf, 0, nRead);
                out.flush();
            }
            in.close();
            out.close();
            int code = con.getResponseCode();
            String msg = con.getResponseMessage();
            BufferedReader reader = null;
            // If there's a problem, log it!
            if (code != 200)
                logger.warning("NGAS returned HTTP code: " + code
                        + " in response to archive request, in "
                        + "JClient.myArchive. Please see instance of "
                        + "Status returned for more information.");
            reader = new BufferedReader(
                    new InputStreamReader((code == 200) ? con.getInputStream()
                            : con.getErrorStream()));
            String xml = "";
            String line = null;
            while ((line = reader.readLine()) != null)
                xml = xml + line + "\n";
            reader.close();
            con.disconnect();
            return new Status(code, msg, xml, dataRaw + "ngams.xml", logger);
        } catch (IOException e) {
            logger.warning("IOException in JClient._archive. Please see "
                    + "instance of Status returned for more details");
            return new Status(false, "Error generated by myArchive in "
                    + "JClient. Description:\n" + e.toString());
        }
    }

    /**
     * Called by the public archiveStream method. Contains the code that
     * actually connects to NGAS.
     * 
     * Holger Meuss: this method is a modified version of
     * 
     * @see _archive.
     * 
     * @param inStream
     *            The data stream to archive.
     * @param content_type
     *            The mime-type of the data to archive
     * @param filename
     *            The name.
     * @param noVersioning
     *            used to deal with the possibility of many files with the same
     *            id. Please see NGAMS users manual.
     * @return Object representing the response of NGAMS to the command sent.
     * 
     *         In fact, the filename parameter is mainly ignored. The file is
     *         stored under the name provided in the MIME header of the digested
     *         stream.
     */
    private Status _ArchiveStream(InputStream inStream, String contentType,
            String filename, String noVersioning) {
        try {
            String contentDisposition = "attachment;";
            int wait = 1;
            URL url = null;
            url = new URL("http", ngamsHost, ngamsPort, "ARCHIVE");
            HttpURLConnection con = null;
            con = (HttpURLConnection) url.openConnection();
            HttpURLConnection.setFollowRedirects(true);
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", "NG/AMS J-API");
            con.setRequestProperty("Content-Type", contentType);
            // TODO wieder rein (neu berechnen):
            // con.setRequestProperty("Content-Length",
            // String.valueOf(filesize));
            contentDisposition += "filename=\"" + filename + "\";";
            contentDisposition += "wait=\"" + wait + "\";";
            contentDisposition += (noVersioning == "1") ? ""
                    : " no_versioning=0";
            con.setRequestProperty("Content-Disposition", contentDisposition);
            con.connect();
            DataInputStream in = new DataInputStream(inStream);
            DataOutputStream out = new DataOutputStream(con.getOutputStream());
            byte[] buf = new byte[BLOCK_SIZE];
            int nRead = 0;
            while ((nRead = in.read(buf, 0, BLOCK_SIZE)) > 0) {
                out.write(buf, 0, nRead);
                out.flush();
            }
            in.close();
            out.close();
            int code = con.getResponseCode();
            String msg = con.getResponseMessage();
            BufferedReader reader = null;
            // If there's a problem, log it!
            if (code != 200)
                logger.warning("NGAS returned HTTP code: " + code
                        + " in response to archive request, in "
                        + "JClient._archive. Please see instance of "
                        + "Status returned for more information.");
            reader = new BufferedReader(
                    new InputStreamReader((code == 200) ? con.getInputStream()
                            : con.getErrorStream()));
            String xml = "";
            String line = null;
            while ((line = reader.readLine()) != null)
                xml = xml + line + "\n";
            reader.close();
            con.disconnect();
            return new Status(code, msg, xml, null, dataRaw + "ngams.xml",
                    logger);
        } catch (IOException e) {
            logger.warning("IOException in JClient._archive. Please see "
                    + "instance of Status returned for more details");
            return new Status(false, "Error generated by myArchive in "
                    + "JClient. Description:\n" + e.toString());
        }
    }

    /**
     * Retrieve a file. This is called by all the public retrieve methods. It
     * contains code for connecting to NGAS.
     * 
     * @param cmd
     *            The command to send.
     * @param fileNameDestination
     *            The name for the file when it reaches your computer.
     * @return Object representing the response of NGAMS to the command sent.
     */
    private Status _Retrieve(String cmd, String fileNameDestination) {
        try {
            HttpURLConnection con = this.myConnectionManager.getConnection(cmd);
            int code = con.getResponseCode();
            String msg = con.getResponseMessage();
            if (code != 200) { // Error handling here: HTTP says all is not OK
                String xml = "";
                BufferedReader reader = null;
                reader = new BufferedReader(new InputStreamReader(con
                        .getErrorStream()));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    xml = xml + line + "\n";
                }
                ;
                reader.close();
                // Log an error message.
                String logMsg = "Error when attempting to send a retrieve "
                        + "command:\n" + cmd
                        + " to NGAS. For further information, "
                        + "consult the instance of Status returned.";
                logger.warning(logMsg);
                return new Status(code, msg, xml, dataRaw + "ngams.xml", logger);
            }
            ;
            // Create objects to read data from the socket and store it in
            // a file.
            File fileDestination = new File(fileNameDestination);
            DataInputStream in = new DataInputStream(con.getInputStream());
            DataOutputStream out = new DataOutputStream(new FileOutputStream(
                    fileDestination));
            byte[] buf = new byte[BLOCK_SIZE];
            int nRead = 0;
            while ((nRead = in.read(buf, 0, BLOCK_SIZE)) > 0) {
                // System.out.println("Read " + totRead + " bytes");
                out.write(buf, 0, nRead);
                out.flush();
            }
            in.close();
            out.close();
            code = con.getResponseCode();
            msg = con.getResponseMessage();
            con.disconnect();
            // Log a message.
            logger.info("Sent retrieve message to NGAMS: " + cmd);
            return new Status(code, msg);
        } catch (IOException e) {
            logger.warning("IOException sending retrieve command to NGAS. "
                    + "Tried to send command: " + cmd);
            return new Status(false, "Error generated by _Retrieve in "
                    + "JClient. Description:\n" + e.toString());
        }
    }

    /**
     * Retrieve a file. This is called by all the public retrieve methods. It
     * contains code for connecting to NGAS.
     * 
     * @param cmd
     *            The command to send.
     * @param fileNameDestination
     *            The name for the file when it reaches your computer.
     * @return Object representing the response of NGAMS to the command sent.
     */
    private Status _RetrieveStream(String cmd, OutputStream outStream) {
        try {
            HttpURLConnection con = this.myConnectionManager.getConnection(cmd);
            int code = con.getResponseCode();
            String msg = con.getResponseMessage();
            if (code != 200) { // Error handling here: HTTP says all is not OK
                String xml = "";
                BufferedReader reader = null;
                reader = new BufferedReader(new InputStreamReader(con
                        .getErrorStream()));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    xml = xml + line + "\n";
                }
                ;
                reader.close();
                // Log an error message.
                String logMsg = "Error when attempting to send a retrieve "
                        + "command:\n" + cmd
                        + " to NGAS. For further information, "
                        + "consult the instance of Status returned.";
                logger.warning(logMsg);
                return new Status(code, msg, xml, dataRaw + "ngams.xml", logger);
            }
            ;
            // Create objects to read data from the socket and out into a file.
            DataInputStream in = new DataInputStream(con.getInputStream());
            DataOutputStream out = new DataOutputStream(outStream);
            String contentLength = con.getHeaderField("content-length");

            byte[] buf = new byte[BLOCK_SIZE];
            int nRead = 0;
            while ((nRead = in.read(buf, 0, BLOCK_SIZE)) > 0) {
                // System.out.println("Read " + totRead + " bytes");
                out.write(buf, 0, nRead);
                out.flush();
            }
            in.close();
            out.close();
            code = con.getResponseCode();
            msg = con.getResponseMessage();
            con.disconnect();
            // Log a message.
            logger.info("Sent retrieve message to NGAMS: " + cmd);
            Status status = new Status(code, msg);
            status.setFileSize(contentLength);
            return status;
        } catch (IOException e) {
            logger.warning("IOException sending retrieve command to NGAS. "
                    + "Tried to send command: " + cmd);
            return new Status(false, "Error generated by _Retrieve in "
                    + "JClient. Description:\n" + e.toString());
        }
    }

    /**
     * Request notification of file arrival from subscription. Note that the
     * number sent to event listeners is the number of files received SO FAR in
     * this session. Thus when the first file arrives it will be 1, when the
     * second file arrives it will be 2 etc. A session is the lifetime of the
     * thread holding the miniserver object and corresponds to a single
     * SUBSCRIBE command.
     * 
     * @param e
     *            Listener to notify when files arrive.
     * @return False if there is no subscription at present; true if request is
     *         succesful.
     */
    public boolean addFileReceivedEventListener(FileReceivedEventListener e) {
        if (miniServer == null) {
            logger.warning("addFileReceivedEventListener failed to add a "
                    + "listener since miniServer == null. Call "
                    + "subscribe to initialise miniServer.");
            return false;
        }
        if (!miniServer.listOfFileReceivedEventListeners.add(e)) {
            logger.warning("Error adding FileReceivedEventListener()");
            return false;
        }
        logger.info("Added FileReceivedEventListener");
        return true;
    }

    /**
     * Archive a file. The content type is guessed based on the file extension.
     * no_versioning is given the default value true.
     * 
     * @param filename
     *            File to archive.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status archive(String filename) {
        return archive(filename, null, true);
    }

    /**
     * Archives a file. Parameter no_versioning is given the default value true.
     * 
     * @param filename
     *            File to archive.
     * @param content_type
     *            mime type of file
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status archive(String filename, String contentType) {
        return archive(filename, contentType, true);
    }

    /**
     * Archive a file.
     * 
     * @param filename
     *            Name of file to archive.
     * @param contentType
     *            Mime-type of file to archive (guessed from extension if
     *            contentType is null)
     * @param noVersioning
     *            Used to deal with the possibility of many files with the same
     *            id. Please see NGAMS users manual.
     * @return Object representing the response of NGAMS to the command sent
     */
    public Status archive(String filename, String contentType,
            boolean noVersioning) {
        File check = new File(filename);
        if (!check.exists()) {
            logger.warning("Errror: the file you wish to send does not exist "
                    + "on your system. Error generated by archive in "
                    + "JClient.");
            return new Status(false, "Errror: the file you wish to send does "
                    + "not exist on your system. Error generated by "
                    + "archive in JClient.");
        }
        if (contentType == null) {
            String msg = "Error: No content type specified.";
            logger.warning(msg);
            return new Status(false, msg);
        }
        return _archive(filename, contentType, noVersioning ? "1" : "0");
    }

    /**
     * Archives a stream. Parameter no_versioning is given the default value
     * true (i.e. "1").
     * 
     * @param in
     *            Stream to archive
     * @param contentType
     *            Mime-type of stream.
     * @param filename
     *            The name (in our case uid, e.g. X0123456789abcdef:X01234567)
     *            of the file to be archived
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status archiveStream(InputStream in, String contentType,
            String filename) {
        return _ArchiveStream(in, contentType, filename, "1");
    }

    /**
     * Sends CLONE command to NGAMS.
     * 
     * @param fileId
     *            The File ID of the file to clone.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status clone(String fileId) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        return sendSimpleCmd("CLONE", parameterMap);
    }

    /**
     * Sends CLONE command to NGAMS.
     * 
     * @param diskId
     *            The disk holding the file to clone.
     * @param fileId
     *            The file to clone.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status clone(String diskId, String fileId) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("disk_id", diskId);
        parameterMap.put("file_id", fileId);
        return sendSimpleCmd("CLONE", parameterMap);
    }

    /**
     * Sends CLONE command to NGAMS. Any parameters specified passed with value
     * null are omitted from the string sent to NGAS.
     * 
     * @param diskId
     *            The disk holding the file to clone.
     * @param fileId
     *            The file to clone.
     * @param fileVersion
     *            The version of the file to clone.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status clone(String diskId, String fileId, String fileVersion) {
        if (diskId == null && fileId == null)
            return new Status(false, "This combination of arguments is "
                    + "illegal, when calling the CLONE command. At "
                    + "least one of disk_id and file_id must be "
                    + "specified. Error generated by clone in " + "JClient.");
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("disk_id", diskId);
        parameterMap.put("file_id", fileId);
        parameterMap.put("file_version", fileVersion);
        return sendSimpleCmd("CLONE", parameterMap);
    }

    /**
     * Find the configuration used by the NGAMS server.
     * 
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status configStatus() {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("configuration_file", null);
        return sendSimpleCmd("STATUS", parameterMap);
    }

    /**
     * Find the status of a particular disk.
     * 
     * @param diskId
     *            The Disk ID.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status diskStatus(String diskId) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("disk_id", diskId);
        return sendSimpleCmd("STATUS", parameterMap);
    }

    /**
     * Sends EXIT command to NGAMS.
     * 
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status exit() {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        return sendSimpleCmd("EXIT", parameterMap);
    }

    /**
     * Find the status of a particular file.
     * 
     * @param fileId
     *            The File ID.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status fileStatus(String fileId) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        return sendSimpleCmd("STATUS", parameterMap);
    }

    /**
     * Tell NGAMS to flush logs it may have cached internally into an internal
     * log file.
     * 
     * @return Status object representing the response from NGAMS.
     */
    public Status flushLog() {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("flush_log", null);
        return sendSimpleCmd("STATUS", parameterMap);
    }

    /**
     * Whether miniserver is now receiving a file. When a subscribe command
     * executed, a miniserver is launched on your machine. This listens for
     * incoming files from NGAS. This tests whether miniserver is in the process
     * of saving a file to disk. If it is and the application that instantiated
     * JClient quits the process will be interrupted and problems may result.
     * 
     * @return whether miniserver is now taking a file
     */
    public boolean getMiniServerTakingFile() {
        if (miniServer != null)
            return miniServer.takingFile;
        else
            return false;
    }

    /**
     * Sends INIT command to NGAMS.
     * 
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status init() {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        return sendSimpleCmd("INIT", parameterMap);
    }

    /**
     * Sends a LABEL command to NGAMS.
     * 
     * @param slotId
     *            The Slot ID of the disk to label.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status label(String slotId) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("slot_id", slotId);
        return sendSimpleCmd("LABEL", parameterMap);
    }

    /**
     * Sends a LABEL command to NGAMS (used if you want to label a disk on a
     * host other than the host in ngamsHost field of this object).
     * 
     * @param slotId
     *            The slot containing the disk to label.
     * @param hostId
     *            The host with that disk on it.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status label(String slotId, String hostId) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("slot_id", slotId);
        parameterMap.put("host_id", hostId);
        return sendSimpleCmd("LABEL", parameterMap);
    }

    /**
     * Sends OFFLINE command to NGAMS.
     * 
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status offline() {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        return sendSimpleCmd("OFFLINE", parameterMap);
    }

    /**
     * Forces NGAMS to go Offline even if it is in the middle of an operation -
     * USE WITH CARE.
     * 
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status offlineForce() {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("force", null);
        return sendSimpleCmd("OFFLINE", parameterMap);
    }

    /**
     * Sends ONLINE command to NGAMS.
     * 
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status online() {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        return sendSimpleCmd("ONLINE", parameterMap);
    }

    /**
     * Formats a command to send to NGAS. Commands accepted by ngas may have
     * parameters. Some of these parameters take values, others do not. A
     * command to send to NGAS is represented internally by a map of parameters
     * and their values. If the command should have no parameters then an empty
     * map should be provided. If the value for a key in the map is
     * <code>null</code>, this is assumed to be a parameter taking no value.
     * This method puts the command into the correct HTTP form. '?' separates
     * the command name from the parameter list. '&' is used to separate
     * parameters from each other. '=' is used to specify the value of a
     * parameter.
     * 
     * @param inCommand
     *            Command name e.g. "SUBSCRIBE".
     * @param inParameterMap
     *            The parameter map to give to the command.
     * 
     * @return a String being a URL in a form recognizable by NGAS.
     */
    private String prepareCommand(String inCommand,
            LinkedHashMap<String, String> inParameterMap) {
        StringBuilder builder = new StringBuilder(inCommand);
        if (inParameterMap.size() > 0) {
            builder.append("?");
            for (String key : inParameterMap.keySet()) {
                String value = inParameterMap.get(key);
                builder.append(key);
                if (value != null) {
                    builder.append("=");
                    builder.append(value);
                }
                builder.append("&");
            }
            // remove last &
            builder.setLength(builder.length() - 1);
        }

        if (this.myProcessing != null) {
	    builder.append("&processing=");
	    builder.append(this.myProcessing);
	    this.myProcessing = null;
	}

        if (this.myProcessingParameters != null) {
            builder.append("&processing_pars=");
            builder.append(this.myProcessingParameters);
            this.myProcessingParameters = null;
        }
        return builder.toString();
    }

    /**
     * Register existing files on a disk.
     * 
     * @param path
     *            The starting path under which ngams will look for files to
     *            register.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status register(String path) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("path", path);
        return sendSimpleCmd("REGISTER", parameterMap);
    }

    /**
     * Register existing files on a disk.
     * 
     * @param path
     *            the starting path under which ngams will look for files to
     *            register.
     * @param mime
     *            Comma separated list of mime-types to take into account, files
     *            with different mime-types will be ignored.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status register(String path, String mime) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("path", path);
        parameterMap.put("mime_type", mime);
        return sendSimpleCmd("REGISTER", parameterMap);
    }

    /**
     * Remove information about entire disks - USE WITH CARE.
     * 
     * @param diskId
     *            The disk to remove information about
     * @param execute
     *            If this is false no information is deleted; a report is
     *            returned saying what information would be deleted if execute
     *            were true
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status remdisk(String diskId, boolean execute) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("disk_id", diskId);
        parameterMap.put("execute", execute ? "1" : "0");
        return sendSimpleCmd("REMDISK", parameterMap);
    }

    /**
     * Delete a file.
     * 
     * @param fileId
     *            the file to delete
     * @param execute
     *            execute if false no files are actually removed; a report is
     *            returned saying what information would be removed if execute
     *            were true.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status remfile(String fileId, boolean execute) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        parameterMap.put("execute", execute ? "1" : "0");
        return sendSimpleCmd("REMFILE", parameterMap);
    }

    /**
     * Delete file on a certain disk.
     * 
     * @param diskId
     *            The disk on which the file to delete is stored.
     * @param fileId
     *            The file to delete.
     * @param execute
     *            If false no files are actually removed; a report is returned
     *            saying what information would be removed if execute were true.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status remfile(String diskId, String fileId, boolean execute) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("disk_id", diskId);
        parameterMap.put("file_id", fileId);
        parameterMap.put("execute", execute ? "1" : "0");
        return sendSimpleCmd("REMFILE", parameterMap);
    }

    /**
     * Delete files; some parameters are optional; please see the NGAMS manual.
     * Optional parameters may be omitted by giving them the value null.
     * 
     * @param diskId
     *            The Disk ID.
     * @param fileId
     *            The File ID.
     * @param fileVersion
     *            The File Version.
     * @param execute
     *            If false no files are actually removed; a report is returned
     *            saying what information would be removed if execute were true.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status remfile(String diskId, String fileId, String fileVersion,
            boolean execute) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("disk_id", diskId);
        parameterMap.put("file_id", fileId);
        parameterMap.put("file_version", fileVersion);
        parameterMap.put("execute", execute ? "1" : "0");
        return sendSimpleCmd("REMFILE", parameterMap);
    }

    /**
     * Cease notification of file arrival from subscription.
     * 
     * @param e
     *            listener who is, as of now, not to be notified of file arrival
     *            from subscription.
     * @return false if there is no subscription at present. True is request is
     *         succesfull or if e is not currently receiving notification of
     *         file arrival from subscription. False if there is no subscription
     *         at current time.
     */
    public boolean removeFileReceivedEventListener(FileReceivedEventListener e) {
        if (miniServer == null) {
            logger.warning("removeFileReceivedEventListener failed to "
                    + "remove a listener since miniServer == null. "
                    + "Call subscribe to initialise miniServer.");
            return false;
        }
        miniServer.listOfFileReceivedEventListeners.removeElement(e);
        logger.info("removed FileReceivedEventListener");
        return true;
    }

    /**
     * Retrieve a file.
     * 
     * @param fileId
     *            the file id of the file to retrieve
     * @param fileNameDestination
     *            the file name to give the file on your system
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status retrieve(String fileId, String fileNameDestination) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        String cmd = prepareCommand("RETRIEVE", parameterMap);
        return _Retrieve(cmd, fileNameDestination);
    }

    /**
     * Retrieve a file.
     * 
     * @param fileId
     *            The File ID of the file to retrieve.
     * @param fileVersion
     *            The version of the file to retrieve.
     * @param fileNameDestination
     *            The name to give the file on your computer.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status retrieve(String fileId, String fileVersion,
            String fileNameDestination) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        parameterMap.put("file_version", fileVersion);
        String cmd = prepareCommand("RETRIEVE", parameterMap);
        return _Retrieve(cmd, fileNameDestination);
    }

    /**
     * Retrieve a file. Parameters that are given the value null are omitted
     * from the final string sent to NGAS.
     * 
     * @param fileId
     *            the file id of the file to get.
     * @param fileVersion
     *            the version of the file to get.
     * @param internal
     *            whether the file is internal or not.
     * @param fileNameDestination
     *            the name to give the file on your system.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status retrieve(String fileId, String fileVersion, String internal,
            String fileNameDestination) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        parameterMap.put("file_version", fileVersion);
        parameterMap.put("internal", internal);
        String cmd = prepareCommand("RETRIEVE", parameterMap);
        return _Retrieve(cmd, fileNameDestination);
    }

    /**
     * Retrieve an internal file. Could be a log definition file for example.
     * 
     * @param fileId
     *            The id of the file to get.
     * @param filename
     *            The name of the file to get.
     * @param fileNameDestination
     *            the name to give the file on your computer
     * @return object representing the response of NGAMS to the command sent.
     */
    public Status retrieveInternal(String fileId, String filename,
            String fileNameDestination) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        parameterMap.put("internal", filename);
        String cmd = prepareCommand("RETRIEVE", parameterMap);
        return _Retrieve(cmd, fileNameDestination);
    }

    /**
     * Retrieve a log file.
     * 
     * @param fileId
     *            the id of the file to get.
     * @param fileNameDestination
     *            The name to give the file on your system.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status retrieveLog(String fileId, String fileNameDestination) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        parameterMap.put("ng_log", null);
        String cmd = prepareCommand("RETRIEVE", parameterMap);
        return _Retrieve(cmd, fileNameDestination);
    }

    /**
     * Retrieve a file into a stream.
     * 
     * @param fileId
     *            The File ID of the file to retrieve.
     * @param outStream
     *            Stream to which the file contents is written.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status retrieveStream(String fileId, OutputStream outStream) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        String cmd = prepareCommand("RETRIEVE", parameterMap);
        return _RetrieveStream(cmd, outStream);
    }

    /**
     * Retrieve a file into a stream.
     * 
     * @param fileId
     *            The File ID of the file to retrieve.
     * @param fileVersion
     *            The version of the file to retrieve.
     * @param outStream
     *            Stream to which the file contents is written.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status retrieveStream(String fileId, String fileVersion,
            OutputStream outStream) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        parameterMap.put("file_version", fileVersion);
        String cmd = prepareCommand("RETRIEVE", parameterMap);
        return _RetrieveStream(cmd, outStream);
    }

    /**
     * Retrieve a file. Parameters that are given the value null are omitted
     * from the final string sent to NGAS.
     * 
     * @param fileId
     *            the file id of the file to get.
     * @param fileVersion
     *            the version of the file to get.
     * @param internal
     *            whether the file is internal or not.
     * @param outStream
     *            Stream to which the file contents is written.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status retrieveStream(String fileId, String fileVersion,
            String internal, OutputStream outStream) {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("file_id", fileId);
        parameterMap.put("file_version", fileVersion);
        parameterMap.put("internal", internal);
        String cmd = prepareCommand("RETRIEVE", parameterMap);
        return _RetrieveStream(cmd, outStream);
    }

    /**
     * Retrieves a file from a Ngas server into a stream.
     * 
     * @param fileId
     *            The File ID of the file to retrieve.
     * @return the stream associated to the file to be retrieved
     */
    public HttpInputStream retrieveStream(String fileId) {
	LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
	parameterMap.put("file_id", fileId);
	final String cmd = prepareCommand("RETRIEVE", parameterMap);
	HttpInputStream dataStream = null;
	try {
	    final HttpURLConnection con = this.myConnectionManager.getConnection(cmd);
	    dataStream = new HttpInputStream(con,logger);
	} catch (IOException e) {
	    logger.warning("IOException sending retrieve command to NGAS. "
					+ "Tried to send command: " + cmd);
	}
	return dataStream;
    }

	/**
	 * Sets the DPPI that should be used to process the next command. This will
	 * affect ONLY the next issued command and then the DPPI parameters will be
	 * cleared.
	 * 
	 * @param inParameters
	 *            a String stating the parameters that should be added to the
	 *            next issued command.
	 */
	public void setProcessing(String inProcessing) {
		this.myProcessing = inProcessing;
	}

    /**
     * Sets the DPPI that should be used to process the next command. This will
     * affect ONLY the next issued command and then the DPPI will be cleared.
     * 
     * @param inParameters
     *            a String stating the parameters that should be added to the
     *            next issued command.
     */
    public void setProcessingParameters(String inParameters) {
        this.myProcessingParameters = inParameters;
    }

    /**
     * Sends a simple command. Just calls the other method with the same name
     * and the same parameters but passes false for receivingFile.
     * <p>
     * See {@link #prepareCommand(String, LinkedHashMap)} for details on input.
     * 
     * @param inCommand
     *            The command to send.
     * @param inParameterMap
     *            The parameter map to give to the command.
     * 
     * @return Status object representing the response of NGAMS to the command
     *         sent.
     * 
     * @see {@link #prepareCommand(String, LinkedHashMap)}
     */
    private Status sendSimpleCmd(String inCommand,
            LinkedHashMap<String, String> inParameterMap) {
        return sendSimpleCmd(inCommand, inParameterMap, false);
    }

    /**
     * Sends simple commands to NGAS. A simple command is one where all we have
     * to do is send a string and wait for the response. For example STATUS, but
     * not ARCHIVE or RETRIEVE. This method contains the code that actually
     * connects to NGAS via HTTP.
     * <p>
     * See {@link #prepareCommand(String, LinkedHashMap)} for details on input.
     * 
     * @param inCommand
     *            The command to send.
     * @param inParameterMap
     *            The parameter map to give to the command.
     * @param busy
     *            Whether the miniServer is currently receiving a file. Used by
     *            the SUBSCRIBE and UNSUBSCRIBE commands.
     * 
     * @return Status object representing the response of NGAMS to the command
     *         sent.
     * 
     * @see {@link #prepareCommand(String, LinkedHashMap)}
     */
    private Status sendSimpleCmd(String inCommand,
            LinkedHashMap<String, String> inParameterMap, boolean busy) {
        try {
            String completeCommand = prepareCommand(inCommand, inParameterMap);
            HttpURLConnection con = this.myConnectionManager
                    .getConnection(completeCommand);
            int code = con.getResponseCode();
            String msg = con.getResponseMessage();
            InputStream stream = con.getInputStream();
            if (code != 200) {
                stream = con.getErrorStream();
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    stream));
            String line = null;
            String xml = "";
            while ((line = reader.readLine()) != null)
                xml = xml + line + "\n";
            reader.close();
            con.disconnect();
            if (code == 200) {
                String logMsg = "Succesfully sent the following command to "
                        + "NGAS, via HTTP GET: ";
                logMsg += completeCommand;
                logger.info(logMsg);
            } else {
                String logMsg = "Error sending the following command to "
                        + "NGAS, via HTTP GET: ";
                logMsg += completeCommand;
                logMsg += "For more information on the error, consult the "
                        + "instance of Status ";
                logMsg += "returned by this method.";
                logger.warning(logMsg);
            }
            return new Status(code, msg, xml, dataRaw + "ngams.xml", logger);
        } catch (IOException e) {
            return new Status(false, "Error generated by sendSimpleCmd in "
                    + "JClient. Description: " + e.toString());
        }
    }

    /**
     * Find the status of the NGAS system.
     * 
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status status() {
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        return sendSimpleCmd("STATUS", parameterMap);
    }

    /**
     * Used if you want to retrieve many files from NGAS. A mini server is
     * launched on your machine that listens for the HTTP POST commands from
     * NGAS and saves incoming files. If the url specified (implicitly or
     * explicitly) is the same as the URL stored in the URL field, and this
     * JClient has already subscribed to NGAS, nothing is sent to NGAS and a
     * warning is logged. Apart from the URL, if an argument is given the value
     * null it will be omitted from the string sent to NGAS.
     * 
     * @param filterPlugIn
     *            Filter to apply to the files before they are sent.
     * @param plugInPars
     *            Parameters to send to the filter.
     * @param priority
     *            Priority for the data delivery; high numbers indicate low
     *            priority
     * @param startDate
     *            NGAS will send only files whose date is after the specified
     *            startDate.
     * @param url
     *            The URL to which NGAS should send the files; if specified as
     *            null this is taken to be whatever is in the url field of this
     *            JClient instance.
     * @param fileNamesOptions
     *            Specifies what names the files should have when they are saved
     *            in the directory dataRaw.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status subscribe(String filterPlugIn, String plugInPars,
            String priority, String startDate, String url) {
        if (url == null) {
            logger.info("The call to JClient.subscribe speficified the URL "
                    + "parameter as null. Using default url: " + this.url);
            url = this.url;
        }
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("filter_plug_in", filterPlugIn);
        parameterMap.put("plug_in_pars", plugInPars);
        parameterMap.put("priority", priority);
        parameterMap.put("start_date", startDate);
        parameterMap.put("url", url);
        // Set up the server to listen for incoming files
        miniServer = new MiniServer(miniServerPort, BLOCK_SIZE, dataRaw, logger);
        Thread miniServerThread = new Thread(miniServer);
        miniServerThread.start();
        while (!miniServer.ready)
            ; // Wait till the mini server finishes.
        // End subscribe message to NGAMS
        if (!subscribed && url.equals(this.url)) {
            subscribed = true;
            return sendSimpleCmd("SUBSCRIBE", parameterMap);
        } else if (!url.equals(this.url)) {
            logger.info("the call to JClient.subscribe refers to a URL: " + url
                    + " which is different from the default URL: " + this.url);
            return sendSimpleCmd("SUBSCRIBE", parameterMap);
        } else {
            String msg = "The call to JClient.subscribe either specified the "
                    + "parameter URL as null (meaning the default value: "
                    + this.url
                    + " is used) or explicitly passed parameter URL as "
                    + this.url
                    + ". But this URL has previously subscribed to "
                    + "NGAS. A second subscribe command has NOT been sent. You can "
                    + "find out whether the miniServer was receiving a file when "
                    + " this error message was generated by calling "
                    + "status.getReceivingFile() where status is the object "
                    + "returned by this method. At any other time you can find "
                    + "out whether the miniServer is taking a file by calling "
                    + "JClient.getMiniServerTakingFile().";
            logger.warning(msg);
            return new Status(true, msg);
        }
    }

    /**
     * Unsubscribe to data from NGAS. If the URL is the same as the URL
     * specified in the URL field of this instance of JClient, and we are not
     * currently subscribed, then nothing is send to NGAS and a warning is
     * logged.
     * 
     * @param url
     *            The URL which NGAS should stop sending data to; if specified
     *            as null this is taken to be the URL field of this instance of
     *            JClient.
     * @return Object representing the response of NGAMS to the command sent.
     */
    public Status unsubscribe(String url) {
        if (url == null) {
            logger.info("URL specified as null in call to "
                    + "JClient.unsubscribe. Using default: " + this.url);
            url = this.url;
        }
        LinkedHashMap<String, String> parameterMap = new LinkedHashMap<String, String>();
        parameterMap.put("url", url);
        // Stop the server running on this machine, listening for files
        // from NGAMS.
        if (miniServer != null)
            miniServer.serve = false;
        // Tell NGAMS to stop sending files
        if (subscribed && url.equals(this.url)) {
            subscribed = false;
            return sendSimpleCmd("UNSUBSCRIBE", parameterMap,
                    miniServer.takingFile);
        } else if (!url.equals(this.url)) {
            logger.info("the call to JClient.unsubscribe specifies a url "
                    + url + " different to the default url: " + this.url);
            return sendSimpleCmd("UNSUBSCRIBE", parameterMap,
                    miniServer.takingFile);
        } else {
            String msg = "The call to JClient.unsubscribe either specified the "
                    + "parameter url as null (meaning the default value: "
                    + this.url
                    + "is used) or explicitly passed parameter URL as "
                    + this.url
                    + ". But this URK is NOT currently subscribed to "
                    + "NGAS. An unnecessary UNSUBSCRIBE command has NOT been sent "
                    + "to NGAS. You can find out whether the miniServer was "
                    + "receiving a file when this error message was generated by "
                    + "calling status.getReceivingFile() where status is the "
                    + "object returned by this method. At any other time you can "
                    + "find out whether the miniServer is taking a file by "
                    + "calling JClient.getMiniServerTakingFile().";
            logger.warning(msg);
            return new Status(true, msg);
        }
    }
}

/**
 * Listens for incoming files from NGAS. This class is instantiated and the
 * instance launched in its own thread when a subscribe command is executed.
 */
class MiniServer implements Runnable {
    /**
     * Port for incoming files. This object listens for requests arriving on
     * this port.
     */
    int port;

    /**
     * Whether to carry on listening. If serve is set to false, the thread in
     * which the mini server is running will end.
     */
    boolean serve = true;

    /**
     * Whether a file is currently being saved to disk.
     */
    boolean takingFile = false;

    /**
     * Whether the server is set up and ready to listen for incoming files.
     * Subscribe waits for this to be set to true before sending the SUBSCRIBE
     * command.
     */
    boolean ready = false;

    /**
     * The log4j logger for recording events.
     */
    Logger logger;

    /**
     * The FileReceivedEventListeners who are notified of file arrivals from a
     * subscription.
     */
    Vector<FileReceivedEventListener> listOfFileReceivedEventListeners = new Vector<FileReceivedEventListener>();

    /**
     * The directory where to save incoming files.
     */
    String dataRaw;

    /**
     * The number of bytes to read at a time for incoming files.
     */
    int blockSize = 1024;

    /**
     * Instantiate the class MiniServer.
     * 
     * @param port
     *            The port on which to listen for incoming requests.
     * @param blockSize
     *            The number of bytes to read at a time from incoming files.
     * @param dataRaw
     *            The directory where to store incoming files.
     * @param logger
     *            The log4j logger (properly set up already) to record events.
     */
    MiniServer(int port, int blockSize, String dataRaw, Logger logger) {
        this.port = port;
        this.blockSize = blockSize;
        this.dataRaw = dataRaw;
        this.logger = logger;
    }

    /**
     * Sends a message to all the registered FileReceivedEventListeners (stored
     * in the Vector listOfFileReceivedEventListeners) that a new file has
     * arrived. Note: the number sent is the total number of files received in
     * this session so far. A session is a lifetime of the thread containing the
     * miniserver and corresponds to a single subscribe command
     * 
     * @param numFiles
     *            The number of files so far received.
     */
    void notifyListenersOfFileArrival(int numFiles) {
        FileReceivedEventListener listener;
        for (int i = 0; i < listOfFileReceivedEventListeners.size(); i++) {
            listener = (FileReceivedEventListener) listOfFileReceivedEventListeners
                    .elementAt(i);
            listener.fileReceived(new FileReceivedEvent(numFiles));
        }
        logger.info("miniServer is sending message to listeners that a new "
                + "file has arrived");
    }

    /**
     * Main body of server. Contains a while (serve) { ... } loop, so setting
     * serve to false makes running terminate.
     */
    public void run() {
        takingFile = false;
        try {
            ServerSocketChannel ssc = ServerSocketChannel.open();
            ssc.configureBlocking(false);
            InetAddress inetAddress = InetAddress.getLocalHost();
            InetSocketAddress socketAddress = new InetSocketAddress(
                    inetAddress, port);
            ssc.socket().bind(socketAddress);
            int i = 0;
            while (serve) {
                ready = true;
                SocketChannel sc = ssc.accept();
                if (sc != null) {
                    Socket client = sc.socket();
                    takingFile = true;
                    takeBinaryFile(client, i);
                    takingFile = false;
                    sc.close();
                    i = i + 1;
                    notifyListenersOfFileArrival(i);
                }
            }
            ssc.close();
            ready = false;
        } catch (IOException e) {
            logger.warning("IOException in MiniServer.run; description is: "
                    + e.toString());
        } catch (IllegalArgumentException e) {
            logger.warning("IllegalArgumentException in Miniserver.run; "
                    + "description is: " + e.toString());
        }
    }

    /**
     * Saves a single file to disk. The file is sent by NGAS.
     * 
     * @param client
     *            The Socket object which NGAS sends data through.
     * @param fileNumber
     *            As files arrive they are given temporary names based on how
     *            many other files have already been received. This name is
     *            "serverOut" + fileNumber. So the temporary name of the first
     *            file to be received in a session is serverOut0 etc. This
     *            parameter is passed by the run() method. Note: the code for
     *            parsing the HTTP POST header can be replaced by a standard
     *            class written for this purpose this could provide extra
     *            flexibility.
     * @return Object giving information that comes from the HTTP header
     *         received by NGAS.
     */
    FileInfo takeBinaryFile(Socket client, int fileNumber) {
        String fileName = dataRaw + "serverOut" + fileNumber;
        String fileNameFromHeader = null;
        String newline = "\r\n";
        try {
            // Set up objects for reading/writing text/data to client/local
            // file.
            DataInputStream in = new DataInputStream(client.getInputStream());
            PrintWriter textOut = new PrintWriter(new OutputStreamWriter(client
                    .getOutputStream()));
            DataOutputStream fileOutputStream;
            // Get the header.
            // System.out.println("about to get header");
            String header = "";
            char c;
            do {
                c = (char) in.readByte();
                header = header + String.valueOf(c);
            } while (!header.endsWith(newline + newline));
            // System.out.println(":"+header+":");
            // Get the filename from the header, to determine the mime type
            int i = header.indexOf("filename=\"");
            int j = header.indexOf("\"", i + 10);
            if (i == -1 || j == -1) {
                // So such substring found
                logger.warning("Filename wasn't specified in the HTTP header "
                        + "sent by NGAS, for file " + fileNumber);
                return new FileInfo(null, null);
            } else {
                // We have found the filename from the header.
                // obtain the correct file extension, and set up the
                // FileOutputStream to write data to this file.
                fileNameFromHeader = header.substring(i + 10, j);
                File outFile = new File(fileName);
                if (outFile.exists())
                    outFile.delete();
                FileOutputStream fos = new FileOutputStream(outFile);
                fileOutputStream = new DataOutputStream(fos);
            }
            // Get the length from the header.
            i = header.indexOf("length: ");
            if (i == -1) {
                logger.warning("Length wasn't specified in the HTTP header "
                        + "sent by NGAS, for file " + fileNumber
                        + " with name " + fileNameFromHeader);
                ;
                return new FileInfo(null, fileNameFromHeader);
            }
            j = header.indexOf(newline, i);
            int length = Integer.valueOf(header.substring(i + 8, j)).intValue();
            // Get the file from the client; storing it into the file.
            byte[] buf = new byte[blockSize];
            int nRead = 0, totalRead = 0;
            // System.out.println("about to enter while, length is "+length);
            while (totalRead < length
                    && (nRead = in.read(buf, 0, blockSize)) > 0) {
                totalRead = totalRead + nRead;
                fileOutputStream.write(buf, 0, nRead);
                fileOutputStream.flush();
                // System.out.println("have read "+ totalRead + " of " +length+
                // "
                // so far; and " + nRead + " this time.");
            }
            // System.out.println("end of while");
            // Say 'goodbye' to the client.
            textOut.print("HTTP/1.0 200\nContent-Type: text/plain\n\n");
            // Close down the (in/out)putstreams.
            in.close();
            textOut.close();
            fileOutputStream.close();
            return new FileInfo(null, fileNameFromHeader);
        } catch (IOException e) {
            logger.warning("IOEXception in MiniServer.takeBinaryFile; file "
                    + "name is: "
                    + ((fileNameFromHeader == null) ? "unknown"
                            : fileNameFromHeader) + "; description is: "
                    + e.toString());
            return new FileInfo(null, fileNameFromHeader);
        }
    }
} // End-class MiniServer.
