package alma.ngas.client;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ConnectionManager {
    private static final long TIMEOUT = 10 * 60000;

    private static ConnectionManager instance;

    private LinkedHashMap<InetSocketAddress, Date> myConnectionMap = new LinkedHashMap<InetSocketAddress, Date>();

    private Iterator<Map.Entry<InetSocketAddress, Date>> myConnectionIterator;

    private long retryTime = TIMEOUT;

    private Logger logger;

    private ConnectionManager() {
    }

    /**
     * Will retrieve a 'CONNECTED' HttpURLConnection to an NGAS server.
     * <p>
     * The method will connect to the servers specified in
     * {@link #setConnectionList(List)} using round-robin. In case no servers
     * have been set an IOException is thrown. In case a server causes an
     * IOException the server will be marked as unusable for the amount of time
     * stated by {@link #setRetryTime(long)} which default is 10 minutes. If no
     * server is deemed to be available an IOException is thrown.
     * <p>
     * In case a redirect (HTTP code 301 - 303) is returned by a server the
     * method will follow redirects 20 times before bailing out with an
     * IOException.
     * 
     * @param inCommand
     *            a String being the command that will be sent.
     * 
     * @return a 'CONNECTED' HttpURLConnection object, in other words, the
     *         command has already been sent and the response from the server is
     *         ready to be retrieved from the returned connection object.
     * 
     * @throws IOException
     *             in case of communication error.
     */
    public synchronized HttpURLConnection getConnection(String inCommand) throws IOException {
        URL url = null;
        HttpURLConnection outConnection = null;
        if (myConnectionMap.size() == 0) {
            throw new IOException(
                    "The connections to use has not been defined.");
        }
        /*
         * This MUST be set before the connection is created, otherwise it has
         * no effect.
         */
        HttpURLConnection.setFollowRedirects(true);
        int code = 0;
        int redirectCount = 0;
        InetSocketAddress address = null;
        int connectionRetrievalCount = 0;
        boolean done = false;
        while (!done) {
            /*
             * First retrieve a connection that has not been marked as not
             * working.
             */
            connectionRetrievalCount++;
            if (connectionRetrievalCount > this.myConnectionMap.size()) {
                /*
                 * The connection map does not contain any addresses that are
                 * currently not considered broken.
                 */
                throw new IOException(
                        "There are currently no working NGAS servers available.");
            }
            if (!this.myConnectionIterator.hasNext()) {
                this.myConnectionIterator = this.myConnectionMap.entrySet()
                        .iterator();
            }
            Map.Entry<InetSocketAddress, Date> entry = this.myConnectionIterator
                    .next();
            Date date = entry.getValue();
            if (date != null) {
                if (System.currentTimeMillis() > date.getTime()
                        + this.retryTime) {
                    /*
                     * Retry this connection.
                     */
                    if (this.logger != null) {
                        this.logger
                                .info("Connection "
                                        + entry.getKey()
                                        + " will now be attempted again after a retry time of "
                                        + this.retryTime + " milliseconds.");
                    }
                    entry.setValue(null);
                } else {
                    continue;
                }
            }
            address = entry.getKey();
            String host = address.getHostName();
            int port = address.getPort();
            try {
                while (!done) {
                    url = new URL("http", host, port, inCommand);
                    outConnection = (HttpURLConnection) url.openConnection();
                    outConnection.setRequestMethod("GET");
                    outConnection.connect();
                    code = outConnection.getResponseCode();
                    if (code > 300 && code < 304) {
                        redirectCount++;
                        if (redirectCount > 20) {
                            throw new IOException(
                                    "More than 20 redirects have been peformed."
                                            + " This indicates an infinite loop. "
                                            + "The last address tried was "
                                            + host + ":" + port);
                        }
                        Map<String, List<String>> headerMap = outConnection
                                .getHeaderFields();
                        /*
                         * Not possible to just do a get() from the map since
                         * the specification states that headers should be
                         * treated case insensitive.
                         */
                        for (String header : headerMap.keySet()) {
                            if ("Location".equalsIgnoreCase(header)) {
                                String value = headerMap.get(header).get(0);

                                if (value.startsWith("http://"))
                                {
                                    String myvalue = value.replaceAll("http://","");
                                    String[] mytokens = myvalue.split("/");
                                    value = mytokens[0];
                                }

                                String[] tokens = value.split(":");
                                host = tokens[0];
                                if (tokens.length > 1) {
                                    port = Integer.parseInt(tokens[1]);
                                }
                                break;
                            }
                        }
                    } else {
                        done = true;
                    }
                }
            } catch (IOException e) {
                /*
                 * Mark address as broken by adding current time.
                 */
                if (this.logger != null) {
                    this.logger
                            .warning("Connection "
                                    + address
                                    + " is considered broke and will be attempted again after a retry time of "
                                    + this.retryTime + " milliseconds.");
                }
                this.myConnectionMap.put(address, new Date());
            }
        }
        return outConnection;
    }

    public synchronized static ConnectionManager getInstance() {
        if (instance == null) {
            instance = new ConnectionManager();
        }
        return instance;
    }

    public synchronized void setConnectionList(List<InetSocketAddress> inConnectionList) {
        if (inConnectionList == null) {
            throw new IllegalArgumentException(
                    "The supplied connection list may not be null.");
        }
        if (inConnectionList.isEmpty()) {
            throw new IllegalArgumentException(
                    "The supplied connection list may not be empty.");
        }
        this.myConnectionMap = new LinkedHashMap<InetSocketAddress, Date>();
        for (InetSocketAddress address : inConnectionList) {
            this.myConnectionMap.put(address, null);
        }
        this.myConnectionIterator = this.myConnectionMap.entrySet().iterator();
    }

    public synchronized void setLogger(Logger inLogger) {
        this.logger = inLogger;
    }

    public synchronized void setRetryTime(long inMillis) {
        this.retryTime = inMillis;
    }
}
