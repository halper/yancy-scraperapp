package utilities;

import org.apache.log4j.Logger;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * Created by alper on 1/11/17.
 */
public class ConnectionHandler {
    private HttpURLConnection connection;
    private URL url;
    Proxy proxy;
    private static final Logger logger = Logger.getLogger(ConnectionHandler.class);

    public ConnectionHandler(URL url) {
        this.url = url;
        this.proxy = ProxyHolder.getInstance().getProxy();
        setConnection();
    }

    private void setConnection() {
        System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
        try {
            if (url.getProtocol().contains("https")) {
                connection = handleHttps();
            } else {
                connection = handleHttp();
            }
        } catch (IOException ignored) {
        }
    }

    private HttpsURLConnection handleHttps() throws IOException {
        return (HttpsURLConnection) getURLConnection();
    }

    private URLConnection getURLConnection() throws IOException {
        return proxy != null ? url.openConnection(proxy) : url.openConnection();
    }

    private HttpURLConnection handleHttp() throws IOException {
        return (HttpURLConnection) getURLConnection();
    }

    public HttpURLConnection getConnection() {
        return connection;
    }

    public boolean initiateConnection() throws SSLException {
        int code;
        int timeOut = 0;
        int waitFor = 8000;
        int maxTimeoutTrial = 3;

        connection.setRequestProperty("Connection", "keep-alive");
        connection.setRequestProperty("Host", url.getHost());
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36");
        connection.setConnectTimeout(waitFor);
        try {
            connection.setRequestMethod("GET");
            connection.connect();
            code = connection.getResponseCode();
            if (code != 200) return false;
        } catch (SocketTimeoutException e) {
            timeOut++;
            if (timeOut >= maxTimeoutTrial) {
                logger.error("Timeout exception for " + url);
                connection.disconnect();
                return false;
            }
        } catch (IOException e) {
            if (e instanceof SocketException) {
                logger.error("Socket exception for " + url);
            } else if (e instanceof SSLException) {
                logger.error("SSLException for " + url);
            } else {
                logger.error("Exception for " + url, e);
            }
            connection.disconnect();
        }
        return true;
    }
}
