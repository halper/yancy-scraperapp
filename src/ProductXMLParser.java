import org.apache.log4j.Logger;
import org.xml.sax.SAXException;
import utilities.ConnectionHandler;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.concurrent.Callable;

/**
 * Created by alper on 1/11/17.
 */

class ProductXMLParser implements Callable {
    private ProductURL productURL;
    private URL XMLURL;
    private static final Logger logger = Logger.getLogger(ProductXMLParser.class);


    ProductXMLParser(ProductURL productUrl) {
        this.productURL = productUrl;
        try {
            this.XMLURL = new URL(productURL.getUrlString() + ".xml");
        } catch (MalformedURLException e) {
            logger.error("MalformedURL Exc for ProductXMLParser with " + productUrl.getUrlString(), e);
        }
    }


    @Override
    public Product call() throws Exception {
        Product product = null;
        ConnectionHandler connectionHandler = new ConnectionHandler(XMLURL);
        HttpURLConnection connection = connectionHandler.getConnection();
        try {
            if (connectionHandler.initiateConnection()) {

                InputStream is = connection.getInputStream();
                ProductSaxParser psx = new ProductSaxParser(is);
                product = psx.getProduct();
                product.setProductURL(productURL);
            }
        } catch (IOException e) {
            product = null;
            if (e instanceof SocketTimeoutException) {
                logger.error("SocketTimeOutException for " + productURL.getUrlString());
            } else if (e.toString().contains("HTTP/1.1 503")) {
                logger.error("Tunneling error on " + productURL.getUrlString());
            }
            else{
                logger.error("IOException", e);
            }
        } catch (NullPointerException | SAXException s) {
            logger.error("Exception for " + productURL.getUrl(), s);
        } finally {
            connection.disconnect();
            connection = null;
        }

        if (product == null) {
            call();
        }
        return product;
    }

}
