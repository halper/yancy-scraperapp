import org.apache.log4j.Logger;
import org.xml.sax.SAXException;
import utilities.ConnectionHandler;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.concurrent.Callable;

/**
 * Created by alper on 1/11/17.
 */

class ProductXMLParser implements Callable {
    private ProductURL productURL;
    private static final Logger logger = Logger.getLogger( ProductXMLParser.class );

    ProductXMLParser(ProductURL productUrl) {
        this.productURL = productUrl;
    }


    @Override
    public Product call() throws Exception {
        Product product = null;
        ConnectionHandler connectionHandler = new ConnectionHandler(productURL.getUrl().toString() + ".xml");
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
            logger.error("IOException", e);
        } catch (NullPointerException | SAXException s) {
            logger.error("Exception for " + productURL.getUrl(), s);
        }
        finally {
            connection.disconnect();
            connection = null;
        }

        if (product == null) {
            call();
        }
        return product;
    }

}
