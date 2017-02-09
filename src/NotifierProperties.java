import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by alper on 1/19/17.
 */

class NotifierProperties extends Properties {
    private static final Logger logger = Logger.getLogger( NotifierProperties.class );

    NotifierProperties() {
        FileInputStream fis;
        try {
            fis = new FileInputStream("notifier.properties");
            load(fis);
        } catch (IOException e) {
            logger.error("IOException", e);
        }
    }

    String getConsumerKey() {
        return getProperty("ConsumerKey");
    }

    String getConsumerSecret() {
        return getProperty("ConsumerSecret");
    }

    String getAccessToken() {
        return getProperty("AccessToken");
    }

    String getAccessTokenSecret() {
        return getProperty("AccessTokenSecret");
    }

    String getGMailUsername() {
        return getProperty("GMailUsername");
    }

    String getGMailPassword() {
        return getProperty("GMailPassword");
    }
}
