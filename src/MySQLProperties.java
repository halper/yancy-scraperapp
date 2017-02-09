import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by alper on 1/19/17.
 */

class MySQLProperties extends Properties {
    private static final Logger logger = Logger.getLogger( MySQLProperties.class );
    MySQLProperties() {
        FileInputStream fis;
        try {
            fis = new FileInputStream("db.properties");
            load(fis);
        } catch (IOException e) {
            logger.error("File error", e);
        }
    }
}
