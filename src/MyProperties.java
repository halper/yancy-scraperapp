import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by alper on 2/9/17.
 */
abstract class MyProperties extends Properties {
    private static final Logger logger = Logger.getLogger( MyProperties.class );

    MyProperties(String propertiesFile) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(propertiesFile + ".properties");
            load(fis);
        } catch (IOException e) {
            logger.error("File error", e);
        }
    }


}
