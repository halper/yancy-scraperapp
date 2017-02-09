import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Created by alper on 1/13/17.
 */
class ProductURL {
    private URL url;
    private URL imageLoc;
    private String lastMod;
    private long id;
    private LocalDateTime updatedAt;
    private static final Logger logger = Logger.getLogger( ProductURL.class );

    void setUrl(String url) {
        try {
            setUrl(new URL(url));
        } catch (MalformedURLException e) {
            logger.error("Malformed URL: " + url);
        }
    }
    private void setUrl(URL url) {
        this.url = url;
    }

    URL getUrl() {
        return url;
    }

    URL getImageLoc() {
        return imageLoc;
    }

    void setImageLoc(String url) {
        try {
            setImageLoc(new URL(url));
        } catch (MalformedURLException e) {
            logger.error("Malformed imageURL: " + url);
        }
    }

    void setImageLoc(URL imageLoc) {
        this.imageLoc = imageLoc;
    }

    String getLastMod() {
        return lastMod;
    }

    void setLastMod(String lastMod) {
        this.lastMod = lastMod;
    }

    String getUrlString() {
        return url.toString();
    }

    long getId() {
        return id;
    }

    void setId(long id) {
        this.id = id;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
