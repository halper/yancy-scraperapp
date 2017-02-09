package utilities;

/**
 * Created by alper on 1/19/17.
 */

public class NotifierProperties extends MyProperties {

    public NotifierProperties() {
        super("notifier");
    }

    public String getConsumerKey() {
        return getProperty("ConsumerKey");
    }

    public String getConsumerSecret() {
        return getProperty("ConsumerSecret");
    }

    public String getAccessToken() {
        return getProperty("AccessToken");
    }

    public String getAccessTokenSecret() {
        return getProperty("AccessTokenSecret");
    }

    public String getGMailUsername() {
        return getProperty("GMailUsername");
    }

    public String getGMailPassword() {
        return getProperty("GMailPassword");
    }
}
