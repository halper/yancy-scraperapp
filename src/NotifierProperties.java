/**
 * Created by alper on 1/19/17.
 */

class NotifierProperties extends MyProperties {

    NotifierProperties() {
        super("notifier");
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
