import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Created by alper on 1/25/17.
 */
public class TwitterFactorySource {
    public static TwitterFactory getFactory(){
        NotifierProperties tProps = new NotifierProperties();
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(tProps.getConsumerKey())
                .setOAuthConsumerSecret(tProps.getConsumerSecret())
                .setOAuthAccessToken(tProps.getAccessToken())
                .setOAuthAccessTokenSecret(tProps.getAccessTokenSecret());
        return new TwitterFactory(cb.build());
    }
}
