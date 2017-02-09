package utilities;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

/**
 * Created by alper on 1/11/17.
 */
public class HostVerifier implements HostnameVerifier {
    @Override
    public boolean verify(String paramString, SSLSession paramSSLSession) {
        return true;
    }

}
