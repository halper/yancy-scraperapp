package utilities;

import java.net.InetSocketAddress;
import java.net.Proxy;

/**
 * Created by alper on 2/9/17.
 */
public class ProxyHolder {
    private Proxy proxy;
    private static final ProxyHolder INSTANCE = new ProxyHolder();

    public static ProxyHolder getInstance() {
        return INSTANCE;
    }

    public void setProxy(String host, int port) {
        this.proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, port));
    }

    public Proxy getProxy() {
        return proxy;
    }
}
