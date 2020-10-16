package com.datapps.linkoopdb.worker.spi.hessian;

import java.lang.reflect.Proxy;
import java.net.URL;

import com.caucho.hessian.client.HessianProxyFactory;
import com.caucho.hessian.io.HessianRemoteObject;

/**
 * Created by gloway on 2019/5/14.
 */
public class HessianProxyWithContextFactory extends HessianProxyFactory {

    String token;

    public HessianProxyWithContextFactory() {
        this.token = "";
    }

    public HessianProxyWithContextFactory(String token) {
        this.token = token;
    }

    @Override
    public Object create(Class api, URL url, ClassLoader loader) {
        if (api == null) {
            throw new NullPointerException("api must not be null for HessianProxyFactory.create()");
        }
        HessianProxyWithContext handler = new HessianProxyWithContext(url, this, api, token);
        return Proxy.newProxyInstance(loader, new Class[]{api, HessianRemoteObject.class}, handler);
    }

    /**
     * Setter for property 'token'.
     *
     * @param token Value to set for property 'token'.
     */
    public void setToken(String token) {
        this.token = token;
    }
}
