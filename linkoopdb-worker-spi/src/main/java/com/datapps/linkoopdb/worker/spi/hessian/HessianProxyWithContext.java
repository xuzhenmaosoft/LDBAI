package com.datapps.linkoopdb.worker.spi.hessian;

import java.net.URL;

import com.caucho.hessian.client.HessianConnection;
import com.caucho.hessian.client.HessianProxy;
import com.caucho.hessian.client.HessianProxyFactory;

/**
 * Created by gloway on 2019/5/14.
 */
public class HessianProxyWithContext extends HessianProxy {

    String token;

    protected HessianProxyWithContext(URL url, HessianProxyFactory factory) {
        super(url, factory);
    }

    protected HessianProxyWithContext(URL url, HessianProxyFactory factory, Class<?> type) {
        super(url, factory, type);
    }

    public HessianProxyWithContext(URL url, HessianProxyFactory factory, Class<?> type, String token) {
        super(url, factory, type);
        this.token = token;
    }

    @Override
    protected void addRequestHeaders(HessianConnection conn)
    {
        super.addRequestHeaders(conn);

        // add Hessian Header
        conn.addHeader("token", token);
    }

}
