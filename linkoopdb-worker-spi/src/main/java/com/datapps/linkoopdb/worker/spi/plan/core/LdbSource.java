package com.datapps.linkoopdb.worker.spi.plan.core;

import java.io.Serializable;

/**
 * Created by gloway on 2019/2/26.
 */
public abstract class LdbSource implements Serializable {

    public abstract String getStatistic();
}
