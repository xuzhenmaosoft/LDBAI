package com.datapps.linkoopdb.worker.spi.plan.core;

import com.datapps.linkoopdb.jdbc.Row;

/**
 * Created by gloway on 2019/3/6.
 */
public class LdbLocalRelation extends LdbSource {

    public Row[] data;

    public LdbLocalRelation() {
        data = new Row[0];
    }

    public LdbLocalRelation(Row[] data) {
        this.data = data;
    }

    @Override
    public String getStatistic() {
        return null;
    }


}
