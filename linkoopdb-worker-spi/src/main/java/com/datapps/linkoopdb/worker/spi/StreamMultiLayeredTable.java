package com.datapps.linkoopdb.worker.spi;

import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamMultiLayeredTable extends MultiLayeredTable {

    protected static final Logger logger = LoggerFactory.getLogger(StreamMultiLayeredTable.class);
    private static final long serialVersionUID = -2360403154883948619L;
    private boolean asSource;
    private boolean asSink;
    private Object tableInfo;

    public static StreamMultiLayeredTable copy(MultiLayeredTable source) {
        try {
            StreamMultiLayeredTable dist = new StreamMultiLayeredTable();
            BeanUtils.copyProperties(dist, source);
            dist.setTableInfo(source.getStable());
            return dist;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public boolean isAsSource() {
        return asSource;
    }

    public void setAsSource(boolean asSource) {
        this.asSource = asSource;
    }

    public boolean isAsSink() {
        return asSink;
    }

    public void setAsSink(boolean asSink) {
        this.asSink = asSink;
    }

    public Object getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(Object tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public Object getStable() {
        return this.getTableInfo();
    }

    @Override
    public void setStable(Object stable) {
        this.setTableInfo(stable);
    }
}
