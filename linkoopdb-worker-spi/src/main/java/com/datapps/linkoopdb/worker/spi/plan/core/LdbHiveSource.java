package com.datapps.linkoopdb.worker.spi.plan.core;

/**
 * Created by gloway on 2019/2/26.
 */
public class LdbHiveSource extends LdbSource {

    public String tableName;
    public String[] columns;

    public LdbHiveSource(String tableName, String[] columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }


    @Override
    public String getStatistic() {
        return null;
    }
}
