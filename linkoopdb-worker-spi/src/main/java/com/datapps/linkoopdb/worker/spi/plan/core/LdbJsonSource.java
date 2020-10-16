package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.Map;

import com.datapps.linkoopdb.jdbc.types.Type;

public class LdbJsonSource extends LdbSource {

    Map<String, String> options;
    Type[] fieldTypes;
    String path;
    String[] columns;

    public LdbJsonSource(Map<String, String> options, Type[] fieldTypes, String path, String[] columns) {
        this.options = options;
        this.fieldTypes = fieldTypes;
        this.path = path;
        this.columns = columns;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public Type[] getFieldTypes() {
        return fieldTypes;
    }

    public String getPath() {
        return path;
    }

    public String[] getColumns() {
        return columns;
    }

    @Override
    public String getStatistic() {
        return null;
    }
}
