package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.Map;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/2/26.
 */
public class LdbCsvSource extends LdbSource {

    Map<String, String> options;
    Type[] fieldTypes;
    String path;
    String[] columns;

    public LdbCsvSource(Map<String, String> options, Type[] fieldTypes, String path, String[] columns) {
        this.options = options;
        this.fieldTypes = fieldTypes;
        this.path = path;
        this.columns = columns;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public Type[] getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(Type[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
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
