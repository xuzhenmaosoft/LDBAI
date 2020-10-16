package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.Map;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * 流表.
 */
public class LdbStreamSource extends LdbSource {

    private Map<String, String> options;
    private Type[] fieldTypes;
    private String[] columns;
    private String schema;
    private String table;

    public LdbStreamSource(Map<String, String> options, Type[] fieldTypes, String[] columns,
        String schema, String name) {
        this.options = options;
        this.fieldTypes = fieldTypes;
        this.columns = columns;
        this.schema = schema;
        this.table = name;
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

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    @Override
    public String getStatistic() {
        return null;
    }
}
