package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * kafka source.
 */
public class LdbKafkaSource extends LdbSource {

    public static final String KEY_SERVERS = "kafka.bootstrap.servers";
    public static final String KEY_TOPIC = "subscribe";

    public KAFKA_FORMAT format;
    private String servers;
    private String subscribe;
    private String subscribePattern;
    private List<String> columnList;
    private Type[] colTypes;

    private String csvSeparator = ",";
    private String csvQuoteChar = "\"";
    private String csvEscapeChar = "\\";

    public LdbKafkaSource(KAFKA_FORMAT format, String servers, String subscribe, List<String> columnList, Type[] colTypes) {
        this.format = format;
        this.servers = servers;
        this.subscribe = subscribe;
        this.columnList = columnList;
        this.colTypes = colTypes;
    }

    @Override
    public String getStatistic() {
        return null;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getSubscribe() {
        return subscribe;
    }

    public void setSubscribe(String subscribe) {
        this.subscribe = subscribe;
    }

    public String getSubscribePattern() {
        return subscribePattern;
    }

    public void setSubscribePattern(String subscribePattern) {
        this.subscribePattern = subscribePattern;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public void setColumnList(String[] columns) {
        for (String col : columns) {
            columnList.add(col);
        }
    }

    public Type[] getColTypes() {
        return colTypes;
    }

    public void setColTypes(Type[] colTypes) {
        this.colTypes = colTypes;
    }

    public void setColumnList(List<String> columnList) {
        this.columnList = columnList;
    }

    public String getCsvSeparator() {
        return csvSeparator;
    }

    public void setCsvSeparator(String csvSeparator) {
        this.csvSeparator = csvSeparator;
    }

    public String getCsvQuoteChar() {
        return csvQuoteChar;
    }

    public void setCsvQuoteChar(String csvQuoteChar) {
        this.csvQuoteChar = csvQuoteChar;
    }

    public String getCsvEscapeChar() {
        return csvEscapeChar;
    }

    public void setCsvEscapeChar(String csvEscapeChar) {
        this.csvEscapeChar = csvEscapeChar;
    }

    public enum KAFKA_FORMAT {
        CSV, JSON
    }

    public static KAFKA_FORMAT valuesOf(String value) {
        switch (value) {
            case "json":
                return KAFKA_FORMAT.JSON;
            case "csv":
                return KAFKA_FORMAT.CSV;
            default:
                throw new RuntimeException("not support kafka format " + value);
        }
    }
}
