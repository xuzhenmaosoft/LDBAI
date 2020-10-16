/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import com.datapps.linkoopdb.jdbc.LdbSqlNameManager;
import com.datapps.linkoopdb.jdbc.Row;
import com.datapps.linkoopdb.jdbc.SessionInterface;
import com.datapps.linkoopdb.jdbc.lib.OrderedHashSet;
import com.datapps.linkoopdb.jdbc.navigator.RowSetNavigator;
import com.datapps.linkoopdb.jdbc.result.ResultMetaData;
import com.datapps.linkoopdb.jdbc.rowio.RowInputInterface;
import com.datapps.linkoopdb.jdbc.rowio.RowOutputInterface;
import com.datapps.linkoopdb.jdbc.types.RowType;

public class RowSetNavigatorWrappedRowSet implements RowSet {

    private RowSetNavigator navigator;
    private RowType rowType;
    private String[] columns;

    public RowSetNavigatorWrappedRowSet() {
    }

    public RowSetNavigatorWrappedRowSet(RowSetNavigator navigator, RowType rowType, OrderedHashSet columns) {
        this.navigator = navigator;
        this.rowType = rowType;
        this.columns = LinkoopDBCollectionUtils.collectBy(columns, (col) -> ((LdbSqlNameManager.LdbSqlName) col).getNameString(), String.class);
    }

    public RowSetNavigatorWrappedRowSet(RowSetNavigator navigator, RowType rowType, String[] columns) {
        this.navigator = navigator;
        this.navigator.reset();
        this.rowType = rowType;
        this.columns = columns;
    }

    public long getId() {
        return navigator.getId();
    }

    public void setId(long id) {
        navigator.setId(id);
    }

    @Override
    public Object[] getCurrent() {
        return navigator.getCurrent();
    }

    public void setCurrent(Object[] data) {
        navigator.setCurrent(data);
    }

    @Override
    public Object getField(int i) {
        return navigator.getField(i);
    }

    @Override
    public Row getCurrentRow() {
        return navigator.getCurrentRow();
    }

    public void add(Object[] data) {
        navigator.add(data);
    }

    public boolean addRow(Row row) {
        return navigator.addRow(row);
    }

    @Override
    public void removeCurrent() {
        navigator.removeCurrent();
    }

    public void reset() {
        navigator.reset();
    }

    public void clear() {
        navigator.clear();
    }

    @Override
    public void release() {
        navigator.release();
    }

    public boolean isClosed() {
        return navigator.isClosed();
    }

    public SessionInterface getSession() {
        return navigator.getSession();
    }

    public void setSession(SessionInterface session) {
        navigator.setSession(session);
    }

    public int getSize() {
        return navigator.getSize();
    }

    public boolean isEmpty() {
        return navigator.isEmpty();
    }

    @Override
    public boolean next() {
        return navigator.next();
    }

    @Override
    public long getRowId() {
        return navigator.getRowId();
    }

    public boolean hadNext() {
        return navigator.hadNext();
    }

    public boolean beforeFirst() {
        return navigator.beforeFirst();
    }

    public boolean afterLast() {
        return navigator.afterLast();
    }

    public boolean first() {
        return navigator.first();
    }

    public boolean last() {
        return navigator.last();
    }

    public int getRowNumber() {
        return navigator.getRowNumber();
    }

    public boolean absolute(int position) {
        return navigator.absolute(position);
    }

    public boolean relative(int rows) {
        return navigator.relative(rows);
    }

    public boolean previous() {
        return navigator.previous();
    }

    public boolean isFirst() {
        return navigator.isFirst();
    }

    public boolean isLast() {
        return navigator.isLast();
    }

    public boolean isBeforeFirst() {
        return navigator.isBeforeFirst();
    }

    public boolean isAfterLast() {
        return navigator.isAfterLast();
    }

    public void writeSimple(RowOutputInterface out, ResultMetaData meta) {
        navigator.writeSimple(out, meta);
    }

    public void readSimple(RowInputInterface in, ResultMetaData meta) {
        navigator.readSimple(in, meta);
    }

    public void write(RowOutputInterface out, ResultMetaData meta) {
        navigator.write(out, meta);
    }

    public void read(RowInputInterface in, ResultMetaData meta) {
        navigator.read(in, meta);
    }

    public boolean isMemory() {
        return navigator.isMemory();
    }

    public int getRangePosition() {
        return navigator.getRangePosition();
    }

    @Override
    public RowType getRowType() {
        return rowType;
    }

    @Override
    public String[] getColumns() {
        return columns;
    }
}
