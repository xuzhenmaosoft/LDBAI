/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import com.datapps.linkoopdb.jdbc.Row;
import com.datapps.linkoopdb.jdbc.types.RowType;

public class SimpleRowSet implements RowSet {

    RowType rowType;
    String[] columns;
    Row[] rows;
    int i = 0;

    public SimpleRowSet(RowType rowType, String[] columns, Row[] rows) {
        this.rowType = rowType;
        this.columns = columns;
        this.rows = rows;
    }

    @Override
    public RowType getRowType() {
        return rowType;
    }

    @Override
    public String[] getColumns() {
        return columns;
    }

    @Override
    public Object getField(int col) {
        return rows[i].getField(col);
    }

    @Override
    public boolean next() {
        if (i < rows.length - 1) {
            i++;
            return true;
        }
        return false;
    }

    @Override
    public Row getCurrentRow() {
        return rows[i];
    }

    @Override
    public Object[] getCurrent() {
        return rows[i].getData();
    }

    @Override
    public void removeCurrent() {

    }

    @Override
    public void release() {

    }

    @Override
    public long getRowId() {
        return 0;
    }

    public Row[] getRows() {
        return rows;
    }
}
