/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;

import com.datapps.linkoopdb.jdbc.types.Type;

public class LayeredChunkAction implements Serializable {

    public static final int INVALID = 0;
    public static final int INSERT = 1; // placeholder never used
    public static final int UPDATE = 2;
    public static final int DELETE = 3;
    public static final int ADDCOLUMN = 4;
    public static final int DROPCOLUMN = 5;
    public static final int RENAMECOLUMN = 6;
    public static final int RETYPECOLUMN = 7;
    public static final int OVERWRITE = 8;

    public static final int RENAMETABLE = 9;

    private long id;
    private long cid = -1;
    private int actionType = INVALID;
    private Object filter;

    // update columns
    private String[] columns;

    // update expressions with the same length to columns
    // it real type is SExpression[]
    private Object expressions;

    // alter column
    private int colIndex;
    private String newColumnName;
    private Type newColumnType;
    private String oldColumnName;
    private Type oldColumnType;

    public LayeredChunkAction() {
    }

    public LayeredChunkAction(int actionType) {
        this.actionType = actionType;
    }

    public static LayeredChunkAction buildOverwriteAction(long id, long cid) {
        LayeredChunkAction action = new LayeredChunkAction(OVERWRITE);
        action.id = id;
        action.cid = cid;
        return action;
    }

    public static LayeredChunkAction buildDeleteAction(long id, long cid, Object filter) {
        LayeredChunkAction action = new LayeredChunkAction(DELETE);
        action.filter = filter;
        action.cid = cid;
        action.id = id;
        return action;
    }

    public static LayeredChunkAction buildUpdateAction(long id, long cid, String[] columns, Object expressions, Object filter) {
        LayeredChunkAction action = new LayeredChunkAction(UPDATE);
        action.columns = columns;
        action.expressions = expressions;
        action.filter = filter;
        action.cid = cid;
        action.id = id;
        return action;
    }

    public static LayeredChunkAction buildAddColumnAction(long id, long cid, int colIndex, String newColumnName, Type newColumnType) {
        LayeredChunkAction action = new LayeredChunkAction(ADDCOLUMN);
        action.colIndex = colIndex;
        action.newColumnName = newColumnName;
        action.newColumnType = newColumnType;
        action.cid = cid;
        action.id = id;
        return action;
    }

    public static LayeredChunkAction buildDropColumnAction(long id, long cid, int colIndex, String oldColumnName, Type oldColumnType) {
        LayeredChunkAction action = new LayeredChunkAction(DROPCOLUMN);
        action.colIndex = colIndex;
        action.oldColumnName = oldColumnName;
        action.oldColumnType = oldColumnType;
        action.cid = cid;
        action.id = id;
        return action;
    }

    public static LayeredChunkAction buildRenameColumnAction(long id, long cid, int colIndex, String newColumnName, String oldColumnName) {
        LayeredChunkAction action = new LayeredChunkAction(RENAMECOLUMN);
        action.colIndex = colIndex;
        action.newColumnName = newColumnName;
        action.oldColumnName = oldColumnName;
        action.cid = cid;
        action.id = id;
        return action;
    }

    public static LayeredChunkAction buildRetypeColumnAction(long id, long cid, int colIndex, Type newColumnType, String oldColumnName, Type oldColumnType) {
        LayeredChunkAction action = new LayeredChunkAction(RETYPECOLUMN);
        action.colIndex = colIndex;
        action.newColumnType = newColumnType;
        action.oldColumnName = oldColumnName;
        action.oldColumnType = oldColumnType;
        action.cid = cid;
        action.id = id;
        return action;
    }

    public static LayeredChunkAction buildAlterColumnAction(long id, long cid, int colIndex, String newColumnName, Type newColumnType, String oldColumnName,
        Type oldColumnType) {
        LayeredChunkAction action;
        if (oldColumnName == null) {
            action = new LayeredChunkAction(ADDCOLUMN);
        } else if (newColumnName == null && newColumnType == null) {
            action = new LayeredChunkAction(DROPCOLUMN);
        } else if (newColumnName == null) {
            action = new LayeredChunkAction(RETYPECOLUMN);
        } else if (newColumnType == null) {
            action = new LayeredChunkAction(RENAMECOLUMN);
        } else {
            throw new RuntimeException("Illegal column operation");
        }
        action.colIndex = colIndex;
        action.newColumnName = newColumnName;
        action.newColumnType = newColumnType;
        action.oldColumnName = oldColumnName;
        action.oldColumnType = oldColumnType;
        action.cid = cid;
        action.id = id;
        return action;
    }

    public int getActionType() {
        return actionType;
    }

    public void setActionType(int actionType) {
        this.actionType = actionType;
    }

    public Object getFilter() {
        return filter;
    }

    public void setFilter(Object filter) {
        this.filter = filter;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public Object getExpressions() {
        return expressions;
    }

    public void setExpressions(Object expressions) {
        this.expressions = expressions;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getNewColumnName() {
        return newColumnName;
    }

    public void setNewColumnName(String newColumnName) {
        this.newColumnName = newColumnName;
    }

    public Type getNewColumnType() {
        return newColumnType;
    }

    public void setNewColumnType(Type newColumnType) {
        this.newColumnType = newColumnType;
    }

    public String getOldColumnName() {
        return oldColumnName;
    }

    public void setOldColumnName(String oldColumnName) {
        this.oldColumnName = oldColumnName;
    }

    public Type getOldColumnType() {
        return oldColumnType;
    }

    public void setOldColumnType(Type oldColumnType) {
        this.oldColumnType = oldColumnType;
    }

    public int getColIndex() {
        return colIndex;
    }

    public void setColIndex(int colIndex) {
        this.colIndex = colIndex;
    }

    public long getCid() {
        return cid;
    }

    public void setCid(long cid) {
        this.cid = cid;
    }
}
