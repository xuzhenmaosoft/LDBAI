package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.jdbc.Row;
import com.datapps.linkoopdb.jdbc.types.RowType;
import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.RowSet;
import com.datapps.linkoopdb.worker.spi.SimpleRowSet;

/**
 * Created by gloway on 2019/9/4.
 */
public class QueryResultRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.QUERY_RESULT;
    }

    RowSet partResult;
    String physicalPlan;

    long count;

    public static QueryResultRpcMessage buildQueryResultMessage(RowType rowType, String[] columns, Row[] rows, String physicalPlan) {
        QueryResultRpcMessage queryResultRpcMessage = new QueryResultRpcMessage();
        queryResultRpcMessage.partResult = new SimpleRowSet(rowType, columns, rows);
        queryResultRpcMessage.physicalPlan = physicalPlan;
        return queryResultRpcMessage;
    }

    public static QueryResultRpcMessage buildCountResultMessage(long count) {
        QueryResultRpcMessage queryResultRpcMessage = new QueryResultRpcMessage();
        queryResultRpcMessage.count = count;
        return queryResultRpcMessage;
    }

    private QueryResultRpcMessage() {

    }


    public RowSet getPartResult() {
        return partResult;
    }

    public String getPhysicalPlan() {
        return physicalPlan;
    }

    public String[] getColumns() {
        return partResult.getColumns();
    }

    public Type[] getWorkerTypes() {
        if (partResult.getRowType() != null) {
            return partResult.getRowType().colTypes;
        } else {
            return null;
        }
    }

    public long getCount() {
        return count;
    }
}
