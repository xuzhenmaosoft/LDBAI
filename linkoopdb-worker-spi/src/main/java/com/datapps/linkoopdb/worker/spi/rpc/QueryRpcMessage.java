package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.stream.WorkerSession;

/**
 * Created by gloway on 2019/9/3.
 */
public class QueryRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.QUERY;
    }

    public enum QueryMessageType {
        QUERY_SQL, FUNCTION_CALL, COUNT, HIVE_SQL, SHOW_PLAN, SNAPSHOT_RESULT_COUNT, RETRIEVE_RESULT_CHANGES, RETRIEVE_RESULT_PAGE
    }

    private QueryMessageType queryMessageType;

    private SubmitInfo submitInfo;
    protected RelNode relNode;

    private RexNode rexNode;

    protected String sql;

    protected int showType;

    public static QueryRpcMessage buildQueryMessage(SubmitInfo submitInfo, RelNode relNode) {
        QueryRpcMessage batchRpcMessage = new QueryRpcMessage();
        batchRpcMessage.queryMessageType = QueryMessageType.QUERY_SQL;
        batchRpcMessage.submitInfo = submitInfo;
        batchRpcMessage.relNode = relNode;
        return batchRpcMessage;
    }

    public static QueryRpcMessage buildQueryFunctionMessage(SubmitInfo submitInfo, RexNode rexNode) {
        QueryRpcMessage batchRpcMessage = new QueryRpcMessage();
        batchRpcMessage.queryMessageType = QueryMessageType.FUNCTION_CALL;
        batchRpcMessage.submitInfo = submitInfo;
        batchRpcMessage.rexNode = rexNode;
        return batchRpcMessage;
    }

    public static QueryRpcMessage buildCountMessage(SubmitInfo submitInfo, RelNode relNode) {
        QueryRpcMessage batchRpcMessage = new QueryRpcMessage();
        batchRpcMessage.queryMessageType = QueryMessageType.COUNT;
        batchRpcMessage.submitInfo = submitInfo;
        batchRpcMessage.relNode = relNode;
        return batchRpcMessage;
    }

    public static QueryRpcMessage buildQueryHiveSqlMessage(String sql) {
        QueryRpcMessage batchRpcMessage = new QueryRpcMessage();
        batchRpcMessage.queryMessageType = QueryMessageType.HIVE_SQL;
        batchRpcMessage.sql = sql;
        return batchRpcMessage;
    }

    public static QueryRpcMessage buildShowPlanMessage(SubmitInfo submitInfo, RelNode relNode, int showType) {
        QueryRpcMessage batchRpcMessage = new QueryRpcMessage();
        batchRpcMessage.queryMessageType = QueryMessageType.SHOW_PLAN;
        batchRpcMessage.submitInfo = submitInfo;
        batchRpcMessage.relNode = relNode;
        batchRpcMessage.showType = showType;
        return batchRpcMessage;
    }

    public static StreamQueryRpcMessage buildStreamQueryRpcMessage(WorkerSession streamSession,
        QueryMessageType messageType, SubmitInfo submitInfo) {
        return new StreamQueryRpcMessage(streamSession, messageType, submitInfo);
    }

    public static StreamQueryRpcMessage buildStreamQueryRpcMessage(WorkerSession streamSession,
        QueryMessageType messageType, SubmitInfo submitInfo, RelNode relNode) {
        return new StreamQueryRpcMessage(streamSession, messageType, submitInfo, relNode);
    }

    public static StreamQueryRpcMessage buildPlanStreamQueryRpcMessage(WorkerSession streamSession,
        RelNode relNode, int showType) {

        StreamQueryRpcMessage message = new StreamQueryRpcMessage(streamSession, QueryMessageType.SHOW_PLAN,
            null, relNode);
        message.showType = showType;
        return message;
    }

    private QueryRpcMessage() {
    }

    protected QueryRpcMessage(QueryMessageType messageType, SubmitInfo submitInfo) {
        this.queryMessageType = messageType;
        this.submitInfo = submitInfo;
    }

    public QueryMessageType getQueryMessageType() {
        return queryMessageType;
    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public RexNode getRexNode() {
        return rexNode;
    }

    public String getSql() {
        return sql;
    }

    public int getShowType() {
        return showType;
    }

}
