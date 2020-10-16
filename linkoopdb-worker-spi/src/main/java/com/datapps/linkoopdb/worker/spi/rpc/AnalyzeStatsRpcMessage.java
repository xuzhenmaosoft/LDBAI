package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;

/**
 * Created by gloway on 2019/9/3.
 */
public class AnalyzeStatsRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.TABLE_STATISTICS;
    }

    private SubmitInfo submitInfo;
    private RelNode relNode;
    private String[] columns;
    private boolean withHistogram;
    private String tableId;
    private String name;

    public static AnalyzeStatsRpcMessage buildAnalyzeStatsMessage(SubmitInfo submitInfo, RelNode relNode,
        String[] columns, boolean withHistogram, String tableId, String name) {
        AnalyzeStatsRpcMessage analyzeStatsRpcMessage = new AnalyzeStatsRpcMessage();
        analyzeStatsRpcMessage.submitInfo = submitInfo;
        analyzeStatsRpcMessage.relNode = relNode;
        analyzeStatsRpcMessage.columns = columns;
        analyzeStatsRpcMessage.withHistogram = withHistogram;
        analyzeStatsRpcMessage.tableId = tableId;
        analyzeStatsRpcMessage.name = name;
        return analyzeStatsRpcMessage;
    }

    private AnalyzeStatsRpcMessage() {

    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public String[] getColumns() {
        return columns;
    }

    public boolean isWithHistogram() {
        return withHistogram;
    }

    public String getTableId() {
        return tableId;
    }

    public String getName() {
        return name;
    }
}
