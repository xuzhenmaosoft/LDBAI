package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.HashMap;
import java.util.List;

/**
 * Time： 2020-7-15 10:42
 * Desc: dfs作为存储引擎时对表索引做的操作，在server和worker端进行rpc通信，
 * 主要调用parquet-index中的创建索引接口,此处定义索引操作类型
 *
 * @Author: 一钢
 */
public class IndexRpcMessage implements RpcMessage {

    public static final int Append = 1;
    public static final int Overwrite = 2;
    public static final int ErrorIfExists = 3;
    public static final int Ignore = 4;

    private String tableName;
    private String idxName;
    private List<String> idxNames;
    private List<String> tableChunkPaths;
    private List<String> idxColNames;
    private String tablePath;
    private int saveMode;
    private HashMap prop;
    private String fpp;

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.DFS_INDEX_ACTION;
    }

    private IndexRpcMessageType indexRpcMessageType;

    public enum IndexRpcMessageType {
        /**
         * 创建索引
         */
        CREATE_INDEX,
        /**
         * 删除索引
         */
        DROP_INDEX,
        /**
         * 维护索引，包括表插入，更新，删除时索引的维护
         */
        UPDATE_INDEX,
        /**
         * 插入数据后的增量创建索引
         */
        INCR_CREATE_INDEX
    }

    private IndexRpcMessage() {
    }

    public static IndexRpcMessage buildCreateIndexAction(List<String> idxNames, HashMap prop,
            List<String> tableChunkPaths, List<String> idxColNames, String tablePath, int saveMode, String fpp) {
        IndexRpcMessage indexRpcMessage = new IndexRpcMessage();
        indexRpcMessage.indexRpcMessageType = IndexRpcMessageType.CREATE_INDEX;
        indexRpcMessage.idxColNames = idxColNames;
        indexRpcMessage.idxNames = idxNames;
        indexRpcMessage.prop = prop;
        indexRpcMessage.tableChunkPaths = tableChunkPaths;
        indexRpcMessage.tablePath = tablePath;
        indexRpcMessage.saveMode = saveMode;
        indexRpcMessage.fpp = fpp;
        return indexRpcMessage;
    }

    public static IndexRpcMessage buildDropIndexAction( String tableName, String indexName,String tablePath) {
        IndexRpcMessage indexRpcMessage = new IndexRpcMessage();
        indexRpcMessage.indexRpcMessageType = IndexRpcMessageType.DROP_INDEX;
        indexRpcMessage.tablePath = tablePath;
        indexRpcMessage.idxName = indexName;
        indexRpcMessage.tableName = tableName;
        return indexRpcMessage;
    }

    public static IndexRpcMessage buildIncrementalCreateIndexAction(List<String> tableChunkPaths) {
        IndexRpcMessage indexRpcMessage = new IndexRpcMessage();
        indexRpcMessage.indexRpcMessageType = IndexRpcMessageType.INCR_CREATE_INDEX;
        indexRpcMessage.tableChunkPaths = tableChunkPaths;
        return indexRpcMessage;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getIdxNames() {
        return idxNames;
    }

    public String getIdxName() {
        return idxName;
    }

    public List<String> getTableChunkPaths() {
        return tableChunkPaths;
    }

    public String getTablePath() {
        return tablePath;
    }

    public List<String> getIdxColNames() {
        return idxColNames;
    }

    public int getSaveMode() {
        return saveMode;
    }

    public IndexRpcMessageType getIndexActionType() {
        return indexRpcMessageType;
    }

    public HashMap getProp() {
        return prop;
    }

    public String getFpp() {
        return fpp;
    }
}
