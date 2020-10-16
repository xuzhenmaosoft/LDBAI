package com.datapps.linkoopdb.worker.spi.plan.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.LocatedShard;
import com.datapps.linkoopdb.worker.spi.LocatedShardWriteState;
import com.datapps.linkoopdb.worker.spi.Shard;

public class LdbPallasSource extends LdbSource implements Serializable {

    public static final String PALLASID = "_LDBID_";
    public static final int PALLAS_ROW_STORAGE = 0;
    public static final int PALLAS_COLUMN_STORAGE = 1;
    public long count;
    private String tableName;
    private List<Shard> shardList;
    private String txnId;
    private long sn = 0;
    private LinkedHashMap<String, Integer> columnAndTypes;
    private List<String> columnList = new ArrayList<>();
    //序列化为json之前colTypes必须set为null，否则序列化失败
    private Type[] colTypes;
    private List<String> primaryKeys;
    private List<Integer> primaryKeyPos;
    private Map<String, List<Integer>> indexs = Collections.emptyMap();
    private String snapshot;
    private String[] partitionColumns;
    private int[] fileToBinaryColumn;
    private int[] blobCols;
    private boolean isForUpdate = false;
    private long txnSize = 0L;
    private long perInsertSize = 0L;
    private long perIngestSize = 0L;
    private boolean isDictCompress = false;
    private boolean isReadForLoad = false; // 读数据是否为了执行load语句
    private Set<String> pallasSourceBlobToBinaryFields;
    private Set<LocatedShardWriteState> writeStates;
    public int minNumDuplicateMustSuccess;         //执行dml必须成功的最小副本数
    private int pallasStorageOrientation;
    private WriteMode writeMode;
    private int writeMaxBufferSize;
    private ReadMode readMode;
    private boolean isLoadInTxn = false;
    private boolean hasOriginPK = false;
    private boolean isNewKeyRule = false;

    public LdbPallasSource(List<Shard> shardList, Long sn, String txnId,
        String tableName, String snapshot, List<String> pkList,
        Map<String, List<Integer>> indexs, int minNumDuplicateMustSuccess) {
        this.shardList = shardList;
        this.sn = sn;
        this.txnId = txnId;
        this.tableName = tableName;
        this.snapshot = snapshot;
        this.primaryKeys = pkList;
        this.indexs = (indexs == null ? this.indexs : indexs);
        this.minNumDuplicateMustSuccess = minNumDuplicateMustSuccess;
    }

    public LdbPallasSource() {

    }

    public static Map<Long, List<String>> rowListSplitToShards(List<Long> shardIds, List<String> rowList) {
        Map<Long, List<String>> res = new HashMap<>();
        for (long shardId : shardIds) {
            res.put(shardId, new ArrayList<>());
        }
        int shardCount = shardIds.size();
        for (int i = 0; i < rowList.size(); i++) {
            res.get(shardIds.get(i % shardCount)).add(rowList.get(i));
        }

        return res;
    }

    public List<List<String>> getStorageNodeIdWithDuplicate() {
        List<List<String>> lists = new ArrayList<>();
        if (!shardList.isEmpty()) {
            for (Shard shard : shardList) {
                List<String> locs = new ArrayList<>();
                for (LocatedShard loc : shard.getLocs()) {
                    locs.add(loc.getDataNodeInfo().getDomain() + ":" + loc.getDataNodeInfo().getGrpcPort());
                }
                lists.add(locs);
            }
        }
        return lists;
    }

    public Map<String, Map<Long, String[]>> calculateNodeIdAndShardOfTable() throws Exception {
        Map<String, Map<Long, String[]>> nodeIdToShardId = new LinkedHashMap<>();
        for (Shard shard : shardList) {
            if (shard.getLocs() == null || shard.getLocs().length == 0) {
                throw new Exception("Something wrong");
            }
            // 传递来的第一个locatedshard就是主shard
            // if (!shard.getLocs()[0].isMain) {
            //     throw new Exception("有问题");
            // }
            String mainHostPort = shard.getLocs()[0].getDataNodeInfo().getDomain() + ":"
                + shard.getLocs()[0].getDataNodeInfo().getGrpcPort();

            if (!nodeIdToShardId.containsKey(mainHostPort)) {
                nodeIdToShardId.put(mainHostPort, new HashMap<>());
            }

            Map<Long, String[]> shardIdDupStorageId = nodeIdToShardId.get(mainHostPort);
            String[] dupStorageIds = new String[shard.getLocs().length - 1];
            if (shard.getLocs().length > 1) {
                for (int i = 1; i < shard.getLocs().length; i++) {
                    dupStorageIds[i - 1] = shard.getLocs()[i].getDataNodeInfo().getDomain() + ":"
                        + shard.getLocs()[i].getDataNodeInfo().getGrpcPort();
                }
            }
            shardIdDupStorageId.put(shard.getShardId(), dupStorageIds);
        }

        return nodeIdToShardId;
    }

    public Set<String> getStorageNodeIdSet() {
        Set<String> nodeIdSet = new HashSet<>();
        for (Shard shard : shardList) {
            for (LocatedShard loc : shard.getLocs()) {
                String nodeId = loc.getDataNodeInfo().getDomain() + ":" + loc.getDataNodeInfo().getGrpcPort();
                nodeIdSet.add(nodeId);
            }
        }
        return nodeIdSet;
    }

    public Long getSn() {
        return sn;
    }

    public void setSn(Long sn) {
        this.sn = sn;
    }

    public List<Shard> getShardList() {
        return shardList;
    }

    public void setShardList(List<Shard> shardList) {
        this.shardList = shardList;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTxnId() {
        return this.txnId;
    }

    public void setTxnId(String txnId) {
        this.txnId = txnId;
    }

    public LinkedHashMap<String, Integer> getColumnAndTypes() {
        return this.columnAndTypes;
    }

    public void setColumnAndTypes(String[] cols, Type[] types) {
        LinkedHashMap<String, Integer> columnAndTypes = new LinkedHashMap<>();
        if (cols.length != types.length) {
            throw new RuntimeException("column's length is not matching with columnType's length!");
        }
        for (int i = 0; i < cols.length; i++) {
            columnAndTypes.put(cols[i], types[i].getJDBCTypeCode());
        }
        this.columnAndTypes = columnAndTypes;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public void setColumnList(String[] columns) {
        for (String col : columns) {
            columnList.add(col);
        }
    }

    public List<String> getPrimaryKeys() {
        return this.primaryKeys;
    }

    public void setPrimaryKeys(List<String> pks) {
        this.primaryKeys = pks;
    }

    public List<Integer> getPrimaryKeyPos() {
        return this.primaryKeyPos;
    }

    public void setPrimaryKeyPos(List<Integer> primaryKeyPos) {
        this.primaryKeyPos = primaryKeyPos;
    }

    public Map<String, List<Integer>> getIndexs() {
        return this.indexs;
    }

    public void setIndexs(Map<String, List<Integer>> indexs) {
        this.indexs = indexs;
    }

    public String[] getPartitionColumns() {
        return this.partitionColumns;
    }

    public void setPartitionColumns(String[] partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }

    public String toJson() {
        try {
            Gson gson = new Gson();
            return gson.toJson(this);
        } catch (Exception e) {
            throw new RuntimeException("Serialize PallasTable failed! ", e);
        }
    }

    public long getTxnSize() {
        return txnSize;
    }

    public void setTxnSize(long txnSize) {
        this.txnSize = txnSize;
    }

    public long getPerInsertSize() {
        return perInsertSize;
    }

    public void setPerInsertSize(long perInsertSize) {
        this.perInsertSize = perInsertSize;
    }

    public long getPerIngestSize() {
        return perIngestSize;
    }

    public void setPerIngestSize(long perIngestSize) {
        this.perIngestSize = perIngestSize;
    }

    public Type[] getColTypes() {
        return colTypes;
    }

    public void setColTypes(Type[] colTypes) {
        this.colTypes = colTypes;
    }

    public int[] getBlobCols() {
        return blobCols;
    }

    public void setBlobCols(int[] blobCols) {
        this.blobCols = blobCols;
    }

    public int[] getFileToBinaryColumn() {
        return fileToBinaryColumn;
    }

    public void setFileToBinaryColumn(int[] fileToBinaryColumn) {
        this.fileToBinaryColumn = fileToBinaryColumn;
    }

    public void setIsForUpdate(boolean isForUpdate) {
        this.isForUpdate = isForUpdate;
    }

    public boolean getIsForUpdate() {
        return isForUpdate;
    }

    public boolean isDictCompress() {
        return isDictCompress;
    }

    public void setDictCompress(boolean isDictCompress) {
        this.isDictCompress = isDictCompress;
    }

    public Set<LocatedShardWriteState> getWriteStates() {
        return writeStates;
    }

    public void setWriteStates(Set<LocatedShardWriteState> writeStates) {
        this.writeStates = writeStates;
    }

    @Override
    public String getStatistic() {
        return null;
    }

    public Set<String> getPallasSourceBlobToBinaryFields() {
        return pallasSourceBlobToBinaryFields;
    }

    public void setPallasSourceBlobToBinaryFields(Set<String> pallasSourceBlobToBinaryFields) {
        this.pallasSourceBlobToBinaryFields = pallasSourceBlobToBinaryFields;
    }

    public int getPallasStorageOrientation() {
        return pallasStorageOrientation;
    }

    public boolean isColumnStore() {
        return pallasStorageOrientation == PALLAS_COLUMN_STORAGE;
    }

    public void setPallasStorageOrientation(int pallasStorageOrientation) {
        this.pallasStorageOrientation = pallasStorageOrientation;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(WriteMode writeMode) {
        this.writeMode = writeMode;
    }

    public int getWriteMaxBufferSize() {
        return writeMaxBufferSize;
    }

    public void setWriteMaxBufferSize(int writeMaxBufferSize) {
        this.writeMaxBufferSize = writeMaxBufferSize;
    }

    public ReadMode getReadMode() {
        return readMode;
    }

    public void setReadMode(ReadMode readMode) {
        this.readMode = readMode;
    }

    public boolean isReadForLoad() {
        return isReadForLoad;
    }

    public void setReadForLoad(boolean readForLoad) {
        isReadForLoad = readForLoad;
    }

    public boolean isLoadInTxn() {
        return isLoadInTxn;
    }

    public void setLoadInTxn(boolean loadInTxn) {
        isLoadInTxn = loadInTxn;
    }

    public boolean isHasOriginPK() {
        return hasOriginPK;
    }

    public void setHasOriginPK(boolean hasOriginPK) {
        this.hasOriginPK = hasOriginPK;
    }

    public boolean isNewKeyRule() {
        return isNewKeyRule;
    }

    public void setNewKeyRule(boolean isNewKeyRule) {
        this.isNewKeyRule = isNewKeyRule;
    }

    public enum WriteMode {
        BY_ROW_NUM, BY_BUFFER_SIZE, BY_KV
    }

    public enum ReadMode {
        BY_LEGACY, BY_KV
    }
}
