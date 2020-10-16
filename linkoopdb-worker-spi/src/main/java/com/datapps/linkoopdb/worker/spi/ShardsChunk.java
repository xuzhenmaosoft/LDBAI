package com.datapps.linkoopdb.worker.spi;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datapps.linkoopdb.worker.spi.plan.core.LdbPallasSource;

public class ShardsChunk extends StorageChunk {

    List<Shard> shardList = new ArrayList<>();
    int[] primaryKey;
    List<Integer> originPrimaryKey;
    String originTableName;
    // 此pallas表是否是流表标记
    boolean stream = false;

    // 表示pallas表是否采用字典编码压缩
    boolean isDictCompress = false;

    int minNumDuplicateMustSuccess = 0;

    private int pallasStorageOrientation = LdbPallasSource.PALLAS_ROW_STORAGE;

    private boolean isNewKeyRule = false;

    public ShardsChunk(String chunk) {
        this.setChunk(chunk);
    }

    public List<Shard> getShardList() {
        return shardList;
    }

    public void setShardList(List<Shard> shardList) {
        this.shardList = shardList;
    }

    public int[] getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(int[] primaryKeyPosition) {
        this.primaryKey = primaryKeyPosition;
    }

    public List<Integer> getOriginPrimaryKey() {
        return originPrimaryKey;
    }

    public void setOriginPrimaryKey(List<Integer> originPrimaryKeyPositon) {
        this.originPrimaryKey = originPrimaryKeyPositon;
    }

    public String getOriginTableName() {
        return this.originTableName;
    }

    public void setOriginTableName(String originTableName) {
        this.originTableName = originTableName;
    }

    public void setStream(boolean stream) {
        this.stream = stream;
    }

    public boolean isStream() {
        return stream;
    }

    public boolean isDictCompress() {
        return isDictCompress;
    }

    public void setDictCompress(boolean dictCompress) {
        isDictCompress = dictCompress;
    }

    public int getMinNumDuplicateMustSuccess() {
        return minNumDuplicateMustSuccess;
    }

    public void setMinNumDuplicateMustSuccess(int minNumDuplicateMustSuccess) {
        this.minNumDuplicateMustSuccess = minNumDuplicateMustSuccess;
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

    public int getPallasStorageOrientation() {
        return pallasStorageOrientation;
    }

    public void  setPallasStorageOrientation(int orientation) {
        this.pallasStorageOrientation = orientation;
    }

    public boolean isNewKeyRule() {
        return isNewKeyRule;
    }

    public void setNewKeyRule(boolean newKeyRule) {
        isNewKeyRule = newKeyRule;
    }
}
