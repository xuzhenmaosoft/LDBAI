package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;

public class Shard implements Cloneable, Serializable {

    public transient String tableName;
    long shardId;
    long maxSn;
    String tableId;     //其实存的是table name的uuid，该值在alter table name时不会修改
    LocatedShard[] locs;
    String ownSchema;   //该shard所属的表所在的schema
    transient long oriShardId;  //shard分裂用，记载分裂前的shardId

    public long getShardId() {
        return shardId;
    }

    public void setShardId(long shardId) {
        this.shardId = shardId;
    }

    public long getMaxSn() {
        return maxSn;
    }

    public void setMaxSn(long maxSn) {
        this.maxSn = maxSn;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public LocatedShard[] getLocs() {
        return locs;
    }

    public void setLocs(LocatedShard[] locs) {
        this.locs = locs;
    }

    public String fetchNodeUrl() {
        DataNodeInfo dataNodeInfo = locs[0].getDataNodeInfo();
        return dataNodeInfo.getDomain() + ":" + dataNodeInfo.getGrpcPort();
    }


    public String getOwnSchema() {
        return ownSchema;
    }

    public void setOwnSchema(String ownSchema) {
        this.ownSchema = ownSchema;
    }

    public long getOriShardId() {
        return oriShardId;
    }

    public void setOriShardId(long oriShardId) {
        this.oriShardId = oriShardId;
    }

    //使用新的shardid clone一份新的shard，只有shardid 和 locs和原来不同
    public Shard cloneWithNewId(long newShardId) throws CloneNotSupportedException {
        Shard s = (Shard) super.clone();
        s.oriShardId = s.shardId;
        s.shardId = newShardId;
        s.locs = new LocatedShard[this.locs.length];
        for (int i = 0; i < locs.length; i++) {
            s.locs[i] = this.locs[i].cloneWithNewId(newShardId);
        }
        return s;
    }
}
