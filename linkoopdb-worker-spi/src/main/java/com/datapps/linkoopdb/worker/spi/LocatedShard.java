package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;

public class LocatedShard implements Cloneable, Serializable {

    public static final int STATE_ALLOCING = 0;
    public static final int STATE_NORMAL = 1;               //正常状态，数据是可用的，最新的
    public static final int STATE_WAIT_FOR_UPDATE = 2;      //数据可能已经不是最新了，更待更新
    public static final int STATE_WAIT_FOR_DELETE = 4;      //该lshard已经被逻辑删除了，等待实际删除
    public static final int STATE_WAIT_FOR_COPY = 8;        //该lshard是新增加的副本，等待数据拷贝
    public static final int STATE_LOCK_FOR_COPY = 16;       //该lshard正在执行针对它的拷贝命令
    public static final int STATE_LOCK_FOR_DELETE = 32;     //该lshard正在执行针对它的删除命令
    public static final int STATE_LOCK_FOR_UPDATE = 64;     //该lshard正在执行针对它的更新命令
    public static final int STATE_LOCK_FOR_MOVE = 128;      //该lshard正在执行针对它的移动命令
    public static final int STATE_BEING_MOVED_TO = 256;     //该lshard为正在执行移动命令的lshard的目标lshard，只有在移动命令实际执行时才会创建出来

    public static final int STATE_FOR_ALIGN = 1024;

    public static final int NODE_PING_STATE_ONLINE = 1;     //表示locatedshard所在节点的当前状态，为在线
    public static final int NODE_PING_STATE_OFFLINE = 2;    //表示locatedshard所在节点的当前状态，为下线

    public boolean main = false;
    public transient int inCommand = -1;        //代表了当前shard处于哪种命令下，-1为不在命令队列中
    public transient long nodePingState = NODE_PING_STATE_OFFLINE;           //代表了当前located shard所在节点的当前状态，维护在内存中，并不持久化

    public transient String tableName;
    public transient String tableUuid;
    public transient String tableSchema;

    long shardId;
    String name;
    int state = STATE_ALLOCING;
    String storageNodeId;
    DataNodeInfo dataNodeInfo;

    public long getShardId() {
        return shardId;
    }

    public void setShardId(long shardId) {
        this.shardId = shardId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public DataNodeInfo getDataNodeInfo() {
        return dataNodeInfo;
    }

    public void setDataNodeInfo(DataNodeInfo dataNodeInfo) {
        this.dataNodeInfo = dataNodeInfo;
        if (dataNodeInfo != null) {
            this.storageNodeId = dataNodeInfo.getStorageNodeId();
        }
    }

    public boolean isMain() {
        return this.main;
    }

    public void setMain(boolean main) {
        this.main = main;
    }

    public String getStorageNodeId() {
        return storageNodeId;
    }

    public String getDomainAndPort() {
        return this.dataNodeInfo.domain + ":" + this.dataNodeInfo.grpcPort;
    }

    public void setStorageNodeId(String storageNodeId) {
        this.storageNodeId = storageNodeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocatedShard clone() {
        LocatedShard lshard = new LocatedShard();
        lshard.setName(this.name);
        lshard.setStorageNodeId(this.storageNodeId);
        lshard.setShardId(this.shardId);
        lshard.setMain(this.main);
        lshard.nodePingState = this.nodePingState;
        lshard.setState(this.state);
        lshard.setDataNodeInfo(this.dataNodeInfo);
        return lshard;
    }

    //使用新的shardid clone一份新的LocatedShard，只有shardid和原来不同
    LocatedShard cloneWithNewId(long newShardId) throws CloneNotSupportedException {
        LocatedShard l = (LocatedShard) super.clone();
        l.setShardId(newShardId);
        return l;
    }

    public static String getStateName(int state) {
        switch (state) {
            case STATE_NORMAL:
                return "normal";
            case STATE_WAIT_FOR_UPDATE:
                return "wait_for_update";
            case STATE_WAIT_FOR_DELETE:
                return "wait_for_delete";
            case STATE_WAIT_FOR_COPY:
                return "wait_for_copy";
            case STATE_LOCK_FOR_COPY:
                return "lock_for_copy";
            case STATE_LOCK_FOR_DELETE:
                return "lock_for_delete";
            case STATE_LOCK_FOR_UPDATE:
                return "lock_for_update";
            case STATE_LOCK_FOR_MOVE:
                return "lock_for_move";
            case STATE_BEING_MOVED_TO:
                return "being_moved_to";
            default:
                return "other";
        }
    }
}
