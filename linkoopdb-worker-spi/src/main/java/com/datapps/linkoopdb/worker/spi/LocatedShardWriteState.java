package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;

public class LocatedShardWriteState implements Serializable {
    private static final long serialVersionUID = -6673828395491403999L;
    long shardId;
    String storageId;
    int state;
    String message;

    public LocatedShardWriteState() {
        //do noting
    }

    public LocatedShardWriteState(String storageId, long shardId, int state, String message) {
        this.storageId = storageId;
        this.shardId = shardId;
        this.state = state;
        this.message = message;
    }

    public LocatedShardWriteState(String storageId, long shardId, String message) {
        this(storageId, shardId, LocatedShard.STATE_WAIT_FOR_UPDATE, message);
    }

    public long getShardId() {
        return shardId;
    }

    public void setShardId(long shardId) {
        this.shardId = shardId;
    }

    public String getStorageId() {
        return storageId;
    }

    public void setStorageId(String storageId) {
        this.storageId = storageId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public int hashCode() {
        return (this.storageId + this.shardId).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof LocatedShardWriteState)) {
            return false;
        } else {
            LocatedShardWriteState lsws = (LocatedShardWriteState) obj;
            return this.storageId.equals(lsws.storageId)
                && this.shardId == lsws.shardId;
        }
    }
}
