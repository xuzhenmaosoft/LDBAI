/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LayeredChunk implements Serializable {

    //存储引擎类型
    private int storageEngine;

    private long id;
    private long cid = -1;

    private long maxCacheId = -1;
    private StorageChunk chunk;
    private List<LayeredChunkAction> actions = new ArrayList<>();
    private LayeredChunk child;

    public LayeredChunk() {
    }

    public LayeredChunk(StorageChunk chunk, LayeredChunk child) {
        this.child = child;
        this.chunk = chunk;
    }

    public void addAction(LayeredChunkAction action) {
        actions.add(action);
    }

    public LayeredChunk getChild() {
        return child;
    }

    public void setChild(LayeredChunk child) {
        this.child = child;
    }

    public List<LayeredChunkAction> getActions() {
        return actions;
    }

    public StorageChunk getChunk() {
        return chunk;
    }

    public void setChunk(StorageChunk chunk) {
        this.chunk = chunk;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getCid() {
        return cid;
    }

    public void setCid(long cid) {
        this.cid = cid;
    }

    public long getMaxCacheId() {
        return maxCacheId;
    }

    public void setMaxCacheId(long maxCacheId) {
        this.maxCacheId = maxCacheId;
    }

    public int getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(int storageEngine) {
        this.storageEngine = storageEngine;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LayeredChunk that = (LayeredChunk) o;

        if (id != that.id) {
            return false;
        }
        if (cid != that.cid) {
            return false;
        }
        if (actions.size() != that.actions.size()) {
            return false;
        }
        return chunk != null ? chunk.equals(that.chunk) : that.chunk == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (int) (maxCacheId ^ (maxCacheId >>> 32));
        result = 31 * result + (actions.size() ^ (actions.size() >>> 32));
        result = 31 * result + (chunk != null ? chunk.hashCode() : 0);
        return result;
    }
}
