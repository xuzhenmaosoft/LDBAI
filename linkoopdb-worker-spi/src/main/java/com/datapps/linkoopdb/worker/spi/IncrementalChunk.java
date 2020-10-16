/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import com.datapps.linkoopdb.worker.spi.config.TableParameters;

/**
 * 存储引擎为hdfs时的StorageChunk的具体实现
 */
public class IncrementalChunk extends StorageChunk {

    private String path;

    public IncrementalChunk() {
    }

    public IncrementalChunk(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getWriteParallelism() {
        String writeParallelism = getParameters().get(TableParameters.writeParallelism.key());
        if (writeParallelism == null) {
            return 0;
        } else {
            return Integer.parseInt(writeParallelism);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        IncrementalChunk that = (IncrementalChunk) o;

        return path != null ? path.equals(that.path) : that.path == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (path != null ? path.hashCode() : 0);
        return result;
    }
}
