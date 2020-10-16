package com.datapps.linkoopdb.worker.spi.util.lock;

import java.util.Objects;

import org.apache.hadoop.hdfs.DFSClient;

public class Lease {

    private final Thread thread;
    private final DFSClient dfsClient;

    public Lease(Thread thread, DFSClient dfsClient) {
        this.thread = thread;
        this.dfsClient = dfsClient;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lease lease = (Lease) o;
        return Objects.equals(thread, lease.thread)
                && Objects.equals(dfsClient, lease.dfsClient);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thread, dfsClient);
    }

    @Override
    public String toString() {
        return "Lease{"
                + "thread=" + thread
                + ", dfsClient=" + dfsClient
                + '}';
    }
}
