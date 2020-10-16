/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/**
 * This class is for Hadoop 2.x FlieContext API.
 */
public class LinkoopDBLocalDelegateToFileSystem extends DelegateToFileSystem {

    protected LinkoopDBLocalDelegateToFileSystem(URI uri, Configuration conf) throws IOException, URISyntaxException {
        super(uri, new LinkoopDBLocalFileSystem(), conf, "ldb", false);
    }

    @Override
    public int getUriDefaultPort() {
        return -1;
    }
}
