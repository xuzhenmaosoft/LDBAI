package com.datapps.linkoopdb.worker.spi.fs;

public class HttpFileSystem extends AbstractHttpFileSystem {

    @Override
    public String getScheme() {
        return "http";
    }
}
