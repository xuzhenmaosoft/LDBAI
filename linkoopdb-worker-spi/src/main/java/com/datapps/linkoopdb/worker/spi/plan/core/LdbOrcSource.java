package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.UUID;

public class LdbOrcSource extends LdbSource {

    private String[] paths;

    public LdbOrcSource(String[] paths) {
        this.paths = paths;
    }

    @Override
    public String getStatistic() {
        return null;
    }

    public String[] getPaths() {
        return paths;
    }

    public void setPaths(String[] paths) {
        this.paths = paths;
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return UUID.randomUUID().hashCode();
    }
}
