package com.datapps.linkoopdb.worker.spi.plan.queryengine;

import java.util.ArrayList;
import java.util.List;

import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.Rule;


public class Batch {

    String name;
    int maxIterations;
    List<Rule> rules = new ArrayList<>();

    public Batch(String name, int maxIterations) {
        this.name = name;
        this.maxIterations = maxIterations;
    }
}
