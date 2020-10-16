package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Created by gloway on 2019/1/8.
 */
public abstract class SingleRel extends RelNode {

    public RelNode child;

    public SingleRel(RelNode child) {
        this.child = child;
    }

    @Override
    public final List<RelNode> getChildren() {
        return ImmutableList.of(child);
    }


}
