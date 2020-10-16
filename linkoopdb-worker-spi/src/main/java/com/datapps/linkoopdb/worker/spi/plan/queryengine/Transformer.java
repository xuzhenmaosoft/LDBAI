package com.datapps.linkoopdb.worker.spi.plan.queryengine;

import com.datapps.linkoopdb.worker.spi.plan.TreeNode;

@FunctionalInterface
public interface Transformer<T extends TreeNode> {

    T execute(T treeNode);
}
