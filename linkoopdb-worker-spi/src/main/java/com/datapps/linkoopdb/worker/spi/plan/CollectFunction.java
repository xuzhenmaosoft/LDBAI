package com.datapps.linkoopdb.worker.spi.plan;

import java.util.Optional;

@FunctionalInterface
public interface CollectFunction<T, R> {

    Optional<R> transform(T treeNode);
}
