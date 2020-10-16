package com.datapps.linkoopdb.worker.spi.config;

/**
 * @author yukkit.zhang
 * @date 2020/8/20
 */
public interface DataTypeValidator<T> {

    boolean doValid(T t);
}
