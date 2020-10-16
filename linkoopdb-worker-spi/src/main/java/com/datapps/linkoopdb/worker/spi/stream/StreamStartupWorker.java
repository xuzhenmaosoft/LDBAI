package com.datapps.linkoopdb.worker.spi.stream;

/**
 * flink-worker启动的rpc服务，server会推送相关信息给flink-worker，一般是启动时协调信息的。
 */
public interface StreamStartupWorker {

    boolean checkoutApplicationId(String applicationId) throws Exception;
}
