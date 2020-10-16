package com.datapps.linkoopdb.worker.spi.stream;

import java.io.Serializable;

/**
 * ldb-server中启动的rpc服务，flink-worker会调用此接口获取必要信息。
 */
public interface LdbStreamInteractive {

    // flink-worker启动时调用此接口，决定由哪个worker来启动flink on yarn
    StartupInfo fetchYarnAppId(String workerId, String workerUrl) throws Exception;
    // 将某个worker启动后（或验证合法）的flink on yarn的合法的ApplicationId推送给server
    void pushYarnAppIdToServer(String applicationId, String workerId);

    class StartupInfo implements Serializable {

        private static final long serialVersionUID = 5048479101690989718L;

        public static StartupInfo to(String workerId, String appId, STATUS_FLAG startFlag) {
            StartupInfo info = new StartupInfo();
            info.appId = appId;
            info.startFlag = startFlag;
            info.workerId = workerId;
            return info;
        }
        String workerId;
        String appId;
        STATUS_FLAG startFlag;

        public String getWorkerId() {
            return workerId;
        }

        public String getAppId() {
            return appId;
        }

        public STATUS_FLAG getStartFlag() {
            return startFlag;
        }
    }

    enum STATUS_FLAG {
        STARTED, START
    }
}
