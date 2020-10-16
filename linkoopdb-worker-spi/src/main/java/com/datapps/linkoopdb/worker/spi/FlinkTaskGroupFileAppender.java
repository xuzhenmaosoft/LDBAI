package com.datapps.linkoopdb.worker.spi;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;

/**
 * flink logs use.
 */
public class FlinkTaskGroupFileAppender extends TaskGroupFileAppender {

    @Override
    public void activateOptions() {

        //初始化　spark公共日志
        if(fileName != null) {
            String sparkLogFileName = fileName + File.separator + "flink-common" + File.separator + "ldb-worker.log";
            try {
                sparkCommonLogAppender = new DailyRollingFileAppender(layout, sparkLogFileName, "'.'yyyy-MM-dd");
            } catch (IOException e) {
                errorHandler.error("setFile(" + fileName + "," + fileAppend + ") call failed.",
                    e, ErrorCode.FILE_OPEN_FAILURE);
            }
        } else {
            //LogLog.error("File option not set for appender ["+name+"].");
            LogLog.warn("File option not set for appender ["+name+"].");
            LogLog.warn("Are you using FileAppender instead of ConsoleAppender?");
        }
    }

}
