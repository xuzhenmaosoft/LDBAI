package com.datapps.linkoopdb.worker.spi;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Created by gloway on 2019/6/13.
 */
public class TaskGroupFileAppender extends FileAppender {

    private Cache<String, FileAppender> appenderCache = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build();
    protected DailyRollingFileAppender sparkCommonLogAppender;

    @Override
    public void activateOptions() {

        //初始化　spark公共日志
        if(fileName != null) {
            String sparkLogFileName = fileName + File.separator + "spark-common" + File.separator + "ldb-worker.log";
            try {
                sparkCommonLogAppender = new DailyRollingFileAppender(layout, sparkLogFileName, "'.'yyyy-MM-dd");
            } catch (java.io.IOException e) {
                errorHandler.error("setFile(" + fileName + "," + fileAppend + ") call failed.",
                    e, ErrorCode.FILE_OPEN_FAILURE);
            }
        } else {
            //LogLog.error("File option not set for appender ["+name+"].");
            LogLog.warn("File option not set for appender ["+name+"].");
            LogLog.warn("Are you using FileAppender instead of ConsoleAppender?");
        }
    }

    @Override
    public void append(LoggingEvent event) {
        String tag = (String) event.getMDC("tag");
        FileAppender fileAppender;

        if (tag == null || tag.isEmpty()) {
            fileAppender = sparkCommonLogAppender;
        } else {
            String[] arr = tag.split("\\$");
            if (arr.length == 3 && !"0".equals(arr[1])
                && StringUtils.isNotBlank(arr[0]) && StringUtils.isNotBlank(arr[1]) && StringUtils.isNotBlank(arr[2])) {
                String sessionId = arr[0];
                String taskGroupId = arr[1];
                String statementId = arr[2];

                String message = (String) event.getMessage();
                boolean specialOutput;
                if (message.startsWith("jobStatus")) {
                    statementId = "jobStatus";
                    specialOutput = true;
                    tag = sessionId + "$" + taskGroupId + "$" + statementId;
                } else if (message.startsWith("sinkResult")) {
                    statementId = "sinkResult";
                    specialOutput = true;
                    tag = sessionId + "$" + taskGroupId + "$" + statementId;
                } else {
                    specialOutput = false;
                }

                String statementFileName = fileName + File.separator + sessionId
                    + File.separator + taskGroupId + File.separator
                    + statementId + ".log";
                try {
                    fileAppender = appenderCache.get(tag, () -> {
                        try {
                            Layout newLayout = this.layout;
                            if (specialOutput) {
                                newLayout = new EnhancedPatternLayout();
                            }
                            return new FileAppender(newLayout, statementFileName);
                        } catch (IOException e) {
                            errorHandler.error("setFile(" + statementFileName + "," + fileAppend + ") call failed.",
                                e, ErrorCode.FILE_OPEN_FAILURE);
                            return null;
                        }
                    });
                } catch (ExecutionException e) {
                    errorHandler.error("Synchronization problem", e, ErrorCode.FILE_OPEN_FAILURE);
                    return;
                }
            } else {
                // task id 为0不记录
                return;
            }
        }
        fileAppender.append(event);
    }
}
