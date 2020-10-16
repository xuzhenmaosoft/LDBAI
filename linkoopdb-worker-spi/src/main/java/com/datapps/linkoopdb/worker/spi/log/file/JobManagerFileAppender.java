package com.datapps.linkoopdb.worker.spi.log.file;

import java.io.Flushable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * manage extends job master file appender.
 */
public class JobManagerFileAppender extends RollingFileAppender implements Flushable {

    private String baseLogDir;

    private Map<String, JobMasterFileAppender> appenderCache = new ConcurrentHashMap<>();

    @Override
    public void append(LoggingEvent event) {
        JobLogInfoBase jobInfo = (JobLogInfoBase) event.getMDC(JobLogInfoBase.JOB_LOG_TAG);

        if (jobInfo == null) {
            // LogLog.warn("cannot found job_log_tag");
        } else {
            JobMasterFileAppender appender;
            if (!jobInfo.isOpenFlag()
                && (appender =
                appenderCache.get(jobInfo.genLogKey())) != null) {
                appenderCache.remove(jobInfo.genLogKey());
                appender.close();
                return;
            }

            if (jobInfo.isOpenFlag()) {

                if (!appenderCache.containsKey(jobInfo.genLogKey())) {
                    appender = new JobMasterFileAppender(
                        this.baseLogDir, jobInfo);
                    appender.setLayout(this.getLayout());
                    appender.setMaximumFileSize(this.getMaximumFileSize());
                    appender.setMaxBackupIndex(this.getMaxBackupIndex());
                    appender.setEncoding(this.getEncoding());
                    appender.setImmediateFlush(this.getImmediateFlush());
                    appender.setErrorHandler(this.getErrorHandler());
                    appender.setBufferSize(this.getBufferSize());
                    appender.setBufferedIO(this.getBufferedIO());
                    appender.setAppend(this.getAppend());
                    appender.activateOptions();

                    appenderCache.put(
                        jobInfo.genLogKey(),
                        appender);
                }

                appender = appenderCache.get(jobInfo.genLogKey());
                appender.append(event);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        appenderCache.forEach((tag, fileAppend) -> {
            try {
                fileAppend.flush();
            } catch (IOException ex) {
                LogLog.warn("close file " + fileAppend.getFile() + " failed with {}", ex);
            }
        });
    }

    @Override
    public String getFile() {
        return baseLogDir;
    }

    @Override
    public void setFile(String baseLogDir) {
        this.baseLogDir = baseLogDir;
    }

}


