package com.datapps.linkoopdb.worker.spi.log.file;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;

import org.apache.log4j.RollingFileAppender;

/**
 * extends job master file appender.
 */
public class JobMasterFileAppender extends RollingFileAppender implements Flushable {

    private JobLogInfoBase jobInfo;
    private String baseLogDir;

    public JobMasterFileAppender(String baseLogDir, JobLogInfoBase jobInfo) {
        this.jobInfo = jobInfo;
        this.baseLogDir = baseLogDir;
    }

    @Override
    public void activateOptions() {
        synchronized (this) {
            setFile(new File(this.baseLogDir, getPath(jobInfo)).toString());
            super.activateOptions();
        }
    }

    @Override
    public void flush() throws IOException {
        if (qw != null) {
            qw.flush();
        }
    }

    private String getPath(JobLogInfoBase jobInfo) {
        return new StringBuffer()
            .append("job")
            .append(File.separator)
            .append(jobInfo.getSessionId())
            .append(File.separator)
            .append(jobInfo.getTaskGroupId())
            .append(File.separator)
            .append(jobInfo.getStatementId())
            .append(File.separator)
            .append("ldb-worker.log")
            .toString();
    }
}


