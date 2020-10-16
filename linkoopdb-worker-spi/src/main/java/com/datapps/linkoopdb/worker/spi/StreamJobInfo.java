package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;
import java.text.SimpleDateFormat;

public class StreamJobInfo implements Serializable {

    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final int SIZE_OF_LONG = 8;

    private String jobName;
    private String jobId;
    private long startTime;
    private JobState jobState;
    private String savepointPath;

    public StreamJobInfo(String jobId) {
        this.jobId = jobId;
    }

    public StreamJobInfo(String jobName, String jobId, long startTime, String jobState) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.startTime = startTime;
        this.jobState = JobState.valueOf(jobState);
    }

    public StreamJobInfo withJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public StreamJobInfo withStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public StreamJobInfo withJobState(JobState jobState) {
        this.jobState = jobState;
        return this;
    }

    public StreamJobInfo withSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
        return this;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getStartTimeStr() {
        return format.format(startTime);
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public JobState getJobState() {
        return jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public void setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamJobInfo jobInfoVO = (StreamJobInfo) o;

        if (startTime != jobInfoVO.startTime) {
            return false;
        }
        if (jobName != null ? !jobName.equals(jobInfoVO.jobName) : jobInfoVO.jobName != null) {
            return false;
        }
        if (jobState != null ? !(jobState == jobInfoVO.jobState) : jobInfoVO.jobState != null) {
            return false;
        }

        if (savepointPath != null ? !savepointPath.equals(jobInfoVO.savepointPath) : jobInfoVO.savepointPath != null) {
            return false;
        }

        return jobId != null ? jobId.equals(jobInfoVO.jobId) : jobInfoVO.jobId == null;
    }

    @Override
    public int hashCode() {
        int result = jobName != null ? jobName.hashCode() : 0;
        result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
        result = 31 * result + (jobState != null ? jobState.hashCode() : 0);
        result = 31 * result + (savepointPath != null ? savepointPath.hashCode() : 0);
        result = 31 * result + (int) (startTime ^ (startTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "StreamJobInfo{"
            + "jobName='" + jobName + '\''
            + ", jobId='" + jobId + '\''
            + ", startTime=" + startTime
            + ", jobState=" + jobState
            + ", savepointPath='" + savepointPath + '\''
            + '}';
    }

    public enum JobState {
        CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED, RESTARTING, SUSPENDING, SUSPENDED, RECONCILING
    }

    public static long statementId(String jobId) {
        return byteArrayToLong(hexStringToByte(jobId), SIZE_OF_LONG);
    }

    public static long sessionId(String jobId) {
        return byteArrayToLong(hexStringToByte(jobId), 0);
    }

    public static String taskgroupIdFromJobname(String jobName) {
        int index = jobName.indexOf(':');
        if (index > -1) {
            String prefix = jobName.substring(0, index);
            if (prefix.contains("$")) {
                String[] parts = jobName.split("$");
                if (parts.length == 3) {
                    return parts[1];
                }
            }
        }
        return "0";
    }

    private static long byteArrayToLong(byte[] ba, int offset) {
        long l = 0;

        for (int i = 0; i < SIZE_OF_LONG; ++i) {
            l |= (ba[offset + SIZE_OF_LONG - 1 - i] & 0xffL) << (i << 3);
        }

        return l;
    }

    private static byte[] hexStringToByte(final String hex) {
        final byte[] bts = new byte[hex.length() / 2];
        for (int i = 0; i < bts.length; i++) {
            bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return bts;
    }
}
