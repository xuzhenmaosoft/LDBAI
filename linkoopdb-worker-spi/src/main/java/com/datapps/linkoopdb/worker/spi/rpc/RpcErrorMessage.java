package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.jdbc.LdbSqlRetryException;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Created by gloway on 2019/9/12.
 */
public class RpcErrorMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.ERROR_MESSAGE;
    }

    String stackTrace;
    String message;
    String cause;
    boolean retryable;

    public static RpcErrorMessage buildErrorMessage(Throwable e) {
        RpcErrorMessage rpcErrorMessage = new RpcErrorMessage();
        rpcErrorMessage.stackTrace = ExceptionUtils.getStackTrace(e);
        rpcErrorMessage.message = e.getMessage();
        rpcErrorMessage.cause = (e.getCause() == null ? e.getMessage() : e.getCause().toString());
        rpcErrorMessage.retryable = isRetryable(e);
        return rpcErrorMessage;
    }

    private static boolean isRetryable(Throwable cause) {
        Throwable tmpCause = cause;
        while (tmpCause != null) {
            if (tmpCause instanceof LdbSqlRetryException
                || (tmpCause.getMessage() != null && tmpCause.getMessage().contains(LdbSqlRetryException.class.getSimpleName()))) {
                return true;
            }
            tmpCause = tmpCause.getCause();
        }
        return false;
    }

    private RpcErrorMessage(){

    }

    public String getStackTrace() {
        return stackTrace;
    }

    public String getMessage() {
        return message;
    }

    public String getCause() {
        return cause;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
