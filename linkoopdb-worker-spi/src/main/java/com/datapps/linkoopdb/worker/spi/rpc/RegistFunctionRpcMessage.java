package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.Set;

/**
 * Created by gloway on 2019/9/4.
 */
public class RegistFunctionRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.REGISTER_FUNCTION;
    }

    public enum RegistFunctionMessageType {
        REGIST_SPARK_FUNCTION, REGIST_AI_FUNCTION, REMOVE_FUNC, UNREGIST_SPARK_FUNCTION, REGIST_SPARK_PYTHON_FUNCTION
    }

    private RegistFunctionMessageType registFunctionMessageType;

    long sessionId;
    Set<String> methods;
    String jarDir;
    String udfSpecificName;

    String pythonFullPath;
    String pythonMethodName;

    boolean deleteJar;

    public static RegistFunctionRpcMessage buildRegistFunctionMessage(long sessionId, Set<String> methods, String jarDir) {
        RegistFunctionRpcMessage registFunctionRpcMessage = new RegistFunctionRpcMessage();
        registFunctionRpcMessage.registFunctionMessageType = RegistFunctionMessageType.REGIST_SPARK_FUNCTION;
        registFunctionRpcMessage.sessionId = sessionId;
        registFunctionRpcMessage.methods = methods;
        registFunctionRpcMessage.jarDir = jarDir;
        return registFunctionRpcMessage;
    }

    public static RegistFunctionRpcMessage buildRegistPythonFunctionMessage(String pythonFullPath, String pythonMethodName) {
        RegistFunctionRpcMessage registFunctionRpcMessage = new RegistFunctionRpcMessage();
        registFunctionRpcMessage.registFunctionMessageType = RegistFunctionMessageType.REGIST_SPARK_PYTHON_FUNCTION;
        registFunctionRpcMessage.pythonFullPath = pythonFullPath;
        registFunctionRpcMessage.pythonMethodName = pythonMethodName;
        return registFunctionRpcMessage;
    }

    public static RegistFunctionRpcMessage buildRegistAiFunctionMessage(Set<String> methods) {
        RegistFunctionRpcMessage registFunctionRpcMessage = new RegistFunctionRpcMessage();
        registFunctionRpcMessage.registFunctionMessageType = RegistFunctionMessageType.REGIST_AI_FUNCTION;
        registFunctionRpcMessage.methods = methods;
        return registFunctionRpcMessage;
    }

    public static RegistFunctionRpcMessage buildRemoveFunctionMessage(long sessionId, boolean isJar) {
        RegistFunctionRpcMessage registFunctionRpcMessage = new RegistFunctionRpcMessage();
        registFunctionRpcMessage.registFunctionMessageType = RegistFunctionMessageType.REMOVE_FUNC;
        registFunctionRpcMessage.sessionId = sessionId;
        registFunctionRpcMessage.deleteJar = isJar;
        return registFunctionRpcMessage;
    }

    public static RegistFunctionRpcMessage buildUnregistFunctionMessage(long sessionId, Set<String> methods, String specificName) {
        RegistFunctionRpcMessage registFunctionRpcMessage = new RegistFunctionRpcMessage();
        registFunctionRpcMessage.registFunctionMessageType = RegistFunctionMessageType.UNREGIST_SPARK_FUNCTION;
        registFunctionRpcMessage.sessionId = sessionId;
        registFunctionRpcMessage.methods = methods;
        registFunctionRpcMessage.udfSpecificName = specificName;
        return registFunctionRpcMessage;
    }

    private RegistFunctionRpcMessage() {

    }

    public RegistFunctionMessageType getRegistFunctionMessageType() {
        return registFunctionMessageType;
    }

    public long getSessionId() {
        return sessionId;
    }

    public Set<String> getMethods() {
        return methods;
    }

    public String getJarDir() {
        return jarDir;
    }

    public boolean isDeleteJar() {
        return deleteJar;
    }

    public String getUdfSpecificName() {
        return udfSpecificName;
    }

    public String getPythonFullPath() {
        return pythonFullPath;
    }

    public String getPythonMethodName() {
        return pythonMethodName;
    }

}
