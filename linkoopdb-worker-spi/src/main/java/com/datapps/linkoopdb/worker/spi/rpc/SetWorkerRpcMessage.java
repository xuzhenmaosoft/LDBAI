package com.datapps.linkoopdb.worker.spi.rpc;

import com.google.common.base.Preconditions;

import com.datapps.linkoopdb.worker.spi.ResourceGroup;

/**
 * Created by gloway on 2019/9/3.
 */
public class SetWorkerRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.SET_WORKER;
    }

    private SetWorkerMessageType setWorkerMessageType;

    public enum SetWorkerMessageType {
        SET_MAX_EXE, SET_MIN_EXE, SHUTDOWN, INIT_WORKER, NEW_RESOURCE_GROUP, KILL_RESOURCE_GROUP
    }

    int executorNum;

    String token;

    ResourceGroup resourceGroup;

    String masterHost;
    int masterPort;

    String applicationId;

    public static SetWorkerRpcMessage buildSetMaxExecutor(int executorNum) {
        SetWorkerRpcMessage setWorkerRpcMessage = new SetWorkerRpcMessage();
        setWorkerRpcMessage.setWorkerMessageType = SetWorkerMessageType.SET_MAX_EXE;
        setWorkerRpcMessage.executorNum = executorNum;
        return setWorkerRpcMessage;
    }

    public static SetWorkerRpcMessage buildSetMinExecutor(int executorNum) {
        SetWorkerRpcMessage setWorkerRpcMessage = new SetWorkerRpcMessage();
        setWorkerRpcMessage.setWorkerMessageType = SetWorkerMessageType.SET_MIN_EXE;
        setWorkerRpcMessage.executorNum = executorNum;
        return setWorkerRpcMessage;
    }

    public static SetWorkerRpcMessage buildShutDown(String token) {
        SetWorkerRpcMessage setWorkerRpcMessage = new SetWorkerRpcMessage();
        setWorkerRpcMessage.setWorkerMessageType = SetWorkerMessageType.SHUTDOWN;
        setWorkerRpcMessage.token = token;
        return setWorkerRpcMessage;
    }

    public static SetWorkerRpcMessage buildInitWorker(String masterHost, int masterRegisterPort) {
        SetWorkerRpcMessage setWorkerRpcMessage = new SetWorkerRpcMessage();
        setWorkerRpcMessage.setWorkerMessageType = SetWorkerMessageType.INIT_WORKER;
        setWorkerRpcMessage.masterHost = masterHost;
        setWorkerRpcMessage.masterPort = masterRegisterPort;
        return setWorkerRpcMessage;
    }

    public static SetWorkerRpcMessage buildNewResourceGroup(ResourceGroup resourceGroup, String applicationId) {
        SetWorkerRpcMessage setWorkerRpcMessage = new SetWorkerRpcMessage();
        setWorkerRpcMessage.setWorkerMessageType = SetWorkerMessageType.NEW_RESOURCE_GROUP;
        setWorkerRpcMessage.resourceGroup = resourceGroup;
        setWorkerRpcMessage.applicationId = applicationId;
        return setWorkerRpcMessage;
    }

    public static SetWorkerRpcMessage buildKillResourceGroup(ResourceGroup resourceGroup) {
        SetWorkerRpcMessage setWorkerRpcMessage = new SetWorkerRpcMessage();
        setWorkerRpcMessage.setWorkerMessageType = SetWorkerMessageType.KILL_RESOURCE_GROUP;
        setWorkerRpcMessage.resourceGroup = Preconditions.checkNotNull(resourceGroup, "resource group is null.");
        return setWorkerRpcMessage;
    }

    private SetWorkerRpcMessage() {

    }

    public SetWorkerMessageType getSetWorkerMessageType() {
        return setWorkerMessageType;
    }

    public int getExecutorNum() {
        return executorNum;
    }

    public String getToken() {
        return token;
    }

    public ResourceGroup getResourceGroup() {
        return resourceGroup;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public String getApplicationId() {
        return applicationId;
    }
}
