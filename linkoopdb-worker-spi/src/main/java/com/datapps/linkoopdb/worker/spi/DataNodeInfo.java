package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;

public class DataNodeInfo implements Serializable {

    public static final int STATE_OFFLINE = 0;
    public static final int STATE_ONLINE = 1;
    public static final int STATE_TO_BE_OBSERVE = 2;      //主动或者被动挂掉1小时以内的节点，在此时间段内，若重启时提供了正确的storageId，则认可其上面维护的shards信息，并做统筹安排
    public static final int STATE_UNREACHABLE = 3;      //已初始化但未注册的节点

    public transient long observeTime = 0L;
    public transient int inCommand = -1;        //代表了当前节点处于哪种命令下，-1为不在命令队列中
    public transient int load = 0;              //节点负载情况
    public transient int primary = 0;              //包含主shard个数
    public transient int backup = 0;               //包含备份shard个数
    public transient int align = 0;                //包含对齐shard个数
    String name;                                   // pallas pod name，仅作为pod的名字
    String domain;                                    //如果启动在k8s中，则为k8s容器host，否则为主机host
    int grpcPort;
    String storageNodeId;                           //pallas节点id
    String host;                                    //物理机主机名
    String storagePath;                                 //物理机存储目录
    int state = STATE_TO_BE_OBSERVE;
    String rack;                                    //机架（集群节点分组用）
    String zone;                                    //机房（集群节点分组用）

    public DataNodeInfo() {

    }

    public DataNodeInfo(String storageNodeId, String host, String storagePath) {
        this.storageNodeId = storageNodeId;
        this.host = host;
        this.storagePath = storagePath;
    }

    public DataNodeInfo(String name, String domain, int grpcPort, String host, String storagePath, String storageNodeId, int state) {
        this.name = name;
        this.domain = domain;
        this.grpcPort = grpcPort;
        this.host = host;
        this.storagePath = storagePath;
        this.storageNodeId = storageNodeId;
        this.state = state;
    }

    public DataNodeInfo(String name, int grpcPort, String host, String storagePath, String storageNodeId, int state) {
        this.name = name;
        this.domain = ClusterConf.isLauncherModeK8S() ? name : host;
        this.grpcPort = grpcPort;
        this.host = host;
        this.storagePath = storagePath;
        this.storageNodeId = storageNodeId;
        this.state = state;
    }

    public DataNodeInfo(String domain, int grpcPort, String storageNodeId, int state) {
        this.name = domain;
        this.domain = domain;
        this.host = domain;
        this.grpcPort = grpcPort;
        this.storageNodeId = storageNodeId;
        this.state = state;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public String getStorageNodeId() {
        return storageNodeId;
    }

    public void setStorageNodeId(String storageNodeId) {
        this.storageNodeId = storageNodeId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getAddress() {
        return domain + ":" + grpcPort;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public String getRack() {
        return rack;
    }

    public void setRack(String rack) {
        this.rack = rack;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public DataNodeInfo clone() {
        DataNodeInfo dn = new DataNodeInfo();
        dn.setName(this.name);
        dn.setDomain(this.domain);
        dn.setGrpcPort(this.grpcPort);
        dn.setStorageNodeId(this.storageNodeId);
        dn.setHost(this.host);
        dn.setStoragePath(this.storagePath);
        dn.setState(this.state);
        dn.setRack(this.rack);
        dn.setZone(this.zone);
        return dn;
    }
}
