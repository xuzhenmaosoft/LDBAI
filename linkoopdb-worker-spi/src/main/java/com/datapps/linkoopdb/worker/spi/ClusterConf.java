package com.datapps.linkoopdb.worker.spi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterConf {
    static final Logger logger = LoggerFactory.getLogger("AUDIT");
    public static final String SERVER_MODE_PB = "ha";            //server模式为主备，高可用
    public static final String SERVER_MODE_SINGLE = "single";    //server模式为单点

    public static final String LAUNCHER_MODE_LOCAL = "local";        //worker加载模式为本地，开发调试用
    public static final String LAUNCHER_MODE_SINGLE = "single";        //worker加载模式为单点，单机版本部署使用
    public static final String LAUNCHER_MODE_CLUSTER = "cluster";    //worker加载模式为yarn集群加载

    public static final String CLUSTER_LAUNCHER_YARN = "yarn";    //worker加载模式为yarn集群加载
    public static final String CLUSTER_LAUNCHER_K8S = "k8s";    //worker加载模式为k8s集群加载

    public static volatile boolean running = false;
    public static String serverMode = SERVER_MODE_SINGLE;
    public static String launcherMode = LAUNCHER_MODE_CLUSTER;
    private static String storageLauncherMode = LAUNCHER_MODE_LOCAL;
    public static Map<String, Triple<String, String, Integer>> clusterMembersMap = new HashMap<>();
    public static Map<String, MemberNodeDef> clusterMemberNodes = new HashMap<>();
    public static List<String> dbMembers = new ArrayList<>();
    private static volatile boolean mainFlag = false;        //集群模式下标识当前server是否是主服务器; 多线程修改读取
    private static boolean rmAtomixDataWhenFirstStart = false;        //初次启动时是否删除atomix的历史状态
    public static List<String> hostList = new ArrayList<>();

    public static List<Triple<String, String, Integer>> getClusterMembers() {
        return new ArrayList<>(clusterMembersMap.values());
    }

    public static void init() {

    }

    public static void setClusterMembers(String clusterList) {
        if (clusterList != null) {
            clusterMembersMap.clear();
            clusterMemberNodes.clear();
            dbMembers.clear();
            String[] servers = clusterList.split(",");
            if (servers != null) {
                for (String server : servers) {
                    String[] info = server.split(":");
                    if (info.length < 4) {
                        continue;
                    }
                    MemberNodeDef mn = new MemberNodeDef(info[0], info[1],
                        Integer.valueOf(info[2]), Integer.valueOf(info[3]));
                    Triple<String, String, Integer> tri = Triple.of(info[0], info[1], Integer.valueOf(info[2]));
                    clusterMembersMap.put(info[0], tri);
                    clusterMemberNodes.put(info[0], mn);
                    dbMembers.add(info[1] + ":" + info[3]);
                    hostList.add(info[1]);
                }
            }
        }
    }

    public static MemberNodeDef getMemberInfoById(String memberId) {
        return clusterMemberNodes.get(memberId);
    }

    public static List<String> getDbMembers() {
        return dbMembers;
    }

    public static String getDbMembersStr() {
        return StringUtils.join(dbMembers.toArray(new String[dbMembers.size()]), ",");
    }

    public static String getServerMode() {
        return serverMode;
    }

    public static void setServerMode(String serverMode) {
        ClusterConf.serverMode = serverMode;
    }

    public static boolean isRmAtomixDataWhenFirstStart() {
        return rmAtomixDataWhenFirstStart;
    }

    public static void setRmAtomixDataWhenFirstStart(boolean rmAtomixDataWhenFirstStart) {
        ClusterConf.rmAtomixDataWhenFirstStart = rmAtomixDataWhenFirstStart;
    }

    public static String getLauncherMode() {
        return launcherMode;
    }

    public static void setLauncherMode(String launcherMode) {
        ClusterConf.launcherMode = launcherMode;
    }

    public static String getStorageLauncherMode() {
        return storageLauncherMode;
    }

    public static void setStorageLauncherMode(String storageLauncherMode) {
        ClusterConf.storageLauncherMode = storageLauncherMode;
    }

    public static boolean isServerModePB() {
        return serverMode.equalsIgnoreCase(SERVER_MODE_PB);
    }

    public static boolean isServerModeSingle() {
        return serverMode.equalsIgnoreCase(SERVER_MODE_SINGLE);
    }

    public static boolean isLauncherModeLocal() {
        return launcherMode.equalsIgnoreCase(LAUNCHER_MODE_LOCAL);
    }

    public static boolean isLauncherModeSingle() {
        return launcherMode.equalsIgnoreCase(LAUNCHER_MODE_SINGLE);
    }

    public static boolean isLauncherModeLocalOrSingle() {
        return isLauncherModeSingle() || isLauncherModeLocal();
    }

    public static boolean isLauncherModeCluster() {
        return launcherMode.equalsIgnoreCase(LAUNCHER_MODE_CLUSTER);
    }

    public static boolean isLauncherModeYarn() {
        return launcherMode.equalsIgnoreCase(CLUSTER_LAUNCHER_YARN);
    }

    public static boolean isLauncherModeK8S() {
        return launcherMode.startsWith(CLUSTER_LAUNCHER_K8S);
    }

    public static boolean isStorageLauncherModeK8S() {
        return storageLauncherMode.equalsIgnoreCase(CLUSTER_LAUNCHER_K8S);
    }

    public static boolean isStorageLauncherModeLocal() {
        return storageLauncherMode.equalsIgnoreCase(LAUNCHER_MODE_LOCAL);
    }

    public static boolean isFreeLauncherModel() {
        return launcherMode.equalsIgnoreCase(LAUNCHER_MODE_LOCAL);
    }

    public static boolean isMainFlag() {
        return mainFlag;
    }

    public static void setMainFlag(boolean mainFlag) {
        logger.info("Set main flag: {}", mainFlag);
        ClusterConf.mainFlag = mainFlag;
    }

    public static void setMainFlag(boolean mainFlag, long currentTime) {
        logger.info("Set main flag: {}::{}", mainFlag, currentTime);
        ClusterConf.mainFlag = mainFlag;
    }

    public static boolean isRunning() {
        return running;
    }

    public static void setRunning(boolean running) {
        ClusterConf.running = running;
    }

    public static class MemberNodeDef {

        public String memberId;
        public String memberHost;
        public int memberPort;
        public int dbPort;

        public MemberNodeDef(String memberId, String memberHost, int memberPort, int dbPort) {
            this.memberId = memberId;
            this.memberHost = memberHost;
            this.memberPort = memberPort;
            this.dbPort = dbPort;
        }
    }
}
