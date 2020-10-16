/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class ZKClient {

    private static final Logger logger = LoggerFactory.getLogger(ZKClient.class);
    private static final String WORKERS_PATH = "/linkoopdb/workers";
    private String zkRoot = null;
    private CuratorFramework zkc;
    private String absoultWorkersPath;

    public ZKClient(String zkConnect) {
        if (zkConnect == null) {
            throw new RuntimeException("invalid worker.launcher.zookeeper.connect=" + zkConnect);
        }

        int slashEnd = zkConnect.indexOf('/');
        if (slashEnd > 0) {
            absoultWorkersPath = zkConnect.substring(slashEnd) + WORKERS_PATH;
            zkRoot = zkConnect.substring(0, slashEnd);
        } else {
            absoultWorkersPath = WORKERS_PATH;
            zkRoot = zkConnect;
        }
        try {
            zkc = CuratorFrameworkFactory.newClient(zkRoot,
                new ExponentialBackoffRetry(500, 3));
            zkc.start();
            if (zkc.checkExists().forPath(absoultWorkersPath) == null) {
                String root = zkc.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT).forPath(absoultWorkersPath);
                logger.info("Root node {} created", root);
            } else {
                logger.info("Root node {} exits", zkConnect.substring(0, slashEnd + 1) + WORKERS_PATH);
            }
        } catch (Exception e) {
            logger.error("Can not create worker root zk node on zookeeper: {}", zkConnect, e);
            throw new RuntimeException("Can not create zk root node", e);
        }
    }

    public static void main(String[] args) throws Exception {
        ZKClient client = new ZKClient("172.17.0.1:2181/mytest");
        client.addWorker("001", "http://my001:7897");

        List<String> workers = client.getWorkers();
        for (String worker : workers) {
            System.out.println("worker: " + worker);
        }

        client.removeWorker("001");
        workers = client.getWorkers();
        System.out.println("after romove: " + workers.size() + " left");

        for (String worker : workers) {
            System.out.println("worker: " + worker);
        }

        client.addWorker("001", "http://my001:7897");

        client.removeAllWorker();
        workers = client.getWorkers();
        for (String worker : workers) {
            System.out.println("worker: " + worker);
        }
    }

    public List<String> getWorkers() {
        logger.info("Loading workers from zk");
        try {
            List<String> urls = new ArrayList<>();
            for (String child : getWorkersId()) {
                urls.add(getWorker(child));
            }
            return urls;
        } catch (Exception e) {
            logger.error("can not load workers from zk");
            throw new RuntimeException("can not load workers from zk", e);
        }
    }

    public List<String> getWorkersId() {
        try {
            return zkc.getChildren().forPath(absoultWorkersPath);
        } catch (Exception e) {
            logger.error("can not load workers from zk");
            throw new RuntimeException("can not load workers from zk", e);
        }
    }

    /**
     * get worker url from zk node
     *
     * @param workerId worker id
     * @return worker service url, null if not found
     */
    public String getWorker(String workerId) {
        try {
            return new String(zkc.getData().forPath(absoultWorkersPath + "/" + workerId), "utf8");
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * get worker url from zk node with timeout
     *
     * @param workerId worker id
     * @param timeout timeout limit
     * @return worker service url, null if not found in time
     */
    public String getWorker(String workerId, long timeout) {
        String worker = null;
        long time = timeout;
        while (time > 0 && worker == null) {
            worker = getWorker(workerId);
            time -= 100;

            if (worker == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return worker;
    }

    public void addWorker(String workerId, String workerUrl) {
        String workerPath = ZKPaths.makePath(absoultWorkersPath, workerId);
        logger.info("Store worker url {} to zk {}", workerUrl, workerPath);
        try {
            zkc.create().withMode(CreateMode.PERSISTENT)
                .forPath(workerPath, workerUrl.getBytes("utf8"));
        } catch (Exception e) {
            logger.error("Can not add worker to zk");
            throw new RuntimeException("Can not load workers from zk", e);
        }
    }

    public void removeAllWorker() {

        logger.info("Remove all worker from zk");
        try {
            zkc.delete().deletingChildrenIfNeeded().forPath(absoultWorkersPath);
        } catch (Exception e) {
            logger.error("Can not delete workers", e);
        }

    }

    public boolean removeWorker(String workerId) {
        logger.info("Remove worker {} from zk {}", workerId, zkRoot + absoultWorkersPath);
        try {
            String workerPath = ZKPaths.makePath(absoultWorkersPath, workerId);
            if (zkc.checkExists().forPath(workerPath) != null) {
                zkc.delete().forPath(workerPath);
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.warn("Remove worker {} from zk failed with exception {}", workerId, e.getMessage());
            return false;
        }
    }

    public void close() {
        zkc.close();
    }
}
