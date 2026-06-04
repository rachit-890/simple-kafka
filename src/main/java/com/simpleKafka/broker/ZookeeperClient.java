package com.simpleKafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClient implements Watcher {

    private static final int SESSION_TIMEOUT = 3000;

    private final String host;
    private final int port;

    private ZooKeeper zooKeeper;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private String getConnectString() {
        return host + ":" + port;
    }

    public void connect() throws IOException, InterruptedException {

        zooKeeper = new ZooKeeper(
                getConnectString(),
                SESSION_TIMEOUT,
                this
        );

        connectedSignal.await();

        createPath("/brokers");
        createPath("/brokers/ids");

        createPath("/topics");

        System.out.println("Connected to ZooKeeper");
    }

    private void createPath(String path) {

        try {

            Stat stat = zooKeeper.exists(path, false);

            if (stat == null) {

                zooKeeper.create(
                        path,
                        new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT
                );

                System.out.println("Created path: " + path);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void createPersistentNode(String path, String data)
            throws KeeperException, InterruptedException {

        Stat stat = zooKeeper.exists(path, false);

        if (stat == null) {

            zooKeeper.create(
                    path,
                    data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );

            System.out.println("Created persistent node: " + path);

        } else {

            zooKeeper.setData(
                    path,
                    data.getBytes(),
                    -1
            );

            System.out.println("Updated node: " + path);
        }
    }

    public boolean createEphemeralNode(String path, String data)
            throws KeeperException, InterruptedException {

        Stat stat = zooKeeper.exists(path, false);

        if (stat == null) {

            zooKeeper.create(
                    path,
                    data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
            );

            System.out.println("Created ephemeral node: " + path);

            return true;
        }

        return false;
    }

    public String getData(String path)
            throws KeeperException, InterruptedException {

        byte[] data = zooKeeper.getData(
                path,
                false,
                null
        );

        return new String(data);
    }

    public void deleteNode(String path)
            throws KeeperException, InterruptedException {

        Stat stat = zooKeeper.exists(path, false);

        if (stat != null) {

            zooKeeper.delete(path, stat.getVersion());

            System.out.println("Deleted node: " + path);
        }
    }

    public void close() throws InterruptedException {

        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    @Override
    public void process(WatchedEvent event) {

        if (event.getState()
                == Event.KeeperState.SyncConnected) {

            connectedSignal.countDown();

        } else if (event.getState()
                == Event.KeeperState.Disconnected) {

            System.out.println(
                    "Disconnected from ZooKeeper"
            );

        } else if (event.getState()
                == Event.KeeperState.Expired) {

            System.out.println(
                    "ZooKeeper session expired"
            );
        }
    }
}