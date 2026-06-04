package com.simpleKafka.broker;


public class ZooKeeperTest {

    public static void main(String[] args)
            throws Exception {

        ZookeeperClient zk =
                new ZookeeperClient(
                        "localhost",
                        2181
                );

        zk.connect();

        System.out.println("Connected!");

        zk.close();
    }
}