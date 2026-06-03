package com.simpleKafka.broker;

import org.apache.zookeeper.ZooKeeper;

public class SimpleKafkaBroker {

    public static void main(String[] args) {

        System.out.println("Broker Started");
        System.out.println(ZooKeeper.class.getName());

    }
}