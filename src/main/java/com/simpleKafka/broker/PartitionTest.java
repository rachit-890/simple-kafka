package com.simpleKafka.broker;

public class PartitionTest {

    public static void main(String[] args)
            throws Exception {

        Partition partition =
                new Partition(
                        0,
                        "./data"
                );

        partition.append(
                "Order Created".getBytes()
        );

        partition.append(
                "Payment Done".getBytes()
        );

        System.out.println(
                partition.readMessages(0)
        );
    }
}