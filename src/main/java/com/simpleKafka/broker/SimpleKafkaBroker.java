package com.simpleKafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

public class SimpleKafkaBroker {

    private final int brokerId;
    private final String host;
    private final int port;

    private ServerSocketChannel serverChannel;

    public SimpleKafkaBroker(
            int brokerId,
            String host,
            int port) {

        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
    }

    public void start() throws IOException {

        serverChannel = ServerSocketChannel.open();

        serverChannel.bind(
                new InetSocketAddress(host, port)
        );

        System.out.println(
                "Broker "
                        + brokerId
                        + " started on "
                        + host
                        + ":" + port
        );

        while (true) {

            System.out.println(
                    "Waiting for client connections..."
            );

            var client = serverChannel.accept();

            System.out.println(
                    "Client connected: "
                            + client.getRemoteAddress()
            );

            client.close();
        }
    }

    public static void main(String[] args)
            throws Exception {

        SimpleKafkaBroker broker =
                new SimpleKafkaBroker(
                        1,
                        "localhost",
                        9092
                );

        broker.start();
    }
}