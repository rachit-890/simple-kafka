package com.simpleKafka.client;

import com.simpleKafka.broker.Protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class SimpleKafkaClient {

    private final String brokerHost;
    private final int brokerPort;

    public SimpleKafkaClient(
            String brokerHost,
            int brokerPort) {

        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    public void initialize() {

        System.out.println(
                "Client initialized"
        );
    }

    public long send(
            String topic,
            int partition,
            byte[] message)
            throws IOException {

        ByteBuffer request =
                Protocol.encodeProduceRequest(
                        topic,
                        partition,
                        message
                );

        try (SocketChannel channel =
                     SocketChannel.open()) {

            channel.connect(
                    new InetSocketAddress(
                            brokerHost,
                            brokerPort
                    )
            );

            channel.write(request);

            System.out.println(
                    "Message sent"
            );
        }

        return -1;
    }

    public List<byte[]> fetch(
            String topic,
            int partition,
            long offset,
            int maxBytes)
            throws IOException {

        ByteBuffer request =
                Protocol.encodeFetchRequest(
                        topic,
                        partition,
                        offset,
                        maxBytes
                );

        try (SocketChannel channel =
                     SocketChannel.open()) {

            channel.connect(
                    new InetSocketAddress(
                            brokerHost,
                            brokerPort
                    )
            );

            channel.write(request);

            System.out.println(
                    "Fetch request sent"
            );
        }

        return new ArrayList<>();
    }
}