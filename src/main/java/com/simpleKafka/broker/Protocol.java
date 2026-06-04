package com.simpleKafka.broker;

import java.nio.ByteBuffer;

public class Protocol {

    // =========================
    // Request Types
    // =========================
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // =========================
    // Response Types
    // =========================
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte ERROR_RESPONSE = 0x7F;

    /**
     * Encode Produce Request
     */
    public static ByteBuffer encodeProduceRequest(
            String topic,
            int partition,
            byte[] message) {

        byte[] topicBytes = topic.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 +                     // request type
                        2 +                     // topic length
                        topicBytes.length +     // topic bytes
                        4 +                     // partition
                        4 +                     // message length
                        message.length          // message bytes
        );

        buffer.put(PRODUCE);

        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        buffer.putInt(partition);

        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();

        return buffer;
    }

    /**
     * Encode Fetch Request
     */
    public static ByteBuffer encodeFetchRequest(
            String topic,
            int partition,
            long offset,
            int maxBytes) {

        byte[] topicBytes = topic.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 +
                        2 +
                        topicBytes.length +
                        4 +
                        8 +
                        4
        );

        buffer.put(FETCH);

        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        buffer.putInt(partition);

        buffer.putLong(offset);

        buffer.putInt(maxBytes);

        buffer.flip();

        return buffer;
    }

    /**
     * Metadata Request
     */
    public static ByteBuffer encodeMetadataRequest() {

        ByteBuffer buffer = ByteBuffer.allocate(1);

        buffer.put(METADATA);

        buffer.flip();

        return buffer;
    }

    /**
     * Create Topic Request
     */
    public static ByteBuffer encodeCreateTopicRequest(
            String topic,
            int partitions,
            short replicationFactor) {

        byte[] topicBytes = topic.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 +
                        2 +
                        topicBytes.length +
                        4 +
                        2
        );

        buffer.put(CREATE_TOPIC);

        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        buffer.putInt(partitions);

        buffer.putShort(replicationFactor);

        buffer.flip();

        return buffer;
    }

    // =========================
    // Result Classes
    // =========================

    public static class ProduceResult {

        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }
    }

    public static class FetchResult {

        private final byte[][] messages;
        private final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public byte[][] getMessages() {
            return messages;
        }

        public String getError() {
            return error;
        }
    }

    public static class MetadataResult {

        private final String error;

        public MetadataResult(String error) {
            this.error = error;
        }

        public String getError() {
            return error;
        }
    }
}