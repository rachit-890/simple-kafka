package com.simpleKafka.broker;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {

    private final int id;
    private final String logFile;

    private final AtomicLong nextOffset =
            new AtomicLong(0);

    public Partition(int id, String baseDir)
            throws IOException {

        this.id = id;

        File dir = new File(baseDir);

        if (!dir.exists()) {
            dir.mkdirs();
        }

        this.logFile =
                baseDir + "/partition-" + id + ".log";

        File file = new File(logFile);

        if (!file.exists()) {
            file.createNewFile();
        }
    }

    public long append(byte[] message)
            throws IOException {

        long offset =
                nextOffset.getAndIncrement();

        try (FileWriter writer =
                     new FileWriter(logFile, true)) {

            String record =
                    offset + ":" +
                            new String(message,
                                    StandardCharsets.UTF_8)
                            + "\n";

            writer.write(record);
        }

        return offset;
    }

    public List<String> readMessages(long startOffset)
            throws IOException {

        List<String> result =
                new ArrayList<>();

        try (BufferedReader reader =
                     new BufferedReader(
                             new FileReader(logFile))) {

            String line;

            while ((line = reader.readLine())
                    != null) {

                String[] parts =
                        line.split(":", 2);

                long offset =
                        Long.parseLong(parts[0]);

                if (offset >= startOffset) {
                    result.add(parts[1]);
                }
            }
        }

        return result;
    }
}