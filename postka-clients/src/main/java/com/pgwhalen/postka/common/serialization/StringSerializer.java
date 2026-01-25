package com.pgwhalen.postka.common.serialization;

import java.nio.charset.StandardCharsets;

/**
 * String encoding defaults to UTF8 and can be customized.
 */
public class StringSerializer implements Serializer<String> {

    @Override
    public byte[] serialize(String topic, String data) {
        System.out.println("HI");
        if (data == null) {
            return null;
        }
        return data.getBytes(StandardCharsets.UTF_8);
    }
}
