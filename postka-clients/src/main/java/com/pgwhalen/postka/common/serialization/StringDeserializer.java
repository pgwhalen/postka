package com.pgwhalen.postka.common.serialization;

import java.nio.charset.StandardCharsets;

/**
 * String encoding defaults to UTF8 and can be customized.
 */
public class StringDeserializer implements Deserializer<String> {

    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return new String(data, StandardCharsets.UTF_8);
    }
}
