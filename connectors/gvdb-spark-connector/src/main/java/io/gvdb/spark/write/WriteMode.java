package io.gvdb.spark.write;

import java.util.Locale;

public enum WriteMode {
    UPSERT,
    STREAM_INSERT;

    public static WriteMode fromOption(String value) {
        if (value == null || value.isBlank()) {
            return UPSERT;
        }
        try {
            return WriteMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown write mode: '" + value + "'. Supported values: upsert, stream_insert");
        }
    }
}
