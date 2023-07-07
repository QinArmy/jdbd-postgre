package io.jdbd.mysql.protocol.env;

import io.jdbd.lang.Nullable;

import java.util.Map;

public interface ProtocolEnvironment {

    ProtocolType type();

    @Nullable
    <T> T get(MySQLKey<T> key);

    <T> T geOrDefault(MySQLKey<T> key);

    <T> T getRequired(MySQLKey<T> key);


    static ProtocolEnvironment parse(String url, Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }

}
