package io.jdbd.mysql.env;

import io.jdbd.lang.Nullable;

import java.util.Map;
import java.util.function.Supplier;

public interface Environment {

    @Nullable
    <T> T get(MySQLKey<T> key);

    <T> T get(MySQLKey<T> key, Supplier<T> supplier);

    <T> T getOrDefault(MySQLKey<T> key);

    <T extends Comparable<T>> T getOrMin(MySQLKey<T> key, T minValue);

    <T> T getRequired(MySQLKey<T> key);


    static Environment parse(String url, Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }

}
