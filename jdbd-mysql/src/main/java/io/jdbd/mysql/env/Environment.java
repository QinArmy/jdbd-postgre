package io.jdbd.mysql.env;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;

import java.util.Map;
import java.util.function.Supplier;

public interface Environment {

    @Nullable
    <T> T get(MySQLKey<T> key) throws JdbdException;

    <T> T get(MySQLKey<T> key, Supplier<T> supplier) throws JdbdException;

    <T> T getOrDefault(MySQLKey<T> key) throws JdbdException;


    <T extends Comparable<T>> T getInRange(MySQLKey<T> key, T minValue, T maxValue) throws JdbdException;

    <T> T getRequired(MySQLKey<T> key) throws JdbdException;


    static Environment parse(String url, Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }

}
