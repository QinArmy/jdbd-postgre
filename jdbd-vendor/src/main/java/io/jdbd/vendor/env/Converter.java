package io.jdbd.vendor.env;

import io.jdbd.JdbdException;

public interface Converter<T> {

    Class<T> target();

    T convert(String source) throws JdbdException;

}
