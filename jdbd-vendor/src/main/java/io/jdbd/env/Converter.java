package io.jdbd.env;

import io.jdbd.JdbdException;

public interface Converter<T> {

    Class<T> target();

    T convert(String source) throws JdbdException;

}
