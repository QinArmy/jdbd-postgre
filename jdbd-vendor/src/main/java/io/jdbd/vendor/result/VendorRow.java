package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.result.JdbdRow;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class VendorRow implements JdbdRow {


    @Override
    public final Object get(String columnLabel) throws JdbdException {
        return this.get(mapToIndex(columnLabel));
    }

    @Override
    public final <T> T get(String columnLabel, Class<T> columnClass) throws JdbdException {
        return this.get(mapToIndex(columnLabel), columnClass);
    }

    @Override
    public final Object getNonNull(String columnLabel) throws NullPointerException, JdbdException {
        return this.getNonNull(mapToIndex(columnLabel));
    }

    @Override
    public final <T> T getNonNull(String columnLabel, Class<T> columnClass) throws NullPointerException, JdbdException {
        return this.getNonNull(mapToIndex(columnLabel), columnClass);
    }

    @Override
    public final <T> List<T> getList(String columnLabel, Class<T> elementClass) throws JdbdException {
        return this.getList(mapToIndex(columnLabel), elementClass);
    }

    @Override
    public final <T> Set<T> getSet(String columnLabel, Class<T> elementClass) throws JdbdException {
        return this.getSet(mapToIndex(columnLabel), elementClass);
    }

    @Override
    public final <K, V> Map<K, V> getMap(String columnLabel, Class<K> keyClass, Class<V> valueClass)
            throws JdbdException {
        return this.getMap(mapToIndex(columnLabel), keyClass, valueClass);
    }

    @Override
    public final <T> Publisher<T> getPublisher(String columnLabel, Class<T> valueClass) throws JdbdException {
        return this.getPublisher(mapToIndex(columnLabel), valueClass);
    }


    protected abstract int mapToIndex(String columnLabel);


}
