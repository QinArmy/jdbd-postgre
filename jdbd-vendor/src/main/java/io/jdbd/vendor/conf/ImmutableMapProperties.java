package io.jdbd.vendor.conf;

import org.qinarmy.env.ImmutableMapEnvironment;
import org.qinarmy.env.convert.Converter;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ImmutableMapProperties<K extends IPropertyKey>
        extends ImmutableMapEnvironment implements Properties<K> {

    public static <K extends IPropertyKey> ImmutableMapProperties<K> getInstance(Map<String, String> source) {
        return new ImmutableMapProperties<>(source);
    }

    private ImmutableMapProperties(Map<String, String> source) {
        super(source);
    }

    @Override
    public int size() {
        return this.source.size();
    }

    @Override
    public Map<String, String> getSource() {
        return this.source;
    }


    @Nullable
    @Override
    public String getProperty(K key) {
        return getProperty(key.getKey());
    }

    @Override
    public String getProperty(K key, String defaultValue) {
        return getProperty(key.getKey(), defaultValue);
    }

    @Override
    public <T> T getProperty(K key, Class<T> targetType) {
        return getProperty(key.getKey(), targetType);
    }

    @Override
    public <T> T getProperty(K key, Class<T> targetType, T defaultValue) {
        return getProperty(key.getKey(), targetType, defaultValue);
    }

    @Override
    public List<String> getPropertyList(K key) {
        return getPropertyList(key.getKey(), String.class);
    }

    @Override
    public <T> List<T> getPropertyList(K key, Class<T> targetArrayType) {
        return getPropertyList(key.getKey(), targetArrayType);
    }

    @Override
    public <T> List<T> getPropertyList(K key, Class<T> targetArrayType, List<T> defaultList) {
        return getPropertyList(key.getKey(), targetArrayType, defaultList);
    }

    @Override
    public <T> Set<T> getPropertySet(K key, Class<T> targetArrayType) {
        return getPropertySet(key.getKey(), targetArrayType);
    }

    @Override
    public Set<String> getPropertySet(K key) {
        return getPropertySet(key.getKey(), String.class);
    }

    @Override
    public <T> Set<T> getPropertySet(K key, Class<T> targetArrayType, Set<T> defaultSet) {
        return getPropertySet(key.getKey(), targetArrayType, defaultSet);
    }

    @Override
    public String getRequiredProperty(K key) throws IllegalStateException {
        return getRequiredProperty(key.getKey());
    }

    @Override
    public <T> T getRequiredProperty(K key, Class<T> targetType) throws IllegalStateException {
        return getRequiredProperty(key.getKey(), targetType);
    }

    @Override
    public String getOrDefault(K key) throws IllegalStateException {
        String value = getProperty(key);
        if (value != null) {
            return value;
        }
        String defaultText = key.getDefault();
        if (defaultText == null) {
            throw new IllegalStateException(String.format("not found value for key[%s]", key.getKey()));
        }
        return defaultText;
    }

    @Override
    public <T> T getOrDefault(K key, Class<T> targetType) throws IllegalStateException {
        T value = getProperty(key, targetType);
        if (value != null) {
            return value;
        }
        String defaultText = key.getDefault();
        if (defaultText == null) {
            throw new IllegalStateException(String.format("not found value for key[%s]", key.getKey()));
        } else {
            Converter<String, T> converter = this.converterManager.getConverter(String.class, targetType);
            if (converter == null) {
                throw new IllegalStateException(
                        String.format("not found Converter for [%s,%s]", String.class, targetType));
            } else {
                value = converter.convert(defaultText);
            }
        }
        return value;
    }
}