package io.jdbd.vendor.conf;

import org.qinarmy.env.ImmutableMapEnvironment;
import org.qinarmy.env.convert.Converter;
import org.qinarmy.env.convert.ConverterManager;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ImmutableMapProperties extends ImmutableMapEnvironment implements Properties {

    public static ImmutableMapProperties getInstance(Map<String, String> source) {
        return new ImmutableMapProperties(source);
    }


    public static ImmutableMapProperties getInstance(Map<String, String> source
            , ConverterManager converterManager) {
        return new ImmutableMapProperties(source, converterManager);
    }

    private ImmutableMapProperties(Map<String, String> source) {
        super(source);
    }

    private ImmutableMapProperties(Map<String, String> source, ConverterManager converterManager) {
        super(source, converterManager);
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
    public String get(PropertyKey key) {
        return getProperty(key.getKey());
    }

    @Override
    public String get(PropertyKey key, String defaultValue) {
        return getProperty(key.getKey(), defaultValue);
    }

    @Override
    public <T> T get(PropertyKey key, Class<T> targetType) {
        return getProperty(key.getKey(), targetType);
    }

    @Override
    public <T> T get(PropertyKey key, Class<T> targetType, T defaultValue) {
        return getProperty(key.getKey(), targetType, defaultValue);
    }

    @Override
    public List<String> getList(PropertyKey key) {
        return getPropertyList(key.getKey(), String.class);
    }

    @Override
    public <T> List<T> getList(PropertyKey key, Class<T> targetArrayType) {
        return getPropertyList(key.getKey(), targetArrayType);
    }

    @Override
    public <T> List<T> getList(PropertyKey key, Class<T> targetArrayType, List<T> defaultList) {
        return getPropertyList(key.getKey(), targetArrayType, defaultList);
    }

    @Override
    public <T> Set<T> getSet(PropertyKey key, Class<T> targetArrayType) {
        return getPropertySet(key.getKey(), targetArrayType);
    }

    @Override
    public Set<String> getSet(PropertyKey key) {
        return getPropertySet(key.getKey(), String.class);
    }

    @Override
    public <T> Set<T> getSet(PropertyKey key, Class<T> targetArrayType, Set<T> defaultSet) {
        return getPropertySet(key.getKey(), targetArrayType, defaultSet);
    }

    @Override
    public String getNonNull(PropertyKey key) throws IllegalStateException {
        return getRequiredProperty(key.getKey());
    }

    @Override
    public <T> T getNonNull(PropertyKey key, Class<T> targetType) throws IllegalStateException {
        return getRequiredProperty(key.getKey(), targetType);
    }

    @Override
    public String getOrDefault(PropertyKey key) throws IllegalStateException {
        String value = get(key);
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
    public <T> T getOrDefault(PropertyKey key, Class<T> targetType) throws IllegalStateException {
        T value = get(key, targetType);
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
