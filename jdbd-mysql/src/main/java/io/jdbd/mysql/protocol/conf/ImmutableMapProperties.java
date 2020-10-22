package io.jdbd.mysql.protocol.conf;

import org.qinarmy.env.ImmutableMapEnvironment;
import org.qinarmy.env.convert.Converter;
import org.qinarmy.env.convert.GenericConverter;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ImmutableMapProperties extends ImmutableMapEnvironment implements Properties {

    public static ImmutableMapProperties getInstance(Map<String, String> source) {
        return new ImmutableMapProperties(source);
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


    @Override
    public String getProperty(PropertyKey key) {
        String defaultText = key.getDefaultValue();
        return defaultText == null ? getProperty(key.getKeyName()) : getProperty(key.getKeyName(), defaultText);
    }

    @Override
    public <T> T getProperty(PropertyKey key, Class<T> targetType) {
        T value = getProperty(key.getKeyName(), targetType);
        if (value == null) {
            String defaultText = key.getDefaultValue();
            if (defaultText != null) {
                Converter<String, T> converter = this.converterManager.getConverter(String.class, targetType);
                if (converter != null) {
                    value = converter.convert(defaultText);
                }

            }
        }
        return value;
    }

    @Override
    public List<String> getPropertyList(PropertyKey key) {
        return getPropertyList(key, String.class);
    }

    @Override
    public <T> List<T> getPropertyList(PropertyKey key, Class<T> targetArrayType) {
        List<T> valueList = getPropertyList(key.getKeyName(), targetArrayType);
        if (valueList.isEmpty()) {
            String defaultText = key.getDefaultValue();
            if (defaultText != null) {
                GenericConverter<String, List<T>, T> converter = this.converterManager.getListConverter(
                        String.class, targetArrayType);
                if (converter != null) {
                    valueList = converter.convert(defaultText);
                }

            }
        }
        return valueList;
    }

    @Override
    public <T> List<T> getPropertyList(PropertyKey key, Class<T> targetArrayType, List<T> defaultList) {
        List<T> valueList = getPropertyList(key, targetArrayType);
        if (valueList.isEmpty()) {
            valueList = defaultList;
        }
        return valueList;
    }

    @Override
    public <T> Set<T> getPropertySet(PropertyKey key, Class<T> targetArrayType) {
        Set<T> valueSet = getPropertySet(key.getKeyName(), targetArrayType);
        if (valueSet.isEmpty()) {
            String defaultText = key.getDefaultValue();
            if (defaultText != null) {
                GenericConverter<String, Set<T>, T> converter = this.converterManager.getSetConverter(
                        String.class, targetArrayType);
                if (converter != null) {
                    valueSet = converter.convert(defaultText);
                }

            }
        }
        return valueSet;
    }

    @Override
    public Set<String> getPropertySet(PropertyKey key) {
        return getPropertySet(key, String.class);
    }

    @Override
    public <T> Set<T> getPropertySet(PropertyKey key, Class<T> targetArrayType, Set<T> defaultSet) {
        Set<T> valueSet = getPropertySet(key.getKeyName(), targetArrayType);
        if (valueSet.isEmpty()) {
            valueSet = defaultSet;
        }
        return valueSet;
    }

    @Override
    public String getRequiredProperty(PropertyKey key) throws IllegalStateException {
        String defaultText = key.getDefaultValue();
        return defaultText == null ? getRequiredProperty(key.getKeyName()) : getProperty(key.getKeyName(), defaultText);
    }

    @Override
    public <T> T getRequiredProperty(PropertyKey key, Class<T> targetType) throws IllegalStateException {
        T value = getProperty(key, targetType);
        if (value == null) {
            throw new IllegalStateException(String.format("not found value for key[%s]", key.getKeyName()));
        }
        return value;
    }
}
