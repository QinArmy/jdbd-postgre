package io.jdbd.vendor.conf;

import io.jdbd.PropertyException;
import io.qinarmy.env.ImmutableMapEnvironment;
import io.qinarmy.env.convert.Converter;
import io.qinarmy.env.convert.ConverterManager;
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
        return get(key.getKey());
    }

    @Override
    public String get(PropertyKey key, String defaultValue) {
        return get(key.getKey(), defaultValue);
    }

    @Override
    public <T> T get(PropertyKey key, Class<T> targetType) {
        try {
            return get(key.getKey(), targetType);
        } catch (Exception e) {
            throw cannotConvertException(key, targetType, e);
        }
    }


    @Override
    public <T> T get(PropertyKey key, Class<T> targetType, T defaultValue) {
        try {
            return get(key.getKey(), targetType, defaultValue);
        } catch (Exception e) {
            throw cannotConvertException(key, targetType, e);
        }
    }

    @Override
    public List<String> getList(PropertyKey key) {
        try {
            return getList(key.getKey(), String.class);
        } catch (Exception e) {
            throw cannotConvertException(key, List.class, e);
        }
    }

    @Override
    public <T> List<T> getList(PropertyKey key, Class<T> elementType) {
        try {
            return getList(key.getKey(), elementType);
        } catch (Exception e) {
            throw cannotConvertException(key, elementType, e);
        }
    }

    @Override
    public <T> List<T> getList(PropertyKey key, Class<T> elementType, List<T> defaultList) {
        try {
            return getList(key.getKey(), elementType, defaultList);
        } catch (Exception e) {
            throw cannotConvertException(key, elementType, e);
        }
    }

    @Override
    public <T> Set<T> getSet(PropertyKey key, Class<T> elementType) {
        try {
            return getSet(key.getKey(), elementType);
        } catch (Exception e) {
            throw cannotConvertException(key, elementType, e);
        }
    }

    @Override
    public Set<String> getSet(PropertyKey key) {
        return getSet(key.getKey(), String.class);
    }

    @Override
    public <T> Set<T> getSet(PropertyKey key, Class<T> elementType, Set<T> defaultSet) {
        try {
            return getSet(key.getKey(), elementType, defaultSet);
        } catch (Exception e) {
            throw cannotConvertException(key, elementType, e);
        }
    }

    @Override
    public String getNonNull(PropertyKey key) {
        try {
            return getNonNull(key.getKey());
        } catch (Exception e) {
            throw cannotConvertException(key, String.class, e);
        }
    }

    @Override
    public <T> T getNonNull(PropertyKey key, Class<T> targetType) throws IllegalStateException {
        try {
            return getNonNull(key.getKey(), targetType);
        } catch (Exception e) {
            throw cannotConvertException(key, targetType, e);
        }
    }

    @Override
    public String getOrDefault(PropertyKey key) throws IllegalStateException {
        String value = get(key);
        if (value != null) {
            return value;
        }
        String defaultText = key.getDefault();
        if (defaultText == null) {
            throw new PropertyException(key.getKey(), String.format("not found value for key[%s]", key.getKey()));
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
            throw new PropertyException(key.getKey(), String.format("not found value for key[%s]", key.getKey()));
        } else {
            Converter<T> converter = this.converterManager.getConverter(targetType);
            if (converter == null) {
                throw new PropertyException(
                        key.getKey(), String.format("not found Converter for [%s,%s]", String.class, targetType));
            } else {
                try {
                    value = converter.convert(defaultText);
                } catch (Exception e) {
                    throw cannotConvertException(key, targetType, e);
                }
            }
        }
        return value;
    }

    private static PropertyException cannotConvertException(PropertyKey key, Class<?> targetType, Throwable e) {
        String m = String.format("Property[%s] value couldn't convert to %s.", key, targetType.getName());
        return new PropertyException(key.getKey(), m);
    }


}
