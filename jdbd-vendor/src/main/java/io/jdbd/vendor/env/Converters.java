package io.jdbd.vendor.env;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdNumbers;
import reactor.netty.resources.ConnectionProvider;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class Converters {

    private Converters() {
        throw new UnsupportedOperationException();
    }


    public static void registerDefaultConverter(final BiConsumer<Class<?>, BiFunction<Class<?>, String, ?>> consumer) {

        consumer.accept(String.class, Converters::stringToString);
        consumer.accept(Byte.class, Converters::toByte);
        consumer.accept(Short.class, Converters::toShort);
        consumer.accept(Integer.class, Converters::toInt);

        consumer.accept(Long.class, Converters::toLong);
        consumer.accept(Float.class, Converters::toFloat);
        consumer.accept(Double.class, Converters::toDouble);
        consumer.accept(BigDecimal.class, Converters::toBigDecimal);

        consumer.accept(BigInteger.class, Converters::toBigInteger);
        consumer.accept(Boolean.class, Converters::toBoolean);
        consumer.accept(Path.class, Converters::toPath);
        consumer.accept(Charset.class, Converters::toCharset);

        consumer.accept(UUID.class, Converters::toUUID);
        consumer.accept(ZoneOffset.class, Converters::toZoneOffset);

        consumer.accept(ConnectionProvider.class, Converters::createInstanceFromGetInstanceMethod);


    }


    private static String stringToString(final Class<?> targetType, final String source) {
        return source;
    }


    private static Byte toByte(final Class<?> targetType, final String source) throws JdbdException {
        try {
            final byte value;
            if (JdbdNumbers.isHexNumber(source)) {
                value = Byte.decode(source);
            } else {
                value = Byte.parseByte(source);
            }
            return value;
        } catch (NumberFormatException e) {
            throw convertFailure(source, targetType, e);
        }
    }

    private static Short toShort(final Class<?> targetType, final String source) throws JdbdException {
        try {
            final short value;
            if (JdbdNumbers.isHexNumber(source)) {
                value = Short.decode(source);
            } else {
                value = Short.parseShort(source);
            }
            return value;
        } catch (NumberFormatException e) {
            throw convertFailure(source, targetType, e);
        }
    }


    private static Integer toInt(final Class<?> targetType, final String source) throws JdbdException {
        try {
            final int value;
            if (JdbdNumbers.isHexNumber(source)) {
                value = Integer.decode(source);
            } else {
                value = Integer.parseInt(source);
            }
            return value;
        } catch (NumberFormatException e) {
            throw convertFailure(source, targetType, e);
        }
    }

    private static Long toLong(final Class<?> targetType, final String source) throws JdbdException {
        try {
            final long value;
            if (JdbdNumbers.isHexNumber(source)) {
                value = Long.decode(source);
            } else {
                value = Long.parseLong(source);
            }
            return value;
        } catch (NumberFormatException e) {
            throw convertFailure(source, targetType, e);
        }
    }

    private static Float toFloat(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return Float.parseFloat(source);
        } catch (NumberFormatException e) {
            throw convertFailure(source, targetType, e);
        }
    }

    private static Double toDouble(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return Double.parseDouble(source);
        } catch (NumberFormatException e) {
            throw convertFailure(source, targetType, e);
        }
    }

    private static BigDecimal toBigDecimal(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return new BigDecimal(source);
        } catch (NumberFormatException e) {
            throw convertFailure(source, targetType, e);
        }
    }


    private static BigInteger toBigInteger(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return new BigInteger(source);
        } catch (NumberFormatException e) {
            throw convertFailure(source, targetType, e);
        }
    }


    private static Boolean toBoolean(final Class<?> targetType, final String source) throws JdbdException {
        final boolean value;
        switch (source.toLowerCase(Locale.ROOT)) {
            case "true":
            case "on":
            case "yes":
                value = true;
                break;
            case "false":
            case "off":
            case "no":
                value = false;
                break;
            default:
                throw convertFailure(source, targetType, null);
        }
        return value;
    }

    private static Path toPath(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return Paths.get(source);
        } catch (Exception e) {
            throw convertFailure(source, targetType, e);
        }
    }


    private static Charset toCharset(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return Charset.forName(source);
        } catch (Exception e) {
            throw convertFailure(source, targetType, e);
        }
    }

    private static UUID toUUID(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return UUID.fromString(source);
        } catch (Exception e) {
            throw convertFailure(source, targetType, e);
        }
    }

    private static ZoneOffset toZoneOffset(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return ZoneOffset.of(source);
        } catch (Exception e) {
            throw convertFailure(source, targetType, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Enum<T>> T toEnum(final Class<?> targetType, final String source) throws JdbdException {
        try {
            return Enum.valueOf((Class<T>) targetType, source);
        } catch (IllegalArgumentException e) {
            throw convertFailure(source, targetType, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Enum<T> & TextEnum> T toTextEnum(final Class<?> targetType, final String source)
            throws JdbdException {
        final T value;
        value = (T) TextEnumHelper.getTextMap(targetType).get(source);
        if (value == null) {
            throw convertFailure(source, targetType, null);
        }
        return value;
    }


    public static Object createInstanceFromGetInstanceMethod(final Class<?> interfaceClass, final String source) {

        try {
            final Class<?> implClass;
            implClass = Class.forName(source);

            final String methodName = "getInstance";
            final Method method;
            method = implClass.getMethod(methodName);

            final int modifier;
            modifier = method.getModifiers();

            final boolean match;
            match = Modifier.isPublic(modifier)
                    && Modifier.isStatic(modifier)
                    && method.getParameterCount() == 0
                    && interfaceClass.isAssignableFrom(method.getReturnType());

            if (!match) {
                String m = String.format("%s no public static factory method %s()", source, methodName);
                throw new JdbdException(m);
            }
            return method.invoke(null);
        } catch (JdbdException e) {
            throw e;
        } catch (Exception e) {
            throw convertFailure(source, interfaceClass, e);
        }

    }


    private static class TextEnumHelper {

        private static final ConcurrentMap<Class<?>, Map<String, ? extends TextEnum>> INSTANCE_MAP =
                JdbdCollections.concurrentHashMap();

        private static final Function<Class<?>, Map<String, ? extends TextEnum>> FUNCTION = TextEnumHelper::createEnumMap;


        @SuppressWarnings("unchecked")
        private static <T extends TextEnum> Map<String, T> getTextMap(Class<?> enumClass) {
            return (Map<String, T>) INSTANCE_MAP.computeIfAbsent(enumClass, FUNCTION);
        }

        @SuppressWarnings("unchecked")
        private static <T extends TextEnum> Map<String, T> createEnumMap(final Class<?> enumClass) {
            if (!Enum.class.isAssignableFrom(enumClass) || TextEnum.class.isAssignableFrom(enumClass)) {
                throw new JdbdException(String.format("%s isn't %s.", enumClass.getName(), TextEnum.class));
            }
            for (Field field : enumClass.getDeclaredFields()) {
                if (!Modifier.isFinal(field.getModifiers())) {
                    throw new JdbdException(String.format("%s %s isn't final.", enumClass.getName(), field.getName()));
                }
            }
            final Class<T> clazz = (Class<T>) enumClass;
            final T[] array;
            array = clazz.getEnumConstants();
            final Map<String, T> map = JdbdCollections.hashMap((int) (array.length / 0.75f));

            String text;
            for (T e : array) {
                text = e.text();
                if (map.putIfAbsent(e.text(), e) != null) {
                    throw new JdbdException(String.format("%s text[%s] duplication.", enumClass.getName(), text));
                }
            }
            return JdbdCollections.unmodifiableMap(map);
        }

    }//TextEnumHelper


    private static JdbdException convertFailure(@Nullable String source, Class<?> javaType, @Nullable Throwable clause) {
        final String m = String.format("%s couldn't convert to %s .", source, javaType.getName());
        final JdbdException e;
        if (clause == null) {
            e = new JdbdException(m);
        } else {
            e = new JdbdException(m, clause);
        }
        return e;
    }

}
