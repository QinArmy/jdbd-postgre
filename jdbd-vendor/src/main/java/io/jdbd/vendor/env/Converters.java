package io.jdbd.vendor.env;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.vendor.util.JdbdCollections;

import java.lang.reflect.Field;
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
import java.util.function.Consumer;

public abstract class Converters {

    private Converters() {
        throw new UnsupportedOperationException();
    }


    public static void registerDefaultConverter(final Consumer<Converter<?>> consumer) {

        consumer.accept(StringToStringConverter.INSTANCE);

        consumer.accept(StringToByteConverter.INSTANCE);
        consumer.accept(StringToShortConverter.INSTANCE);
        consumer.accept(StringToIntegerConverter.INSTANCE);
        consumer.accept(StringToLongConverter.INSTANCE);

        consumer.accept(StringToFloatConverter.INSTANCE);
        consumer.accept(StringToDoubleConverter.INSTANCE);
        consumer.accept(StringToBigDecimalConverter.INSTANCE);
        consumer.accept(StringToBigIntegerConverter.INSTANCE);

        consumer.accept(StringToBooleanConverter.INSTANCE);
        consumer.accept(StringToCharsetConverter.INSTANCE);
        consumer.accept(StringToPathConverter.INSTANCE);
        consumer.accept(StringToUuidConverter.INSTANCE);

        consumer.accept(StringToZoneOffsetConverter.INSTANCE);

    }

    private static final class StringToStringConverter implements Converter<String> {

        static final StringToStringConverter INSTANCE = new StringToStringConverter();

        private StringToStringConverter() {
        }

        @Override
        public Class<String> target() {
            return String.class;
        }

        @Override
        public String convert(String source) throws JdbdException {
            return source;
        }
    }

    private static final class StringToByteConverter implements Converter<Byte> {

        static final StringToByteConverter INSTANCE = new StringToByteConverter();

        private StringToByteConverter() {
        }

        @Override
        public Class<Byte> target() {
            return Byte.class;
        }

        @Override
        public Byte convert(final String source) throws JdbdException {
            try {
                final byte value;
                if (isHexNumber(source)) {
                    value = Byte.decode(source);
                } else {
                    value = Byte.parseByte(source);
                }
                return value;
            } catch (NumberFormatException e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToByteConverter

    private static final class StringToShortConverter implements Converter<Short> {

        static final StringToShortConverter INSTANCE = new StringToShortConverter();

        private StringToShortConverter() {
        }

        @Override
        public Class<Short> target() {
            return Short.class;
        }

        @Override
        public Short convert(final String source) throws JdbdException {
            try {
                final short value;
                if (isHexNumber(source)) {
                    value = Short.decode(source);
                } else {
                    value = Short.parseShort(source);
                }
                return value;
            } catch (NumberFormatException e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToShortConverter

    private static final class StringToIntegerConverter implements Converter<Integer> {

        static final StringToIntegerConverter INSTANCE = new StringToIntegerConverter();

        private StringToIntegerConverter() {
        }

        @Override
        public Class<Integer> target() {
            return Integer.class;
        }

        @Override
        public Integer convert(final String source) throws JdbdException {
            try {
                final int value;
                if (isHexNumber(source)) {
                    value = Integer.decode(source);
                } else {
                    value = Integer.parseInt(source);
                }
                return value;
            } catch (NumberFormatException e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToIntegerConverter

    private static final class StringToLongConverter implements Converter<Long> {

        static final StringToLongConverter INSTANCE = new StringToLongConverter();

        private StringToLongConverter() {
        }

        @Override
        public Class<Long> target() {
            return Long.class;
        }

        @Override
        public Long convert(final String source) throws JdbdException {
            try {
                final long value;
                if (isHexNumber(source)) {
                    value = Long.decode(source);
                } else {
                    value = Long.parseLong(source);
                }
                return value;
            } catch (NumberFormatException e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToLongConverter

    private static final class StringToFloatConverter implements Converter<Float> {

        static final StringToFloatConverter INSTANCE = new StringToFloatConverter();

        private StringToFloatConverter() {
        }

        @Override
        public Class<Float> target() {
            return Float.class;
        }

        @Override
        public Float convert(String source) throws JdbdException {
            try {
                return Float.parseFloat(source);
            } catch (NumberFormatException e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToFloatConverter

    private static final class StringToDoubleConverter implements Converter<Double> {

        static final StringToDoubleConverter INSTANCE = new StringToDoubleConverter();

        private StringToDoubleConverter() {
        }

        @Override
        public Class<Double> target() {
            return Double.class;
        }

        @Override
        public Double convert(String source) throws JdbdException {
            try {
                return Double.parseDouble(source);
            } catch (NumberFormatException e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToDoubleConverter

    private static final class StringToBigDecimalConverter implements Converter<BigDecimal> {

        static final StringToBigDecimalConverter INSTANCE = new StringToBigDecimalConverter();

        private StringToBigDecimalConverter() {
        }

        @Override
        public Class<BigDecimal> target() {
            return BigDecimal.class;
        }

        @Override
        public BigDecimal convert(String source) throws JdbdException {
            try {
                return new BigDecimal(source);
            } catch (NumberFormatException e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToBigDecimalConverter

    private static final class StringToBigIntegerConverter implements Converter<BigInteger> {

        static final StringToBigIntegerConverter INSTANCE = new StringToBigIntegerConverter();

        private StringToBigIntegerConverter() {
        }

        @Override
        public Class<BigInteger> target() {
            return BigInteger.class;
        }

        @Override
        public BigInteger convert(String source) throws JdbdException {
            try {
                return new BigInteger(source);
            } catch (NumberFormatException e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToBigIntegerConverter

    private static final class StringToBooleanConverter implements Converter<Boolean> {

        static final StringToBooleanConverter INSTANCE = new StringToBooleanConverter();

        private StringToBooleanConverter() {
        }


        @Override
        public Class<Boolean> target() {
            return Boolean.class;
        }

        @Override
        public Boolean convert(final String source) throws JdbdException {
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
                    throw convertFailure(source, target(), null);
            }
            return value;
        }

    }//StringToBooleanConverter

    static final class StringToEnumConverter<T extends Enum<T>> implements Converter<T> {


        static <T extends Enum<T>> StringToEnumConverter<T> getInstance(Class<T> enumClass) {
            return new StringToEnumConverter<>(enumClass);
        }

        private final Class<T> enumClass;


        private StringToEnumConverter(Class<T> enumClass) {
            this.enumClass = enumClass;
        }


        @Override
        public Class<T> target() {
            return this.enumClass;
        }

        @Override
        public T convert(String source) throws JdbdException {
            try {
                return Enum.valueOf(this.enumClass, source);
            } catch (IllegalArgumentException e) {
                throw convertFailure(source, target(), e);
            }
        }


    }//StringToEnumConverter


    static final class StringToTextEnumConverter<T extends TextEnum> implements Converter<T> {


        static <T extends TextEnum> StringToTextEnumConverter<T> getInstance(Class<T> enumClass) {
            if (!Enum.class.isAssignableFrom(enumClass)) {
                throw new JdbdException(String.format("%s isn't %s.", enumClass.getName(), TextEnum.class));
            }
            for (Field field : enumClass.getDeclaredFields()) {
                if (!Modifier.isFinal(field.getModifiers())) {
                    throw new JdbdException(String.format("%s %s isn't final.", enumClass.getName(), field.getName()));
                }
            }
            return new StringToTextEnumConverter<>(enumClass);
        }

        private final Class<T> enumClass;

        private final Map<String, T> enumMap;

        private StringToTextEnumConverter(final Class<T> enumClass) {
            this.enumClass = enumClass;

            final T[] array;
            array = enumClass.getEnumConstants();
            final Map<String, T> map = JdbdCollections.hashMap((int) (array.length / 0.75f));

            String text;
            for (T e : array) {
                text = e.text();
                if (map.putIfAbsent(e.text(), e) != null) {
                    throw new JdbdException(String.format("%s text[%s] duplication.", enumClass.getName(), text));
                }
            }

            this.enumMap = JdbdCollections.unmodifiableMap(map);
        }


        @Override
        public Class<T> target() {
            return this.enumClass;
        }

        @Override
        public T convert(String source) throws JdbdException {
            final T value;
            value = this.enumMap.get(source);
            if (value == null) {
                throw convertFailure(source, target(), null);
            }
            return value;
        }


    }//StringToTextEnumConverter

    private static final class StringToPathConverter implements Converter<Path> {

        private static final StringToPathConverter INSTANCE = new StringToPathConverter();

        private StringToPathConverter() {
        }

        @Override
        public Class<Path> target() {
            return Path.class;
        }

        @Override
        public Path convert(String source) throws JdbdException {
            try {
                return Paths.get(source);
            } catch (Exception e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToPathConverter

    private static final class StringToCharsetConverter implements Converter<Charset> {

        static final StringToCharsetConverter INSTANCE = new StringToCharsetConverter();

        private StringToCharsetConverter() {
        }

        @Override
        public Class<Charset> target() {
            return Charset.class;
        }

        @Override
        public Charset convert(String source) throws JdbdException {
            try {
                return Charset.forName(source);
            } catch (Exception e) {
                throw convertFailure(source, target(), e);
            }
        }
    }//StringToCharsetConverter

    private static final class StringToUuidConverter implements Converter<UUID> {

        static final StringToUuidConverter INSTANCE = new StringToUuidConverter();

        private StringToUuidConverter() {
        }

        @Override
        public Class<UUID> target() {
            return UUID.class;
        }

        @Override
        public UUID convert(String source) throws JdbdException {
            try {
                return UUID.fromString(source);
            } catch (Exception e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToUuidConverter


    private static final class StringToZoneOffsetConverter implements Converter<ZoneOffset> {

        static final StringToZoneOffsetConverter INSTANCE = new StringToZoneOffsetConverter();

        private StringToZoneOffsetConverter() {
        }

        @Override
        public Class<ZoneOffset> target() {
            return ZoneOffset.class;
        }

        @Override
        public ZoneOffset convert(String source) throws JdbdException {
            try {
                return ZoneOffset.of(source);
            } catch (Exception e) {
                throw convertFailure(source, target(), e);
            }
        }

    }//StringToZoneOffsetConverter

    /**
     * Determine whether the given {@code value} String indicates a hex number,
     * i.e. needs to be passed into {@code Integer.decode} instead of
     * {@code Integer.valueOf}, etc.
     */
    private static boolean isHexNumber(String value) {
        int index = (value.startsWith("-") ? 1 : 0);
        return (value.startsWith("0x", index) || value.startsWith("0X", index) || value.startsWith("#", index));
    }


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
