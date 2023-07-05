package io.jdbd.env;

import io.jdbd.JdbdException;

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
                throw new JdbdException(e.getMessage(), e);
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
                throw new JdbdException(e.getMessage(), e);
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
        public Integer convert(String source) throws JdbdException {
            try {
                final int value;
                if (isHexNumber(source)) {
                    value = Integer.decode(source);
                } else {
                    value = Integer.parseInt(source);
                }
                return value;
            } catch (NumberFormatException e) {
                throw new JdbdException(e.getMessage(), e);
            }
        }
    }

    private static final class StringToLongConverter implements Converter<Long> {

        static final StringToLongConverter INSTANCE = new StringToLongConverter();

        private StringToLongConverter() {
        }

        @Override
        public Class<Long> target() {
            return Long.class;
        }

        @Override
        public Long convert(String source) throws JdbdException {
            return isHexNumber(source) ? Long.decode(source) : Long.parseLong(source);
        }
    }

    private static final class StringToFloatConverter implements Converter<Float> {

        static final StringToFloatConverter INSTANCE = new StringToFloatConverter();

        private StringToFloatConverter() {
        }

        @Override
        public Class<Float> target() {
            return Float.class;
        }

        @Override
        public Float convert(String source) throws IllegalArgumentException {
            return Float.parseFloat(source);
        }
    }

    private static final class StringToDoubleConverter implements Converter<Double> {

        static final StringToDoubleConverter INSTANCE = new StringToDoubleConverter();

        private StringToDoubleConverter() {
        }

        @Override
        public Class<Double> target() {
            return Double.class;
        }

        @Override
        public Double convert(String source) throws IllegalArgumentException {
            return Double.parseDouble(source);
        }
    }

    private static final class StringToBigDecimalConverter implements Converter<BigDecimal> {

        static final StringToBigDecimalConverter INSTANCE = new StringToBigDecimalConverter();

        private StringToBigDecimalConverter() {
        }

        @Override
        public Class<BigDecimal> target() {
            return BigDecimal.class;
        }

        @Override
        public BigDecimal convert(String source) throws IllegalArgumentException {
            return new BigDecimal(source);
        }
    }

    private static final class StringToBigIntegerConverter implements Converter<BigInteger> {

        static final StringToBigIntegerConverter INSTANCE = new StringToBigIntegerConverter();

        private StringToBigIntegerConverter() {
        }

        @Override
        public Class<BigInteger> target() {
            return BigInteger.class;
        }

        @Override
        public BigInteger convert(String source) throws IllegalArgumentException {
            return new BigInteger(source);
        }
    }

    private static final class StringToBooleanConverter implements Converter<Boolean> {

        static final StringToBooleanConverter INSTANCE = new StringToBooleanConverter();

        private StringToBooleanConverter() {
        }


        @Override
        public Class<Boolean> target() {
            return Boolean.class;
        }

        @Override
        public Boolean convert(String source) throws IllegalArgumentException {
            try {
                return Boolean.parseBoolean(source);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("source error", e);
            }
        }
    }

    static final class StringToEnumConverter<T extends Enum<T>> implements Converter<T> {

        @SuppressWarnings("unchecked")
        public static <T extends Enum<T>> StringToEnumConverter<T> getInstance(Class<? extends Enum<?>> enumClass) {
            return new StringToEnumConverter<>((Class<T>) enumClass);
        }

        private final Class<T> enumClass;

        private final Method valueOfTextMethod;

        private StringToEnumConverter(Class<T> enumClass) {
            this.enumClass = enumClass;

            if (NonNameEnum.class.isAssignableFrom(enumClass)) {
                Method method;
                try {
                    method = this.enumClass.getMethod("valueOfText", String.class);
                } catch (NoSuchMethodException e) {
                    throw new IllegalArgumentException(e);
                }
                if (Modifier.isPublic(method.getModifiers())
                        && Modifier.isStatic(method.getModifiers())
                        && method.getReturnType() == this.enumClass) {
                    this.valueOfTextMethod = method;
                } else {
                    throw new IllegalArgumentException(
                            String.format("%s implements %s,but not found valueOfText method."
                                    , enumClass.getName(), NonNameEnum.class.getName()));
                }
            } else {
                this.valueOfTextMethod = null;
            }

        }


        @Override
        public Class<T> target() {
            return this.enumClass;
        }

        @Override
        public T convert(String source) throws IllegalArgumentException {
            final T result;
            if (NonNameEnum.class.isAssignableFrom(this.enumClass)) {
                try {
                    result = this.enumClass.cast(this.valueOfTextMethod.invoke(null, source));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                result = Enum.valueOf(this.enumClass, source);
            }
            return result;
        }


    }


    private static final class StringToPathConverter implements Converter<Path> {

        private static final StringToPathConverter INSTANCE = new StringToPathConverter();

        private StringToPathConverter() {
        }

        @Override
        public Class<Path> target() {
            return Path.class;
        }

        @Override
        public Path convert(String source) throws IllegalArgumentException {
            try {
                return Paths.get(source);
            } catch (Throwable e) {
                throw new IllegalArgumentException(e);
            }
        }

    }

    private static final class StringToCharsetConverter implements Converter<Charset> {

        static final StringToCharsetConverter INSTANCE = new StringToCharsetConverter();

        private StringToCharsetConverter() {
        }

        @Override
        public Class<Charset> target() {
            return Charset.class;
        }

        @Override
        public Charset convert(String source) throws IllegalArgumentException {
            try {
                return Charset.forName(source);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    private static final class StringToUuidConverter implements Converter<UUID> {

        static final StringToUuidConverter INSTANCE = new StringToUuidConverter();

        private StringToUuidConverter() {
        }

        @Override
        public Class<UUID> target() {
            return UUID.class;
        }

        @Override
        public UUID convert(String source) throws IllegalArgumentException {
            try {
                return UUID.fromString(source);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
    }


    /**
     * Determine whether the given {@code value} String indicates a hex number,
     * i.e. needs to be passed into {@code Integer.decode} instead of
     * {@code Integer.valueOf}, etc.
     */
    private static boolean isHexNumber(String value) {
        int index = (value.startsWith("-") ? 1 : 0);
        return (value.startsWith("0x", index) || value.startsWith("0X", index) || value.startsWith("#", index));
    }

}
