package io.jdbd.postgre.config;

import io.jdbd.postgre.PgServerVersion;
import org.qinarmy.env.convert.Converter;

import java.util.function.Consumer;

abstract class Converters {

    private Converters() {
        throw new UnsupportedOperationException();
    }


    static void registerConverter(Consumer<Converter<?, ?>> consumer) {
        consumer.accept(StringToServerVersionConverter.INSTANCE);
    }


    private static final class StringToServerVersionConverter implements Converter<String, PgServerVersion> {

        private static final StringToServerVersionConverter INSTANCE = new StringToServerVersionConverter();

        @Override
        public final Class<String> sourceType() {
            return String.class;
        }

        @Override
        public final Class<PgServerVersion> target() {
            return PgServerVersion.class;
        }

        @Override
        public final PgServerVersion convert(String source) throws IllegalArgumentException {
            return PgServerVersion.from(source);
        }

    }


}
