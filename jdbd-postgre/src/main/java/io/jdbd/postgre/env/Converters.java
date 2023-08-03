package io.jdbd.postgre.env;

import io.jdbd.postgre.PgServerVersion;
import io.qinarmy.env.convert.Converter;

import java.util.function.Consumer;

abstract class Converters {

    private Converters() {
        throw new UnsupportedOperationException();
    }


    static void registerConverter(Consumer<Converter<?>> consumer) {
        consumer.accept(StringToServerVersionConverter.INSTANCE);
    }


    private static final class StringToServerVersionConverter implements Converter<PgServerVersion> {

        private static final StringToServerVersionConverter INSTANCE = new StringToServerVersionConverter();

        @Override
        public Class<PgServerVersion> target() {
            return PgServerVersion.class;
        }

        @Override
        public PgServerVersion convert(String source) throws IllegalArgumentException {
            return PgServerVersion.from(source);
        }

    }


}
