package io.jdbd.postgre.config;

import io.jdbd.postgre.ServerVersion;
import org.qinarmy.env.convert.Converter;

import java.util.function.Consumer;

abstract class Converters {

    private Converters() {
        throw new UnsupportedOperationException();
    }


    static void registerConverter(Consumer<Converter<?, ?>> consumer) {
        consumer.accept(StringToServerVersionConverter.INSTANCE);
    }


    private static final class StringToServerVersionConverter implements Converter<String, ServerVersion> {

        private static final StringToServerVersionConverter INSTANCE = new StringToServerVersionConverter();

        @Override
        public final Class<String> sourceType() {
            return String.class;
        }

        @Override
        public final Class<ServerVersion> target() {
            return ServerVersion.class;
        }

        @Override
        public final ServerVersion convert(String source) throws IllegalArgumentException {
            return ServerVersion.from(source);
        }

    }


}
