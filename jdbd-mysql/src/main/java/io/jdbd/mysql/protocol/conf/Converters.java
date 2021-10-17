package io.jdbd.mysql.protocol.conf;

import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.qinarmy.env.convert.Converter;

import java.util.function.Consumer;

abstract class Converters {

    private Converters() {
    }

    static void registerConverter(Consumer<Converter<?>> consumer) {
        consumer.accept(StringToServerVersionConverter.INSTANCE);
    }


    private static final class StringToServerVersionConverter implements Converter<MySQLServerVersion> {

        private static final StringToServerVersionConverter INSTANCE = new StringToServerVersionConverter();

        @Override
        public Class<MySQLServerVersion> target() {
            return MySQLServerVersion.class;
        }

        @Override
        public MySQLServerVersion convert(String source) throws IllegalArgumentException {
            return MySQLServerVersion.from(source);
        }

    }

}
