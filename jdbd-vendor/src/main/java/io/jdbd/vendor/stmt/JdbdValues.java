package io.jdbd.vendor.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.vendor.util.JdbdStrings;

public abstract class JdbdValues {

    protected JdbdValues() {
        throw new UnsupportedOperationException();
    }


    public static NamedValue namedValue(String name, DataType dataType, @Nullable Object value) {
        return new JdbdNamedValue(name, dataType, value);
    }


    private static abstract class JdbdValue implements Value {

        final DataType type;

        final Object value;

        private JdbdValue(DataType type, @Nullable Object value) {
            this.type = type;
            this.value = value;
        }

        @Override
        public final Object get() {
            return this.value;
        }

        @Override
        public final Object getNonNull() throws NullPointerException {
            final Object value = this.value;
            if (value == null) {
                throw new NullPointerException("value is null");
            }
            return value;
        }

        @Override
        public final DataType getType() {
            return this.type;
        }


    }//JdbdValue


    private static final class JdbdNamedValue extends JdbdValue implements NamedValue {

        private final String name;

        private JdbdNamedValue(String name, DataType type, @Nullable Object value) {
            super(type, value);
            this.name = name;
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public String toString() {
            return JdbdStrings.builder()
                    .append(NamedValue.class.getName())
                    .append("[ name : ")
                    .append(this.name)
                    .append(" , type : ")
                    .append(this.type)
                    .append(" ]") // don't print value.
                    .toString();
        }


    }//JdbdNamedValue


}
