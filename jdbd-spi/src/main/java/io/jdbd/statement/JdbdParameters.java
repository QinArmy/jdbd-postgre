package io.jdbd.statement;

import io.jdbd.lang.Nullable;

import java.util.Objects;

/**
 * <p>
 * This class provider the method create {@link Parameter}
 * </p>
 *
 * @since 1.0
 */
abstract class JdbdParameters {

    private JdbdParameters() {
        throw new UnsupportedOperationException();
    }

    static InOutParameter inoutParam(@Nullable String name, @Nullable Object value) {
        if (name == null) {
            throw new NullPointerException("inout parameter name must non-null");
        } else if (value instanceof OutParameter) {
            String m = String.format("value must be non-%s", OutParameter.class.getName());
            throw new IllegalArgumentException(m);
        }
        return new JdbdInOutParameter(name, value);
    }

    static OutParameter outParam(final @Nullable String name) {
        if (name == null) {
            throw new NullPointerException("out parameter name must non-null");
        }
        final OutParameter parameter;
        if (name.equals("")) {
            parameter = JdbdOutParameter.EMPTY;
        } else {
            parameter = new JdbdOutParameter(name);
        }
        return parameter;
    }

    private static final class JdbdOutParameter implements OutParameter {

        private static final JdbdOutParameter EMPTY = new JdbdOutParameter("");

        private final String name;

        private JdbdOutParameter(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.name);
        }

        @Override
        public boolean equals(final Object obj) {
            final boolean match;
            if (obj == this) {
                match = true;
            } else if (obj instanceof JdbdOutParameter) {
                match = ((JdbdOutParameter) obj).name.equals(this.name);
            } else {
                match = false;
            }
            return match;
        }

        @Override
        public String toString() {
            return String.format("%s[ name : %s , hash : %s]",
                    getClass().getName(),
                    this.name,
                    System.identityHashCode(this)
            );
        }


    }// JdbdOutParameter

    /**
     * <p>
     * This class is standard implementation of {@link InOutParameter}.
     * </p>
     *
     * @since 1.0
     */
    private static final class JdbdInOutParameter implements InOutParameter {


        private final String name;


        private final Object value;

        /**
         * private constructor
         */
        private JdbdInOutParameter(String name, @Nullable Object value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public Object value() {
            return this.value;
        }


        @Override
        public int hashCode() {
            return Objects.hash(this.name, this.value);
        }

        @Override
        public boolean equals(final Object obj) {
            final boolean match;
            if (obj == this) {
                match = true;
            } else if (obj instanceof JdbdInOutParameter) {
                final JdbdInOutParameter o = (JdbdInOutParameter) obj;
                match = o.name.equals(this.name) && Objects.equals(o.value, this.value);
            } else {
                match = false;
            }
            return match;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append(this.getClass().getName())
                    .append("[ name : ")
                    .append(this.name)
                    .append(" , value : ");
            if (this.value instanceof String) {
                builder.append('?');
            } else {
                builder.append(this.value);
            }
            return builder.append(" , hash : ")
                    .append(System.identityHashCode(this))
                    .append(" ]")
                    .toString();
        }


    }//JdbdOutParameter


}
