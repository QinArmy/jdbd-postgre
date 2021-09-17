package io.jdbd.postgre.type;

import java.util.Objects;

public final class PgLine {

    /**
     * @param value format:{a,b,c}
     * @throws IllegalArgumentException when value format error.
     */
    static PgLine from(final String value) {

        if (!value.startsWith("{") || !value.endsWith("}")) {
            throw new IllegalArgumentException("format error");
        }
        final double[] numbers = new double[3];
        final char[] chars = new char[]{',', ',', '}'};
        final int length = value.length();
        for (int i = 0, from = 1, to; i < numbers.length; i++) {
            if (from >= length) {
                throw new IllegalArgumentException("format error");
            }
            to = value.indexOf(chars[i], from);
            if (to < 0) {
                throw new IllegalArgumentException("format error");
            }
            numbers[i] = Double.parseDouble(value.substring(from, to).trim());
            from = to + 1;
        }
        return new PgLine(numbers[0], numbers[1], numbers[2]);
    }

    private final double a;

    private final double b;

    private final double c;

    private PgLine(double a, double b, double c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }


    public final double getA() {
        return this.a;
    }

    public final double getB() {
        return this.b;
    }

    public final double getC() {
        return this.c;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(this.a, this.b, this.c);
    }

    @Override
    public final boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof PgLine) {
            final PgLine v = (PgLine) obj;
            match = v.a == this.a
                    && v.b == this.b
                    && v.c == this.c;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return String.format("{%s,%s,%s}", this.a, this.b, this.c);
    }

}
