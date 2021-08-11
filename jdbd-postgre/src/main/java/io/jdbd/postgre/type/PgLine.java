package io.jdbd.postgre.type;

import java.util.Objects;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LINE">Lines</a>
 */
public final class PgLine {


    /**
     * @param textValue like { A, B, C }
     * @throws IllegalArgumentException when textValue format error.
     */
    public static PgLine parse(final String textValue) {
        final String format = "Text[%s] isn't postgre line";
        if (!textValue.startsWith("{") || !textValue.endsWith("}")) {
            throw new IllegalArgumentException(String.format(format, textValue));
        }
        final int end = textValue.length() - 1;

        final double a, b, c;
        int left = 1, right;
        // a
        right = textValue.indexOf(',', left);
        if (right < 0) {
            throw new IllegalArgumentException(String.format(format, textValue));
        }
        a = Double.parseDouble(textValue.substring(left, right).trim());
        // b
        left = right + 1;
        if (left >= end) {
            throw new IllegalArgumentException(String.format(format, textValue));
        }
        right = textValue.indexOf(',', left);
        if (right < 0) {
            throw new IllegalArgumentException(String.format(format, textValue));
        }
        b = Double.parseDouble(textValue.substring(left, right).trim());
        // c
        left = right + 1;
        c = Double.parseDouble(textValue.substring(left, end).trim());
        return new PgLine(a, b, c);
    }

    public static PgLine create(double a, double b, double c) {
        return new PgLine(a, b, c);
    }

    private final double a;

    private final double b;

    private final double c;

    private PgLine(double a, double b, double c) {
        if (a == 0 && b == 0) {
            throw new IllegalArgumentException(String.format("A[%s] and B[%s] are not both zero", a, b));
        }
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
    public final boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof PgLine) {
            PgLine p = (PgLine) obj;
            match = p.a == this.a
                    && p.b == this.b
                    && p.c == this.c;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return new StringBuilder("{")
                .append(this.a)
                .append(",")
                .append(this.b)
                .append(",")
                .append(this.c)
                .append("}")
                .toString();
    }


}
