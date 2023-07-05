package io.jdbd.vendor.util;


import java.util.Objects;

public final class Pair<F, S> {

    public static <F, S> Pair<F, S> create(F first, S second) {
        return new Pair<>(first, second);
    }

    public final F first;

    public final S second;

    private Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.first, this.second);
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Pair) {
            final Pair<?, ?> o = (Pair<?, ?>) obj;
            match = Objects.equals(o.first, this.first) && Objects.equals(o.second, this.second);
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public String toString() {
        return JdbdStrings.builder()
                .append(Pair.class.getName())
                .append("[ first : ")
                .append(this.first)
                .append(" , second : ")
                .append(this.second)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
