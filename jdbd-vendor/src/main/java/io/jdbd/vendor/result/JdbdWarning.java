package io.jdbd.vendor.result;

import io.jdbd.result.Warning;

import java.util.Objects;

public final class JdbdWarning implements Warning {

    private static final JdbdWarning EMPTY = new JdbdWarning("");

    public static JdbdWarning create(String message) {
        Objects.requireNonNull(message, "message");
        final JdbdWarning warning;
        if (message.equals("")) {
            warning = EMPTY;
        } else {
            warning = new JdbdWarning(message);
        }
        return warning;
    }

    private final String message;

    private JdbdWarning(String message) {
        this.message = message;
    }


    @Override
    public String getMessage() {
        return this.message;
    }


}
