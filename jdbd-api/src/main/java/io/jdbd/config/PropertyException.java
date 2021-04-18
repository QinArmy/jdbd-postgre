package io.jdbd.config;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;

public class PropertyException extends JdbdNonSQLException {

    private final String propertyName;

    public PropertyException(String propertyName, String messageFormat, Object... args) {
        super(messageFormat, args);
        this.propertyName = propertyName;
    }

    public PropertyException(@Nullable Throwable cause, String propertyName, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
        this.propertyName = propertyName;
    }

    public final String getPropertyName() {
        return propertyName;
    }
}
