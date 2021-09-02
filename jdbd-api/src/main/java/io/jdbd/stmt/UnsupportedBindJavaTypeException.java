package io.jdbd.stmt;

import io.jdbd.JdbdNonSQLException;

public final class UnsupportedBindJavaTypeException extends JdbdNonSQLException {

    private final Class<?> notSupportType;

    public UnsupportedBindJavaTypeException(Class<?> notSupportType) {
        super(String.format("Not supported bind java type[%s]", notSupportType.getName()));
        this.notSupportType = notSupportType;
    }


    public final Class<?> getNotSupportType() {
        return this.notSupportType;
    }


}
