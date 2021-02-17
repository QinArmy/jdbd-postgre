package io.jdbd;

import reactor.util.annotation.Nullable;

public class BindParameterException extends SQLBindParameterException {

    private final int parameterIndex;

    public BindParameterException(String message, int parameterIndex) {
        super(message);
        this.parameterIndex = parameterIndex;
    }

    public BindParameterException(String message, Throwable cause, int parameterIndex) {
        super(message, cause);
        this.parameterIndex = parameterIndex;
    }

    public BindParameterException(int parameterIndex, String messageFormat, Object... args) {
        super(messageFormat, args);
        this.parameterIndex = parameterIndex;
    }

    public BindParameterException(@Nullable Throwable cause, int parameterIndex, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
        this.parameterIndex = parameterIndex;
    }

    public BindParameterException(@Nullable Throwable cause, boolean enableSuppression
            , boolean writableStackTrace, int parameterIndex, String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
        this.parameterIndex = parameterIndex;
    }


    public final int getParameterIndex() {
        return this.parameterIndex;
    }


}
