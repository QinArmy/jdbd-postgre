package io.jdbd;

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

    public final int getParameterIndex() {
        return this.parameterIndex;
    }


}
