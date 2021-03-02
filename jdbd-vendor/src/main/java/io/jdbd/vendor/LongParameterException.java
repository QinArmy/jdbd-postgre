package io.jdbd.vendor;

import io.jdbd.BindParameterException;
import io.jdbd.lang.Nullable;

@Deprecated
public class LongParameterException extends BindParameterException {

    public LongParameterException(int parameterIndex, String messageFormat, Object... args) {
        super(parameterIndex, messageFormat, args);
    }

    public LongParameterException(@Nullable Throwable cause, int parameterIndex, String messageFormat, Object... args) {
        super(cause, parameterIndex, messageFormat, args);
    }


}
