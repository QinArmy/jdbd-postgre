package io.jdbd.vendor.stmt;

import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.nio.file.Path;

public class JdbdParamValue implements ParamValue {

    public static JdbdParamValue wrap(int parameterIndex, @Nullable Object value) {
        return new JdbdParamValue(parameterIndex, value);
    }

    protected final int parameterIndex;

    protected final Object value;

    protected JdbdParamValue(int parameterIndex, @Nullable Object value) {
        if (parameterIndex < 0) {
            throw new IllegalArgumentException(String.format("parameterIndex[%s]", parameterIndex));
        }
        this.parameterIndex = parameterIndex;
        this.value = value;
    }

    @Override
    public final int getParamIndex() {
        return this.parameterIndex;
    }

    @Override
    public final boolean isLongData() {
        Object value = this.value;
        return value instanceof Publisher || value instanceof Path;
    }

    @Nullable
    @Override
    public final Object getValue() {
        return this.value;
    }

    @Override
    public final Object getNonNullValue() throws NullPointerException {
        Object value = this.value;
        if (value == null) {
            throw new NullPointerException("this.value");
        }
        return value;
    }


}
