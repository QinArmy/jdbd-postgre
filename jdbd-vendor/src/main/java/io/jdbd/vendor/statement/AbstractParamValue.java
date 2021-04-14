package io.jdbd.vendor.statement;


import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

public abstract class AbstractParamValue implements ParamValue {

    protected final int parameterIndex;

    protected final Object value;

    protected AbstractParamValue(int parameterIndex, @Nullable Object value) {
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
        final boolean longData;
        if (value == null) {
            longData = false;
        } else if (value instanceof Publisher) {
            longData = true;
        } else if (value instanceof byte[]) {
            longData = ((byte[]) value).length > getByteLengthBoundary();
        } else if (value instanceof String) {
            longData = ((String) value).length() > getStringLengthBoundary();
        } else {
            longData = false;
        }
        return longData;
    }


    @Nullable
    @Override
    public final Object getValue() {
        return this.value;
    }

    @Override
    public final Object getRequiredValue() throws NullPointerException {
        Object value = this.value;
        if (value == null) {
            throw new NullPointerException(String.format("Bind parameter[%s] value is null.", this.parameterIndex));
        }
        return value;
    }


    protected abstract int getByteLengthBoundary();

    protected abstract int getStringLengthBoundary();


}
