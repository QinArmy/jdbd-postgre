package io.jdbd.vendor.stmt;


import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.nio.file.Path;

public abstract class AbstractParamValue implements ParamValue {

    protected final int parameterIndex;

    protected final Object value;

    protected AbstractParamValue(int parameterIndex, @Nullable Object value) {
        this.parameterIndex = parameterIndex;
        this.value = value;
    }


    @Override
    public final int getIndex() {
        return this.parameterIndex;
    }

    @Override
    public final boolean isLongData() {
        Object value = this.value;
        final boolean longData;
        if (value == null) {
            longData = false;
        } else if (value instanceof Publisher
                || value instanceof Path) {
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
    public final Object get() {
        return this.value;
    }

    @Override
    public final Object getNonNull() throws NullPointerException {
        Object value = this.value;
        if (value == null) {
            throw new NullPointerException(String.format("Bind parameter[%s] value is null.", this.parameterIndex));
        }
        return value;
    }


    protected abstract int getByteLengthBoundary();

    protected abstract int getStringLengthBoundary();


}
