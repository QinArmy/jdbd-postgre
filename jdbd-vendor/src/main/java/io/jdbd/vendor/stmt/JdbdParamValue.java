package io.jdbd.vendor.stmt;

import reactor.util.annotation.Nullable;

public class JdbdParamValue implements ParamValue {

    public static JdbdParamValue wrap(int index, @Nullable Object value) {
        return new JdbdParamValue(index, value);
    }

    protected final int index;

    protected final Object value;

    protected JdbdParamValue(int index, @Nullable Object value) {
        if (index < 0) {
            throw new IllegalArgumentException(String.format("index[%s]", index));
        }
        this.index = index;
        this.value = value;
    }

    @Override
    public final int getIndex() {
        return this.index;
    }


    @Nullable
    @Override
    public final Object get() {
        return this.value;
    }

    @Override
    public final Object getNonNull() throws NullPointerException {
        final Object value = this.value;
        if (value == null) {
            throw new NullPointerException("this.value");
        }
        return value;
    }


}
