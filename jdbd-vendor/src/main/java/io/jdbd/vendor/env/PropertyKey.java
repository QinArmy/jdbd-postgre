package io.jdbd.vendor.env;

import reactor.util.annotation.Nullable;

@Deprecated
public interface PropertyKey {

    String name();

    String getKey();

    @Nullable
    String getAlias();

    @Nullable
    String getDefault();

    Class<?> getJavaType();

    boolean isCaseSensitive();


}
