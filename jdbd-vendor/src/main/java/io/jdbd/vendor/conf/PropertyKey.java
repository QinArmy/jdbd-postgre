package io.jdbd.vendor.conf;

import reactor.util.annotation.Nullable;

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
