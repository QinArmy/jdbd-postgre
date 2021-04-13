package io.jdbd.vendor.conf;

import reactor.util.annotation.Nullable;

public interface IPropertyKey {

    String name();

    String getKey();

    @Nullable
    String getAlias();

    @Nullable
    String getDefault();

    Class<?> getJavaType();


}
