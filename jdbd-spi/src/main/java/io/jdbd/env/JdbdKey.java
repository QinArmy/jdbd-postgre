package io.jdbd.env;

import io.jdbd.lang.Nullable;

public interface JdbdKey<T> {

     String keyName();

     Class<T> javaType();

     @Nullable
     T defaultValue();


}
