package io.jdbd.vendor.result;

import io.jdbd.meta.DataType;

import java.nio.charset.Charset;

public interface ColumnMeta {

     int getColumnIndex();

     DataType getDataType();

     String getColumnLabel();

     Class<?> getOutputJavaType();

     Charset getColumnCharset();

     boolean isUnsigned();


}
