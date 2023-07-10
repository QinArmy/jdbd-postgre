package io.jdbd.vendor.result;

import io.jdbd.meta.DataType;


public interface ColumnMeta {

     int getColumnIndex();

     DataType getDataType();

     String getColumnLabel();

     boolean isUnsigned();

     boolean isBit();


}
