package io.jdbd.postgre;


import java.sql.JDBCType;

public enum PostgreType implements io.jdbd.meta.SQLType {


    ;

    @Override
    public JDBCType jdbcType() {
        return null;
    }

    @Override
    public Class<?> javaType() {
        return null;
    }

    @Override
    public boolean isUnsigned() {
        return false;
    }

    @Override
    public boolean isNumber() {
        return false;
    }

    @Override
    public boolean isIntegerType() {
        return false;
    }

    @Override
    public boolean isFloatType() {
        return false;
    }

    @Override
    public boolean isText() {
        return false;
    }

    @Override
    public boolean isBlob() {
        return false;
    }

    @Override
    public boolean isString() {
        return false;
    }

    @Override
    public boolean isBinary() {
        return false;
    }

    @Override
    public boolean isTimeType() {
        return false;
    }

    @Override
    public boolean isDecimal() {
        return false;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getVendor() {
        return null;
    }

    @Override
    public Integer getVendorTypeNumber() {
        return null;
    }


}
