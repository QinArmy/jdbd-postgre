package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.meta.KeyMode;
import io.jdbd.meta.NullMode;
import io.jdbd.result.FieldType;
import io.jdbd.result.ResultRowMeta;

import java.util.Collections;
import java.util.Map;

public abstract class VendorResultRowMeta implements ResultRowMeta {

    protected static final Map<String, Integer> EMPTY_LABEL_INDEX_MAP = Collections.emptyMap();

    public final int resultIndex;


    protected VendorResultRowMeta(int resultIndex) {
        this.resultIndex = resultIndex;
    }

    @Override
    public final int getResultNo() {
        return this.resultIndex;
    }

    @Override
    public final DataType getDataType(String columnLabel) {
        return this.getDataType(this.getColumnIndex(columnLabel));
    }

    @Override
    public final String getTypeName(String columnLabel) {
        return this.getTypeName(this.getColumnIndex(columnLabel));
    }

    @Override
    public final JdbdType getJdbdType(String columnLabel) {
        return this.getJdbdType(this.getColumnIndex(columnLabel));
    }

    @Override
    public final FieldType getFieldType(String columnLabel) {
        return this.getFieldType(this.getColumnIndex(columnLabel));
    }

    @Override
    public final int getPrecision(String columnLabel) throws JdbdException {
        return this.getPrecision(this.getColumnIndex(columnLabel));
    }

    @Override
    public final int getScale(String columnLabel) throws JdbdException {
        return this.getScale(this.getColumnIndex(columnLabel));
    }

    @Override
    public final KeyMode getKeyMode(String columnLabel) throws JdbdException {
        return this.getKeyMode(this.getColumnIndex(columnLabel));
    }

    @Override
    public final NullMode getNullMode(String columnLabel) throws JdbdException {
        return this.getNullMode(this.getColumnIndex(columnLabel));
    }

    @Override
    public final boolean isAutoIncrement(String columnLabel) throws JdbdException {
        return this.isAutoIncrement(this.getColumnIndex(columnLabel));
    }

    @Override
    public final Class<?> getOutputJavaType(String columnLabel) throws JdbdException {
        return this.getOutputJavaType(this.getColumnIndex(columnLabel));
    }


    @Override
    public final boolean isSigned(String columnLabel) throws JdbdException {
        return this.isSigned(this.getColumnIndex(columnLabel));
    }

    @Override
    public final String getCatalogName(String columnLabel) throws JdbdException {
        return this.getCatalogName(this.getColumnIndex(columnLabel));
    }

    @Override
    public final String getSchemaName(String columnLabel) throws JdbdException {
        return this.getSchemaName(this.getColumnIndex(columnLabel));
    }

    @Override
    public final String getTableName(String columnLabel) throws JdbdException {
        return this.getTableName(this.getColumnIndex(columnLabel));
    }

    @Override
    public final String getColumnName(String columnLabel) throws JdbdException {
        return this.getColumnName(this.getColumnIndex(columnLabel));
    }

    @Override
    public final boolean isReadOnly(String columnLabel) throws JdbdException {
        return this.isReadOnly(this.getColumnIndex(columnLabel));
    }

    @Override
    public final boolean isWritable(String columnLabel) throws JdbdException {
        return this.isWritable(this.getColumnIndex(columnLabel));
    }


}
