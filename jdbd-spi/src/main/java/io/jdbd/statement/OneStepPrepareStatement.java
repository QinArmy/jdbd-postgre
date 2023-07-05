package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;

import java.sql.JDBCType;

public interface OneStepPrepareStatement extends ServerPrepareStatement {

    @Override
    OneStepPrepareStatement bind(int indexBasedZero,@Nullable Object nullable) throws JdbdException;

    @Override
    OneStepPrepareStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable  Object nullable) throws JdbdException;

    @Override
    OneStepPrepareStatement bind(int indexBasedZero, DataType dataType, @Nullable  Object nullable) throws JdbdException;

    @Override
    OneStepPrepareStatement bind(int indexBasedZero, String dataTypeName,@Nullable  Object nullable) throws JdbdException;

    @Override
    OneStepPrepareStatement addBatch() throws JdbdException;



}
