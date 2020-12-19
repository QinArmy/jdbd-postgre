package io.jdbd.vendor;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;

import java.sql.SQLException;

public final class EmptyRowSink implements MultiResultsSink.RowSink {

    public static EmptyRowSink create(ResultRowMeta rowMeta) {
        return new EmptyRowSink(rowMeta);
    }

    private final ResultRowMeta rowMeta;

    private EmptyRowSink(ResultRowMeta rowMeta) {
        this.rowMeta = rowMeta;
    }

    @Override
    public void error(SQLException e) {
        //no-op
    }

    @Override
    public ResultRowMeta getRowMeta() {
        return this.rowMeta;
    }

    @Override
    public boolean isCanceled() {
        return true;
    }

    @Override
    public void next(ResultRow row) {
        //no-op
    }
}
