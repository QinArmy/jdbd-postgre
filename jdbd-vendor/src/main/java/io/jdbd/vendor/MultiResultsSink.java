package io.jdbd.vendor;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;

import java.sql.SQLException;


public interface MultiResultsSink {

    void error(Throwable e);

    void nextUpdate(ResultStates resultStates, boolean hasMore);

    void nextQueryRowMeta(ResultRowMeta rowMeta);

    RowSink obtainCurrentRowSink();

    void emitRowTerminator(ResultStates resultStates, boolean hasMore);


    interface RowSink {

        void error(SQLException e);

        ResultRowMeta getRowMeta();

        boolean isCanceled();

        void next(ResultRow row);
    }

}
