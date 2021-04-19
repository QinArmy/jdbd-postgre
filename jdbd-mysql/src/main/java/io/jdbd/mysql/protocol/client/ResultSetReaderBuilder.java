package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.vendor.result.ResultRowSink;

import java.util.function.Consumer;
import java.util.function.Supplier;

final class ResultSetReaderBuilder {

    static ResultSetReaderBuilder builder() {
        return new ResultSetReaderBuilder();
    }

    ResultRowSink rowSink;

    MySQLTaskAdjutant adjutant;

    boolean fetchResult;

    Consumer<Integer> sequenceIdUpdater;

    Consumer<JdbdException> errorConsumer;

    Supplier<Boolean> errorJudger;

    boolean resettable;


    private ResultSetReaderBuilder() {

    }

    public ResultSetReaderBuilder rowSink(ResultRowSink rowSink) {
        this.rowSink = rowSink;
        return this;
    }

    public ResultSetReaderBuilder adjutant(MySQLTaskAdjutant adjutant) {
        this.adjutant = adjutant;
        return this;
    }

    public ResultSetReaderBuilder fetchResult(boolean fetchResult) {
        this.fetchResult = fetchResult;
        return this;
    }

    public ResultSetReaderBuilder sequenceIdUpdater(Consumer<Integer> sequenceIdUpdater) {
        this.sequenceIdUpdater = sequenceIdUpdater;
        return this;
    }

    public ResultSetReaderBuilder errorConsumer(Consumer<JdbdException> errorConsumer) {
        this.errorConsumer = errorConsumer;
        return this;
    }

    /**
     * @deprecated use {@link ResultRowSink#isCancelled()}
     */
    @Deprecated
    public ResultSetReaderBuilder errorJudger(Supplier<Boolean> errorJudger) {
        this.errorJudger = errorJudger;
        return this;
    }

    public ResultSetReaderBuilder resettable(boolean resettable) {
        this.resettable = resettable;
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T extends ResultSetReader> T build(Class<T> typeClass) {
        final T reader;
        if (typeClass == TextResultSetReader.class) {
            reader = (T) new TextResultSetReader(this);
        } else if (typeClass == BinaryResultSetReader.class) {
            reader = (T) new BinaryResultSetReader(this);
        } else {
            throw new IllegalArgumentException(String.format("Unknown type[%s]", typeClass.getName()));
        }
        return reader;
    }


}
