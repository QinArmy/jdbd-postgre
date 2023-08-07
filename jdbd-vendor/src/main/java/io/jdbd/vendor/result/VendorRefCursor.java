package io.jdbd.vendor.result;

import io.jdbd.result.CurrentRow;
import io.jdbd.result.RefCursor;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;

import java.util.function.Function;

public abstract class VendorRefCursor implements RefCursor {

    protected final String name;

    protected VendorRefCursor(String name) {
        this.name = name;
    }

    @Override
    public final String name() {
        return this.name;
    }

    @Override
    public final <T> Publisher<T> first(Function<CurrentRow, T> function) {
        return this.first(function, Stmts.IGNORE_RESULT_STATES);
    }


    @Override
    public final <T> Publisher<T> last(Function<CurrentRow, T> function) {
        return this.last(function, Stmts.IGNORE_RESULT_STATES);
    }


    @Override
    public final <T> Publisher<T> prior(Function<CurrentRow, T> function) {
        return this.prior(function, Stmts.IGNORE_RESULT_STATES);
    }


    @Override
    public final <T> Publisher<T> next(Function<CurrentRow, T> function) {
        return this.next(function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public final <T> Publisher<T> absolute(long count, Function<CurrentRow, T> function) {
        return this.absolute(count, function, Stmts.IGNORE_RESULT_STATES);
    }


    @Override
    public final <T> Publisher<T> relative(long count, Function<CurrentRow, T> function) {
        return this.relative(count, function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public final <T> Publisher<T> forward(long count, Function<CurrentRow, T> function) {
        return this.forward(count, function, Stmts.IGNORE_RESULT_STATES);
    }


    @Override
    public final <T> Publisher<T> forwardAll(Function<CurrentRow, T> function) {
        return this.forwardAll(function, Stmts.IGNORE_RESULT_STATES);
    }


    @Override
    public final <T> Publisher<T> backward(long count, Function<CurrentRow, T> function) {
        return this.backward(count, function, Stmts.IGNORE_RESULT_STATES);
    }


    @Override
    public final <T> Publisher<T> backwardAll(Function<CurrentRow, T> function) {
        return this.backwardAll(function, Stmts.IGNORE_RESULT_STATES);
    }


}
