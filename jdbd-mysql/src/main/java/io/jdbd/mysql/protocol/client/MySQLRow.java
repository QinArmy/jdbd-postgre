package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.result.VendorRow;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

abstract class MySQLRow extends VendorRow {

    final MySQLRowMeta rowMeta;

    final Object[] columnArray;


    private MySQLRow(MySQLRowMeta rowMeta) {
        this.rowMeta = rowMeta;
        final int arrayLength;
        arrayLength = rowMeta.columnMetaArray.length;
        this.columnArray = new Object[arrayLength];
    }

    private MySQLRow(JdbdCurrentRow currentRow) {
        this.rowMeta = currentRow.rowMeta;

        final Object[] columnArray = new Object[currentRow.columnArray.length];
        System.arraycopy(currentRow.columnArray, 0, columnArray, 0, columnArray.length);

        this.columnArray = columnArray;
    }


    @Override
    public final int getResultIndex() {
        return this.rowMeta.resultIndex;
    }

    @Override
    public final ResultRowMeta getRowMeta() {
        return this.rowMeta;
    }

    @Override
    public final Object get(int indexBasedZero) throws JdbdException {
        return this.columnArray[this.rowMeta.checkIndex(indexBasedZero)];
    }

    @Override
    public final <T> T get(final int indexBasedZero, final Class<T> columnClass) throws JdbdException {
        final MySQLRowMeta rowMeta;
        rowMeta = this.rowMeta;

        final Object source;
        source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];

        if (columnClass.isInstance(source)) {
            return (T) source;
        }

        final MySQLColumnMeta meta;
        meta = rowMeta.columnMetaArray[indexBasedZero];


        return null;
    }

    @Override
    public final Object getNonNull(final int indexBasedZero) throws NullPointerException, JdbdException {
        final Object value;
        value = this.columnArray[this.rowMeta.checkIndex(indexBasedZero)];
        if (value == null) {
            throw MySQLExceptions.columnIsNull(this.rowMeta.columnMetaArray[indexBasedZero]);
        }
        return value;
    }

    @Override
    public <T> T getNonNull(int indexBasedZero, Class<T> columnClass) throws NullPointerException, JdbdException {
        return null;
    }


    @Override
    public <T> List<T> getList(int indexBasedZero, Class<T> elementClass, IntFunction<List<T>> constructor)
            throws JdbdException {
        return null;
    }

    @Override
    public <T> Set<T> getSet(int indexBasedZero, Class<T> elementClass, IntFunction<Set<T>> constructor)
            throws JdbdException {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(int indexBasedZero, Class<K> keyClass, Class<V> valueClass, IntFunction<Map<K, V>> constructor)
            throws JdbdException {
        return null;
    }

    @Override
    public <T> Publisher<T> getPublisher(int indexBasedZero, Class<T> valueClass) throws JdbdException {
        return null;
    }


    static abstract class JdbdCurrentRow extends MySQLRow implements CurrentRow {

        JdbdCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
        }

        @Override
        public final ResultRow asResultRow() {
            return new MySQLResultRow(this);
        }


    }//JdbdCurrentRow

    private static final class MySQLResultRow extends MySQLRow implements ResultRow {
        private MySQLResultRow(JdbdCurrentRow currentRow) {
            super(currentRow);
        }


    }//MySQLResultRow


    private static <T> BiFunction<MySQLColumnMeta, Class<T>, T> getConverter(Class<T> targetClass) {
        throw new UnsupportedOperationException();
    }


}
