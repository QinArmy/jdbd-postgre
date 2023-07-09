package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.result.VendorRow;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class MySQLRow extends VendorRow {

    private static final Map<String, Integer> EMPTY_LABEL_INDEX_MAP = Collections.emptyMap();


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
        return this.columnArray[indexBasedZero];
    }

    @Override
    public <T> T get(int indexBasedZero, Class<T> columnClass) throws JdbdException {
        return null;
    }

    @Override
    public Object getNonNull(int indexBasedZero) throws NullPointerException, JdbdException {
        return null;
    }

    @Override
    public <T> T getNonNull(int indexBasedZero, Class<T> columnClass) throws NullPointerException, JdbdException {
        return null;
    }

    @Override
    public <T> List<T> getList(int indexBasedZero, Class<T> elementClass) throws JdbdException {
        return null;
    }

    @Override
    public <T> Set<T> getSet(int indexBasedZero, Class<T> elementClass) throws JdbdException {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(int indexBasedZero, Class<K> keyClass, Class<V> valueClass) throws JdbdException {
        return null;
    }

    @Override
    public <T> Publisher<T> getPublisher(int indexBasedZero, Class<T> valueClass) throws JdbdException {
        return null;
    }


    @Override
    protected final int mapToIndex(final String columnLabel) {
        return 0;
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


}
