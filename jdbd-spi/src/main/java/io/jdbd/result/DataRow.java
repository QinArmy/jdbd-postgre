package io.jdbd.result;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import org.reactivestreams.Publisher;

import java.nio.file.CopyOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link ResultRow}</li>
 *         <li>{@link CurrentRow}</li>
 *     </ul>
 * </p>
 * <p>
 * The {@link #getResultNo()} of this interface always return same value with {@link ResultRowMeta} in same query result.
 * See {@link #getRowMeta()}
 * </p>
 *
 * @since 1.0
 */
public interface DataRow extends ResultItem {


    ResultRowMeta getRowMeta();

    /**
     * <p>
     * Bit row means that exists at least one big column. see {@link #isBigColumn(int)}
     * </p>
     *
     * @return true : big row
     */
    boolean isBigRow();

    /**
     * <p>
     * Big column means that byte size is large,possibly is 1GB or more. For example LONG BLOB ,LONG TEXT,GEOMETRY_COLLECTION.
     * Driver cache them to local temp directory.
     * So {@link #get(int)} return :
     *      <ul>
     *          <li>{@link io.jdbd.type.BlobPath}</li>
     *          <li>{@link io.jdbd.type.TextPath}</li>
     *      </ul>
     *      If you need to store them ,then you must move them to other directory by {@link java.nio.file.Files#move(Path, Path, CopyOption...)} method.
     *      Else driver will delete them after result set end.
     * </p>
     *
     * @param indexBasedZero index based zero,the first value is 0 .
     * @return true : big column
     * @throws JdbdException throw when indexBasedZero error
     */
    boolean isBigColumn(int indexBasedZero) throws JdbdException;

    /**
     * <p>
     * Get the value of output of column.
     * </p>
     *
     * @return must be one of following : <ol>
     * <li>null</li>
     * <li>the instance of {@link ResultRowMeta#getFirstJavaType(int) }</li>
     * <li>the instance of {@link ResultRowMeta#getSecondJavaType(int)}</li>
     * </ol>
     * @see ResultRowMeta#getFirstJavaType(int)
     * @see ResultRowMeta#getSecondJavaType(int)
     * @see #isBigColumn(int)
     */
    @Nullable
    Object get(int indexBasedZero) throws JdbdException;


    @Nullable
    <T> T get(int indexBasedZero, Class<T> columnClass) throws JdbdException;

    Object getNonNull(int indexBasedZero) throws NullPointerException, JdbdException;

    <T> T getNonNull(int indexBasedZero, Class<T> columnClass) throws NullPointerException, JdbdException;

    <T> List<T> getList(int indexBasedZero, Class<T> elementClass) throws JdbdException;

    <T> List<T> getList(int indexBasedZero, Class<T> elementClass, IntFunction<List<T>> constructor) throws JdbdException;

    <T> Set<T> getSet(int indexBasedZero, Class<T> elementClass) throws JdbdException;

    <T> Set<T> getSet(int indexBasedZero, Class<T> elementClass, IntFunction<Set<T>> constructor) throws JdbdException;


    <K, V> Map<K, V> getMap(int indexBasedZero, Class<K> keyClass, Class<V> valueClass) throws JdbdException;

    <K, V> Map<K, V> getMap(int indexBasedZero, Class<K> keyClass, Class<V> valueClass, IntFunction<Map<K, V>> constructor) throws JdbdException;


    <T> Publisher<T> getPublisher(int indexBasedZero, Class<T> valueClass) throws JdbdException;

    <T> Publisher<T> getResult(int indexBasedZero, Function<CurrentRow, T> function) throws JdbdException;

    <T> Publisher<T> getResult(int indexBasedZero, int fetchSize, Function<CurrentRow, T> function) throws JdbdException;

    /**
     * <p>
     * This method can be useful in various scenarios,for example {@link io.jdbd.meta.JdbdType#REF_CURSOR}.
     * </p>
     *
     * @param fetchSize non-negative <ul>
     *                  <li>0 : fetch all</li>
     *                  <li>positive</li>
     *                  </ul>
     * @throws JdbdException <ul>
     *                       <li>emit(not throw) session have closed</li>
     *                       <li>emit(not throw) result set have ended</li>
     *                       <li>throw when data type don't support this method</li>
     *                       <li>throw when indexBasedZero error</li>
     *                       <li>throw when fetchSize error</li>
     *                       </ul>
     */
    <T> Publisher<T> getResult(int indexBasedZero, int fetchSize, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) throws JdbdException;


    OrderedFlux getFlux(int indexBasedZero) throws JdbdException;

    /**
     * <p>
     * This method can be useful in various scenarios,for example {@link io.jdbd.meta.JdbdType#REF_CURSOR}.
     * </p>
     *
     * @param fetchSize non-negative <ul>
     *                  <li>0 : fetch all</li>
     *                  <li>positive</li>
     *                  </ul>
     * @throws JdbdException <ul>
     *                       <li>emit(not throw) session have closed</li>
     *                       <li>emit(not throw) result set have ended</li>
     *                       <li>throw when data type don't support this method</li>
     *                       <li>throw when indexBasedZero error</li>
     *                       <li>throw when fetchSize error</li>
     *                       </ul>
     */
    OrderedFlux getFlux(int indexBasedZero, int fetchSize) throws JdbdException;


    boolean isBigColumn(String columnLabel);

    /**
     * <p>
     * This method is equivalent to below:
     * <pre>
     *        final int columnIndex  = this.getRowMeta().getColumnIndex(columnLabel)
     *        return get(columnIndex);
     *     </pre>
     * </p>
     *
     * @see #get(int)
     */
    @Nullable
    Object get(String columnLabel) throws JdbdException;


    /**
     * <p>
     * This method is equivalent to below:
     * <pre>
     *        final int columnIndex  = this.getRowMeta().getColumnIndex(columnLabel)
     *        return get(columnIndex,columnClass);
     *     </pre>
     * </p>
     *
     * @see #get(int, Class)
     */
    @Nullable
    <T> T get(String columnLabel, Class<T> columnClass) throws JdbdException;


    Object getNonNull(String columnLabel) throws NullPointerException, JdbdException;

    <T> T getNonNull(String columnLabel, Class<T> columnClass) throws NullPointerException, JdbdException;


    /**
     * @see #getSet(int, Class)
     */
    <T> List<T> getList(String columnLabel, Class<T> elementClass) throws JdbdException;

    <T> List<T> getList(String columnLabel, Class<T> elementClass, IntFunction<List<T>> constructor) throws JdbdException;

    /**
     * @see #getSet(int, Class)
     */
    <T> Set<T> getSet(String columnLabel, Class<T> elementClass) throws JdbdException;

    <T> Set<T> getSet(String columnLabel, Class<T> elementClass, IntFunction<Set<T>> constructor) throws JdbdException;


    <K, V> Map<K, V> getMap(String columnLabel, Class<K> keyClass, Class<V> valueClass) throws JdbdException;

    /**
     * <p>
     * This can be useful in various scenarios,for example postgre hstore.
     * </p>
     */
    <K, V> Map<K, V> getMap(String columnLabel, Class<K> keyClass, Class<V> valueClass, IntFunction<Map<K, V>> constructor) throws JdbdException;

    <T> Publisher<T> getPublisher(String columnLabel, Class<T> valueClass) throws JdbdException;

    <T> Publisher<T> getResult(String columnLabel, Function<CurrentRow, T> function) throws JdbdException;

    <T> Publisher<T> getResult(String columnLabel, int fetchSize, Function<CurrentRow, T> function) throws JdbdException;

    <T> Publisher<T> getResult(String columnLabel, int fetchSize, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) throws JdbdException;

    OrderedFlux getFlux(String columnLabel) throws JdbdException;

    OrderedFlux getFlux(String columnLabel, int fetchSize) throws JdbdException;


}
