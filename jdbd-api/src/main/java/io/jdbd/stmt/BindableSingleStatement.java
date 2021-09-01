package io.jdbd.stmt;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;
import java.util.function.Consumer;

/**
 * <p>
 * This interface is base interface of below:
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *     </ul>
 * </p>
 *
 * @see BindStatement
 * @see PreparedStatement
 */
public interface BindableSingleStatement extends Statement {


    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param jdbcType       mapping {@link JDBCType}
     * @param nullable       nullable null the parameter value
     */
    void bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param nullable       nullable the parameter value
     * @param sqlType        nonNullValue mapping sql data type name(must upper case).
     */
    void bind(int indexBasedZero, io.jdbd.meta.SQLType sqlType, @Nullable Object nullable) throws JdbdException;

    /**
     * @see BindStatement#bind(int, Object)
     * @see PreparedStatement#bind(int, Object)
     */
    void bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * @see BindStatement#addBatch()
     * @see PreparedStatement#addBatch()
     */
    void addBatch();

    /**
     * @see BindStatement#executeBatch()
     * @see PreparedStatement#executeBatch()
     */
    Publisher<ResultStates> executeBatch();

    /**
     * @see BindStatement#executeUpdate()
     * @see PreparedStatement#executeUpdate()
     */
    Publisher<ResultStates> executeUpdate();

    /**
     * @see BindStatement#executeQuery()
     * @see PreparedStatement#executeQuery()
     */
    Publisher<ResultRow> executeQuery();

    /**
     * @see BindStatement#executeQuery(Consumer)
     * @see PreparedStatement#executeQuery(Consumer)
     */
    Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer);

}
