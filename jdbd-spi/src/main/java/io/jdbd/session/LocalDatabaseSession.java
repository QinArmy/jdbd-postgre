package io.jdbd.session;

import io.jdbd.JdbdException;
import org.reactivestreams.Publisher;

import java.util.Map;

/**
 * <p>
 * This interface representing database session that support local transaction.
 * </p>
 * <p>
 * This interface is is similar to {@code javax.sql.Connection} , except that this interface is reactive.
 * </p>
 * <p>
 * This interface is base interface of {@link io.jdbd.pool.PoolLocalDatabaseSession}.
 * </p>
 * <p>
 * The instance of this interface is created by {@link DatabaseSessionFactory#localSession()} method.
 * </p>
 * <p>
 * Application developer can control local transaction by following :
 *     <ul>
 *         <li>{@link #startTransaction(TransactionOption)}</li>
 *         <li>{@link #startTransaction(TransactionOption, HandleMode)}</li>
 *         <li>{@link #inTransaction()}</li>
 *         <li>{@link #commit()}</li>
 *         <li>{@link #commit(Map)}</li>
 *         <li>{@link #rollback()}</li>
 *         <li>{@link #rollback(Map)}</li>
 *         <li>{@link #setSavePoint()}</li>
 *         <li>{@link #setSavePoint(String)}</li>
 *         <li>{@link #setSavePoint(String, Map)}</li>
 *         <li>{@link #releaseSavePoint(SavePoint)}</li>
 *         <li>{@link #releaseSavePoint(SavePoint, Map)}</li>
 *         <li>{@link #rollbackToSavePoint(SavePoint)}</li>
 *         <li>{@link #rollbackToSavePoint(SavePoint, Map)}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface LocalDatabaseSession extends DatabaseSession {


    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // session is instance of LocalDatabaseSession
     *             session.startTransaction(option,HandleMode.ERROR_IF_EXISTS) ;
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #startTransaction(TransactionOption, HandleMode)
     */
    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option);

    /**
     * <p>
     * Start one local transaction with option.
     * </p>
     * <p>
     * Driver developer should guarantee transaction option (eg: {@link Isolation}) applies only this new transaction.
     * </p>
     * <p>
     * The implementation of this method <strong>perhaps</strong> support some of following :
     *     <ul>
     *         <li>{@link Option#WITH_CONSISTENT_SNAPSHOT}</li>
     *         <li>{@link Option#DEFERRABLE}</li>
     *         <li>{@link Option#NAME}</li>
     *     </ul>
     * </p>
     *
     * @param option non-null transaction option, driver perhaps support dialect transaction option by {@link TransactionOption#valueOf(Option)}.
     * @param mode   the handle mode when have existed local transaction :
     *               <ul>
     *                  <li>{@link HandleMode#ERROR_IF_EXISTS} see {@link #inTransaction()} : emit(not throw) {@link JdbdException}</li>
     *                  <li>{@link HandleMode#COMMIT_IF_EXISTS} : commit existed transaction before new transaction.</li>
     *                  <li>{@link HandleMode#ROLLBACK_IF_EXISTS} : rollback existed transaction before new transaction.</li>
     *               </ul>
     * @return emit <strong>this</strong> or {@link Throwable}. Like {@code reactor.core.publisher.Mono}.
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                           li>appropriate {@link Isolation} isn't supported</li>
     *                           <li>have existed transaction and mode is {@link HandleMode#ERROR_IF_EXISTS},see {@link #inTransaction()}</li>
     *                           <li>session have closed, see {@link SessionCloseException}</li>
     *                           <li>network error</li>
     *                           <li>server response error message, see {@link io.jdbd.result.ServerException}</li>
     *                       </ul>
     * @see #setTransactionCharacteristics(TransactionOption)
     */
    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option, HandleMode mode);


    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // session is instance of LocalDatabaseSession
     *             session.commit(Collections.emptyMap()) ;
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #commit(Map)
     */
    Publisher<LocalDatabaseSession> commit();

    /**
     * <p>
     * COMMIT current local transaction of this session.
     * </p>
     * <p>
     * The implementation of this method <strong>perhaps</strong> support some of following :
     *     <ul>
     *         <li>{@link Option#CHAIN}</li>
     *         <li>{@link Option#RELEASE}</li>
     *     </ul>
     * </p>
     *
     * @param optionMap empty or dialect option map
     * @return emit <strong>this</strong> or {@link Throwable}. Like {@code reactor.core.publisher.Mono}.
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                          <li>driver don't support appropriate {@link Option}</li>
     *                          <li>network error</li>
     *                          <li>session have closed,see {@link SessionCloseException}</li>
     *                          <li>serer response error message, see {@link io.jdbd.result.ServerException}</li>
     *                       </ul>
     */
    Publisher<LocalDatabaseSession> commit(Map<Option<?>, ?> optionMap);

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // session is instance of LocalDatabaseSession
     *             session.rollback(Collections.emptyMap()) ;
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #rollback(Map)
     */
    Publisher<LocalDatabaseSession> rollback();

    /**
     * <p>
     * ROLLBACK current local transaction of this session.
     * </p>
     * <p>
     * The implementation of this method <strong>perhaps</strong> support some of following :
     *     <ul>
     *         <li>{@link Option#CHAIN}</li>
     *         <li>{@link Option#RELEASE}</li>
     *     </ul>
     * </p>
     *
     * @param optionMap empty or dialect option map
     * @return emit <strong>this</strong> or {@link Throwable}. Like {@code reactor.core.publisher.Mono}.
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                          <li>driver don't support appropriate {@link Option}</li>
     *                          <li>network error</li>
     *                          <li>session have closed,see {@link SessionCloseException}</li>
     *                          <li>serer response error message, see {@link io.jdbd.result.ServerException}</li>
     *                       </ul>
     */
    Publisher<LocalDatabaseSession> rollback(Map<Option<?>, ?> optionMap);


    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<LocalDatabaseSession> setTransactionCharacteristics(TransactionOption option);


    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<LocalDatabaseSession> releaseSavePoint(SavePoint savepoint);

    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<LocalDatabaseSession> releaseSavePoint(SavePoint savepoint, Map<Option<?>, ?> optionMap);

    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<LocalDatabaseSession> rollbackToSavePoint(SavePoint savepoint);

    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<LocalDatabaseSession> rollbackToSavePoint(SavePoint savepoint, Map<Option<?>, ?> optionMap);


    /**
     * {@inheritDoc}
     */
    @Override
    LocalDatabaseSession bindIdentifier(StringBuilder builder, String identifier);


}
