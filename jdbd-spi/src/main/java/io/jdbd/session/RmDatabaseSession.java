package io.jdbd.session;

import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.Optional;

/**
 * <p>
 * This interface representing database session that support XA transaction.
 * </p>
 * <p>
 * The 'Rm' of the name this interface means Resource Manager of XA transaction.
 * </p>
 * <p>
 * The instance of this interface is created by {@link DatabaseSessionFactory}.
 * </p>
 * <p>
 * This interface extends {@link DatabaseSession} for support XA interface based on
 * the X/Open CAE Specification (Distributed Transaction Processing: The XA Specification).
 * This document is published by The Open Group and available at
 * <a href="http://www.opengroup.org/public/pubs/catalog/c193.htm">The XA Specification</a>,
 * here ,you can download the pdf about The XA Specification.
 * </p>
 * <p>
 * Application developer can control XA transaction by following :
 *     <ul>
 *         <li>{@link #start(Xid, int)}</li>
 *         <li>{@link #start(Xid, int, TransactionOption)}</li>
 *         <li>{@link #end(Xid, int)}</li>
 *         <li>{@link #end(Xid, int, Map)}</li>
 *         <li>{@link #prepare(Xid)}</li>
 *         <li>{@link #prepare(Xid, Map)}</li>
 *         <li>{@link #commit(Xid, boolean)}</li>
 *         <li>{@link #commit(Xid, boolean, Map)}</li>
 *         <li>{@link #rollback(Xid)}</li>
 *         <li>{@link #rollback(Xid, Map)}</li>
 *         <li>{@link #forget(Xid)}</li>
 *         <li>{@link #forget(Xid, Map)}</li>
 *         <li>{@link #recover(int)}</li>
 *         <li>{@link #recover(int, Map)}</li>
 *         <li>{@link #isSupportForget()}</li>
 *         <li>{@link #startSupportFlags()}</li>
 *         <li>{@link #endSupportFlags()}</li>
 *         <li>{@link #recoverSupportFlags()}</li>
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
 * @see <a href="http://www.opengroup.org/public/pubs/catalog/c193.html">The XA Specification</a>
 */
public interface RmDatabaseSession extends DatabaseSession {

    /**
     * Use TM_NO_FLAGS to indicate no flags value is selected.
     */
    int TM_NO_FLAGS = 0;

    /**
     * Caller is joining existing transaction branch.
     */
    int TM_JOIN = 1 << 21;

    /**
     * Ends a recovery scan.
     */
    int TM_END_RSCAN = 1 << 23;

    /**
     * Starts a recovery scan.
     */
    int TM_START_RSCAN = 1 << 24;

    /**
     * Caller is suspending (not ending) its association with
     * a transaction branch.
     */
    int TM_SUSPEND = 1 << 25;

    /**
     * Disassociates caller from a transaction branch.
     */
    int TM_SUCCESS = 1 << 26;

    /**
     * Caller is resuming association with a suspended
     * transaction branch.
     */
    int TM_RESUME = 1 << 27;

    /**
     * Disassociates the caller and marks the transaction branch
     * rollback-only.
     */
    int TM_FAIL = 1 << 29;

//    /**
//     * Caller is using one-phase optimization.
//     */
//    int TM_ONE_PHASE = 1 << 30;

    /**
     * The transaction branch has been read-only and has been committed.
     */
    int XA_RDONLY = 3;

    /**
     * The transaction work has been prepared normally.
     */
    int XA_OK = 0;



    Publisher<RmDatabaseSession> start(Xid xid, int flags);

    /**
     * <p>
     * Starts work on behalf of a transaction branch specified in
     * <code>xid</code>.
     * If {@link #TM_JOIN} is specified, the start applies to joining a transaction
     * previously seen by the resource manager. If {@link #TM_RESUME} is specified,
     * the start applies to resuming a suspended transaction specified in the
     * parameter <code>xid</code>.
     * If neither {@link #TM_JOIN} nor {@link #TM_RESUME} is specified and the transaction
     * specified by <code>xid</code> has previously been seen by the resource
     * manager, the resource manager throws the XAException exception with
     * {@link XaException#XAER_DUPID} error code.
     * </p>
     * <p>
     * To be safe,{@link RmDatabaseSession} write gtrid and bqual as hex strings. steps :
     * <ul>
     *     <li>Get byte[] with {@link java.nio.charset.StandardCharsets#UTF_8}</li>
     *     <li>write gtrid or bqual as hex strings</li>
     * </ul>
     * </p>
     *
     * @param flags bit set, support below flags:
     *              <ul>
     *                  <li>{@link #TM_NO_FLAGS}</li>
     *                  <li>{@link #TM_JOIN} Note that this flag cannot be used in conjunction with {@link #TM_RESUME}</li>
     *                  <li>{@link #TM_RESUME} Note that this flag cannot be used in conjunction with {@link #TM_JOIN}</li>
     *              </ul>
     * @return a Publisher that only emitting an element or error.The element is this instance.
     * @throws io.jdbd.JdbdException emit(not throw),when
     *                               <ul>
     *                                  <li>xid is null</li>
     *                                  <li>{@link Xid#getGtrid()} have no text</li>
     *                                  <li>{@link Xid#getBqual()} non-null and have no text</li>
     *                                  <li>database don't support appropriate flag</li>
     *                                  <li>transaction states error,see {@link XaStates}</li>
     *                                  <li>database server response error message , see {@link io.jdbd.result.ServerException}</li>
     *                               </ul>
     */
    Publisher<RmDatabaseSession> start(Xid xid, int flags, TransactionOption option);


    Publisher<RmDatabaseSession> end(Xid xid, int flags);

    /**
     * <p>
     * Ends the work performed on behalf of a transaction branch.
     * The resource manager disassociates the XA resource from the
     * transaction branch specified and lets the transaction
     * complete.
     * </p>
     * <p>
     * To be safe,{@link RmDatabaseSession} write gtrid and bqual as hex strings. steps :
     * <ul>
     *     <li>Get byte[] with {@link java.nio.charset.StandardCharsets#UTF_8}</li>
     *     <li>write gtrid or bqual as hex strings</li>
     * </ul>
     * </p>
     *
     * @param flags     bit set, support one of following :
     *                  <ul>
     *                      <li>{@link #TM_SUCCESS} s specified, the portion of work has completed successfully.</li>
     *                      <li>{@link #TM_FAIL} is specified, the portion of work has failed.<br/>
     *                      The resource manager may mark the transaction as rollback-only
     *                      </li>
     *                      <li>{@link #TM_SUSPEND} , the transaction branch is temporarily suspended in an incomplete state.<br/>
     *                      The transaction context is in a suspended state and must be resumed via the <code>start</code><br/>
     *                      method with {@link #TM_RESUME} specified.<br/>
     *                      </li>
     *                  </ul>
     * @param optionMap dialect option ,empty or option map.
     * @throws XaException emit(not throw) when
     *                     <ul>
     *                          <li>xid is null</li>
     *                          <li>{@link Xid#getGtrid()} have no text</li>
     *                          <li>{@link Xid#getBqual()} non-null and have no text</li>
     *                          <li>driver don't support appropriate flags</li>
     *                          <li>driver don't support optionMap</li>
     *                          <li>transaction states error,see {@link XaStates}</li>
     *                          <li>server response error message,see {@link io.jdbd.result.ServerException}</li>
     *                     </ul>
     */
    Publisher<RmDatabaseSession> end(Xid xid, int flags, Map<Option<?>, ?> optionMap);

    Publisher<Integer> prepare(Xid xid);

    /**
     * <p>
     * To be safe,{@link RmDatabaseSession} write gtrid and bqual as hex strings. steps :
     * <ul>
     *     <li>Get byte[] with {@link java.nio.charset.StandardCharsets#UTF_8}</li>
     *     <li>write gtrid or bqual as hex strings</li>
     * </ul>
     * </p>
     *
     * @param xid       non-null
     * @param optionMap optionMap dialect option ,empty or option map.
     * @throws XaException emit(not throw) when
     *                     <ul>
     *                          <li>xid is null</li>
     *                          <li>{@link Xid#getGtrid()} have no text</li>
     *                          <li>{@link Xid#getBqual()} non-null and have no text</li>
     *                          <li>driver don't support optionMap</li>
     *                          <li>transaction states error,see {@link XaStates}</li>
     *                          <li>server response error message,see {@link io.jdbd.result.ServerException}</li>
     *                     </ul>
     */
    Publisher<Integer> prepare(Xid xid, Map<Option<?>, ?> optionMap);


    Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase);

    /**
     * <p>
     * To be safe,{@link RmDatabaseSession} write gtrid and bqual as hex strings. steps :
     * <ul>
     *     <li>Get byte[] with {@link java.nio.charset.StandardCharsets#UTF_8}</li>
     *     <li>write gtrid or bqual as hex strings</li>
     * </ul>
     * </p>
     *
     * @param xid       non-null
     * @param optionMap optionMap dialect option ,empty or option map.
     * @throws XaException emit(not throw) when
     *                     <ul>
     *                          <li>xid is null</li>
     *                          <li>{@link Xid#getGtrid()} have no text</li>
     *                          <li>{@link Xid#getBqual()} non-null and have no text</li>
     *                          <li>driver don't support optionMap</li>
     *                          <li>transaction states error,see {@link XaStates}</li>
     *                          <li>server response error message,see {@link io.jdbd.result.ServerException}</li>
     *                     </ul>
     */
    Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase, Map<Option<?>, ?> optionMap);

    Publisher<RmDatabaseSession> rollback(Xid xid);

    /**
     * <p>
     * To be safe,{@link RmDatabaseSession} write gtrid and bqual as hex strings. steps :
     * <ul>
     *     <li>Get byte[] with {@link java.nio.charset.StandardCharsets#UTF_8}</li>
     *     <li>write gtrid or bqual as hex strings</li>
     * </ul>
     * </p>
     *
     * @param xid       non-null
     * @param optionMap optionMap dialect option ,empty or option map.
     * @throws XaException emit(not throw) when
     *                     <ul>
     *                          <li>xid is null</li>
     *                          <li>{@link Xid#getGtrid()} have no text</li>
     *                          <li>{@link Xid#getBqual()} non-null and have no text</li>
     *                          <li>driver don't support optionMap</li>
     *                          <li>transaction states error,see {@link XaStates}</li>
     *                          <li>server response error message,see {@link io.jdbd.result.ServerException}</li>
     *                     </ul>
     */
    Publisher<RmDatabaseSession> rollback(Xid xid, Map<Option<?>, ?> optionMap);

    Publisher<RmDatabaseSession> forget(Xid xid);

    /**
     * <p>
     * To be safe,{@link RmDatabaseSession} write gtrid and bqual as hex strings. steps :
     * <ul>
     *     <li>Get byte[] with {@link java.nio.charset.StandardCharsets#UTF_8}</li>
     *     <li>write gtrid or bqual as hex strings</li>
     * </ul>
     * </p>
     *
     * @param xid       non-null
     * @param optionMap optionMap dialect option ,empty or option map.
     * @throws XaException emit(not throw) when
     *                     <ul>
     *                          <li>driver don't support this method, see {@link #isSupportForget()}</li>
     *                          <li>xid is null</li>
     *                          <li>{@link Xid#getGtrid()} have no text</li>
     *                          <li>{@link Xid#getBqual()} non-null and have no text</li>
     *                          <li>driver don't support optionMap</li>
     *                          <li>transaction states error,see {@link XaStates}</li>
     *                          <li>server response error message,see {@link io.jdbd.result.ServerException}</li>
     *                     </ul>
     */
    Publisher<RmDatabaseSession> forget(Xid xid, Map<Option<?>, ?> optionMap);

    Publisher<Optional<Xid>> recover(int flags);

    /**
     * <p>
     * To be safe,{@link RmDatabaseSession} write gtrid and bqual as hex strings. steps :
     * <ul>
     *     <li>Get byte[] of trid ( or bqual) with {@link java.nio.charset.StandardCharsets#UTF_8}</li>
     *     <li>write gtrid ( or bqual) as hex strings</li>
     * </ul>
     * so the conversion process of this method is the reverse of above.
     * </p>
     *
     * @param flags     bit sets
     * @param optionMap optionMap dialect option ,empty or option map.
     * @return return the xids whose xid format follow this driver, If xid format don't follow this driver, then it is represented by {@link Optional#empty()}.
     * @throws XaException emit(not throw) when
     *                     <ul>
     *                          <li>driver don't support appropriate flags</li>
     *                          <li>driver don't support optionMap</li>
     *                          <li>server response unknown xid format</li>
     *                          <li>server response error message,see {@link io.jdbd.result.ServerException}</li>
     *                     </ul>
     */
    Publisher<Optional<Xid>> recover(int flags, Map<Option<?>, ?> optionMap);

    /**
     * @return true : support {@link #forget(Xid, Map)} method
     */
    boolean isSupportForget();

    /**
     * @return the sub set of {@link #start(Xid, int, TransactionOption)} support flags(bit set).
     */
    int startSupportFlags();

    /**
     * @return the sub set of {@link #end(Xid, int, Map)} support flags(bit set).
     */
    int endSupportFlags();

    /**
     * @return the sub set of {@link #recover(int, Map)} support flags(bit set).
     */
    int recoverSupportFlags();


    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<RmDatabaseSession> releaseSavePoint(SavePoint savepoint);

    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<RmDatabaseSession> releaseSavePoint(SavePoint savepoint, Map<Option<?>, ?> optionMap);


    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<RmDatabaseSession> rollbackToSavePoint(SavePoint savepoint);


    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<RmDatabaseSession> rollbackToSavePoint(SavePoint savepoint, Map<Option<?>, ?> optionMap);


    /**
     * {@inheritDoc}
     */
    @Override
    RmDatabaseSession bindIdentifier(StringBuilder builder, String identifier);


}
