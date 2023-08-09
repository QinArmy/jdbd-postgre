package io.jdbd.session;

import org.reactivestreams.Publisher;

import java.util.Map;

/**
 * <p>
 * This interface representing database session that support global transaction.
 * </p>
 * <p>
 * This interface extends {@link DatabaseSession} for support XA interface based on
 * the X/Open CAE Specification (Distributed Transaction Processing: The XA Specification).
 * This document is published by The Open Group and available at
 * <a href="http://www.opengroup.org/public/pubs/catalog/c193.htm">The XA Specification</a>,
 * here ,you can download the pdf about The XA Specification.
 * </p>
 *
 * @see <a href="http://www.opengroup.org/public/pubs/catalog/c193.htm">The XA Specification</a>
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

    /**
     * Caller is using one-phase optimization.
     */
    int TM_ONE_PHASE = 1 << 30;

    /**
     * The transaction branch has been read-only and has been committed.
     */
    int XA_RDONLY = 3;

    /**
     * The transaction work has been prepared normally.
     */
    int XA_OK = 0;


    @Override
    Publisher<RmDatabaseSession> releaseSavePoint(SavePoint savepoint);

    @Override
    Publisher<RmDatabaseSession> rollbackToSavePoint(SavePoint savepoint);


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
     *
     * @param flags bit set, support below flags:
     *              <ul>
     *                  <li>{@link #TM_NO_FLAGS}</li>
     *                  <li>{@link #TM_JOIN} Note that this flag cannot be used in conjunction with {@link #TM_RESUME}</li>
     *                  <li>{@link #TM_RESUME} Note that this flag cannot be used in conjunction with {@link #TM_JOIN}</li>
     *              </ul>
     * @return a Publisher that only emitting an element or error.The element is this instance.
     * @throws io.jdbd.JdbdException emit(not throw),when
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
     * @param optionMap dialect option
     * @throws XaException emit(not throw) when
     *                     <ul>
     *                          <li>driver don't support appropriate flags</li>
     *                          <li>driver don't support optionMap</li>
     *                          <li>transaction status error</li>
     *                          <li>server response error message,see {@link io.jdbd.result.ServerException}</li>
     *                     </ul>
     */
    Publisher<RmDatabaseSession> end(Xid xid, int flags, Map<Option<?>, ?> optionMap);

    Publisher<Integer> prepare(Xid xid);

    Publisher<Integer> prepare(Xid xid, Map<Option<?>, ?> optionMap);

    Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase);

    Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase, Map<Option<?>, ?> optionMap);

    Publisher<RmDatabaseSession> rollback(Xid xid);

    Publisher<RmDatabaseSession> rollback(Xid xid, Map<Option<?>, ?> optionMap);

    Publisher<RmDatabaseSession> forget(Xid xid);

    Publisher<RmDatabaseSession> forget(Xid xid, Map<Option<?>, ?> optionMap);

    Publisher<Xid> recover(int flags);

    Publisher<Xid> recover(int flags, Map<Option<?>, ?> optionMap);

    @Override
    RmDatabaseSession bindIdentifier(StringBuilder builder, String identifier);


}
