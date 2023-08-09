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

    int TMNOFLAGS = 0;
    int TMJOIN = 1 << 21;
    int TMENDRSCAN = 1 << 23;
    int TMSTARTRSCAN = 1 << 24;

    int TMSUSPEND = 1 << 25;
    int TMSUCCESS = 1 << 26;
    int TMRESUME = 1 << 27;
    int TMFAIL = 1 << 29;

    int TMONEPHASE = 1 << 30;


    int XA_RDONLY = 3;
    int XA_OK = 0;


    Publisher<RmDatabaseSession> setTransactionOption(TransactionOption option);

    Publisher<RmDatabaseSession> setTransactionOption(TransactionOption option, Map<Option<?>, ?> optionMap);


    @Override
    Publisher<RmDatabaseSession> releaseSavePoint(SavePoint savepoint);

    @Override
    Publisher<RmDatabaseSession> rollbackToSavePoint(SavePoint savepoint);

    /**
     * <p>
     * Start work on behalf of a transaction branch.The appropriate method in XA interface is
     * int xa_start(XID ∗xid, int rmid, long flags).
     * </p>
     *
     * @param flags bit set, support below flags:
     *              <ul>
     *                  <li>{@link #TMNOFLAGS}</li>
     *                  <li>{@link #TMJOIN} Note that this flag cannot be used in conjunction with {@link #TMRESUME}</li>
     *                  <li>{@link #TMRESUME} Note that this flag cannot be used in conjunction with {@link #TMJOIN}</li>
     *              </ul>
     * @return a Publisher that only emitting an element or error.The element is this instance.
     * @throws JdbdXaException emit(not throw),when
     */
    Publisher<RmDatabaseSession> start(Xid xid, int flags);

    Publisher<RmDatabaseSession> start(Xid xid, int flags, Map<Option<?>, ?> optionMap);

    Publisher<RmDatabaseSession> end(Xid xid, int flags);

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
