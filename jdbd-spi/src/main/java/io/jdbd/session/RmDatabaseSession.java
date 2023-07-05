package io.jdbd.session;

import io.jdbd.JdbdXaException;
import org.reactivestreams.Publisher;

/**
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

    Publisher<RmDatabaseSession> end(Xid xid, int flags);

    Publisher<Integer> prepare(Xid xid);

    Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase);

    Publisher<RmDatabaseSession> rollback(Xid xid);

    Publisher<RmDatabaseSession> forget(Xid xid);

    Publisher<Xid> recover(int flags);


}
