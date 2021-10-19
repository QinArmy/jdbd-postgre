package io.jdbd.session;

import org.reactivestreams.Publisher;


public interface XaDatabaseSession extends DatabaseSession {

    Publisher<XaDatabaseSession> start(Xid xid, int flags);

    Publisher<XaDatabaseSession> commit(Xid xid, boolean onePhase);

    Publisher<XaDatabaseSession> end(Xid xid, int flags);

    Publisher<XaDatabaseSession> forget(Xid xid);

    Publisher<Integer> prepare(Xid xid);

    Publisher<Xid> recover(int flag);

    Publisher<XaDatabaseSession> rollback(Xid xid);


}
