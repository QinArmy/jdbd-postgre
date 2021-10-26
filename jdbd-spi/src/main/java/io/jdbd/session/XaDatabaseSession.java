package io.jdbd.session;

import org.reactivestreams.Publisher;


public interface XaDatabaseSession extends DatabaseSession {

    int TMENDRSCAN = 8388608;
    int TMFAIL = 536870912;
    int TMJOIN = 2097152;
    int TMNOFLAGS = 0;

    int TMONEPHASE = 1073741824;
    int TMRESUME = 134217728;
    int TMSTARTRSCAN = 16777216;
    int TMSUCCESS = 67108864;

    int TMSUSPEND = 33554432;
    int XA_RDONLY = 3;
    int XA_OK = 0;

    Publisher<XaDatabaseSession> start(Xid xid, int flags);

    Publisher<XaDatabaseSession> commit(Xid xid, boolean onePhase);

    Publisher<XaDatabaseSession> end(Xid xid, int flags);

    Publisher<XaDatabaseSession> forget(Xid xid);

    Publisher<Integer> prepare(Xid xid);

    Publisher<Xid> recover(int flag);

    Publisher<XaDatabaseSession> rollback(Xid xid);


}
