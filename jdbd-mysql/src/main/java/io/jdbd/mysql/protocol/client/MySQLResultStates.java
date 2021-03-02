package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultStates;
import io.jdbd.mysql.protocol.TerminatorPacket;

public abstract class MySQLResultStates implements ResultStates {

    public static MySQLResultStates from(TerminatorPacket okPacket) {
        return null;
    }


    public int getServerStatus() {
        return 0;
    }

}
