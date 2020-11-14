package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultStates;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.OkPacket;

public abstract class MySQLResultStates implements ResultStates {

    public static MySQLResultStates from(OkPacket okPacket) {
        return null;
    }

    public static MySQLResultStates from(EofPacket eofPacket) {
        return null;
    }

}
