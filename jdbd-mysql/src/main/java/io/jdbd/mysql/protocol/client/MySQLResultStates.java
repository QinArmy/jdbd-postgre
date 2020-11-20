package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.vendor.ReactorMultiResults;

public abstract class MySQLResultStates implements ReactorMultiResults {

    public static MySQLResultStates from(OkPacket okPacket) {
        return null;
    }

    public static MySQLResultStates from(EofPacket eofPacket) {
        return null;
    }

}
