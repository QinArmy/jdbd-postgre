package io.jdbd.postgre.protocol.client;

abstract class PgMessage {

    final byte type;

    PgMessage(byte type) {
        this.type = type;
    }


}
