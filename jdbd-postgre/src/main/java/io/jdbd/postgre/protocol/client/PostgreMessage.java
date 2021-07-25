package io.jdbd.postgre.protocol.client;

abstract class PostgreMessage {

    final byte type;

    PostgreMessage(byte type) {
        this.type = type;
    }


}
