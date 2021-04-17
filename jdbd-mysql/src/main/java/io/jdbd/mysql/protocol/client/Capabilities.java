package io.jdbd.mysql.protocol.client;

abstract class Capabilities {

    Capabilities() {
        throw new UnsupportedOperationException();
    }

    static boolean supportSsl(final int negotiatedCapability) {
        return (negotiatedCapability & ClientProtocol.CLIENT_SSL) != 0;
    }

    static boolean supportMultiStatement(final int negotiatedCapability) {
        return (negotiatedCapability & ClientProtocol.CLIENT_MULTI_STATEMENTS) != 0;
    }

    static boolean supportPsMultiResult(final int negotiatedCapability) {
        return (negotiatedCapability & ClientProtocol.CLIENT_PS_MULTI_RESULTS) != 0;
    }

}
