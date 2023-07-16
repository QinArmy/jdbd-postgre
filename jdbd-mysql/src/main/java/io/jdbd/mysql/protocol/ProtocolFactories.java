package io.jdbd.mysql.protocol;

import java.util.Map;

public abstract class ProtocolFactories {

    private ProtocolFactories() {
        throw new UnsupportedOperationException();
    }


    public static MySQLProtocolFactory from(String url, Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }


}
